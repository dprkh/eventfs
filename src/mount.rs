use std::fs;
use std::path::Path;

use crate::filesystem::{Filesystem, FilesystemError, MountOption, SessionAccessControlList};

/// Background mounted filesystem session.
#[derive(Debug)]
pub struct MountedFilesystem {
    session: Option<fuser::BackgroundSession>,
    filesystem: Filesystem,
}

impl MountedFilesystem {
    /// Unmounts the background filesystem session.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::FilesystemOperation`] when the session was already taken or FUSE
    /// unmounting fails.
    pub fn unmount(mut self) -> Result<(), FilesystemError> {
        let session = self
            .session
            .take()
            .ok_or(FilesystemError::FilesystemOperation)?;
        record_unmount_result(&self.filesystem, unmount(session))
    }
}

impl Drop for MountedFilesystem {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = record_unmount_result(&self.filesystem, unmount(session));
        }
    }
}

pub(crate) fn mount(filesystem: &Filesystem) -> Result<(), FilesystemError> {
    validate_mount_point(filesystem.configuration().mount_point())?;
    let configuration = mount_configuration(filesystem)?;
    filesystem.mark_mounted()?;
    let result = fuser::mount2(
        filesystem.clone(),
        filesystem.configuration().mount_point(),
        &configuration,
    )
    .map_err(|_| FilesystemError::FilesystemOperation);
    record_blocking_mount_result(filesystem, result)
}

pub(crate) fn spawn_mount(filesystem: &Filesystem) -> Result<MountedFilesystem, FilesystemError> {
    validate_mount_point(filesystem.configuration().mount_point())?;
    let configuration = mount_configuration(filesystem)?;
    filesystem.mark_mounted()?;
    match fuser::spawn_mount2(
        filesystem.clone(),
        filesystem.configuration().mount_point(),
        &configuration,
    ) {
        Ok(session) => Ok(MountedFilesystem {
            session: Some(session),
            filesystem: filesystem.clone(),
        }),
        Err(_) => {
            let _ = record_mount_start_failure(filesystem);
            Err(FilesystemError::FilesystemOperation)
        }
    }
}

fn record_blocking_mount_result(
    filesystem: &Filesystem,
    result: Result<(), FilesystemError>,
) -> Result<(), FilesystemError> {
    let unmount_result = filesystem.mark_unmounted();
    if result.is_ok() {
        unmount_result?;
    }
    result
}

fn record_mount_start_failure(filesystem: &Filesystem) -> Result<(), FilesystemError> {
    filesystem.mark_unmounted()
}

fn record_unmount_result(
    filesystem: &Filesystem,
    result: Result<(), FilesystemError>,
) -> Result<(), FilesystemError> {
    result?;
    filesystem.mark_unmounted()
}

fn unmount(session: fuser::BackgroundSession) -> Result<(), FilesystemError> {
    session
        .umount_and_join()
        .map_err(|_| FilesystemError::FilesystemOperation)
}

fn validate_mount_point(path: &Path) -> Result<(), FilesystemError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        _ => Err(FilesystemError::FilesystemOperation),
    }
}

fn mount_configuration(filesystem: &Filesystem) -> Result<fuser::Config, FilesystemError> {
    let mut configuration = fuser::Config::default();
    configuration.acl =
        fuser_session_access_control_list(filesystem.configuration().session_access_control_list());
    let caller_mount_options = filesystem.configuration().mount_options();
    if !caller_mount_options
        .iter()
        .any(|option| matches!(option, MountOption::FilesystemName(_)))
    {
        let volume_name = filesystem
            .storage()
            .volume_name()
            .map_err(|_| FilesystemError::FilesystemOperation)?;
        configuration
            .mount_options
            .push(fuser::MountOption::FSName(volume_name));
    }
    configuration
        .mount_options
        .extend(caller_mount_options.iter().map(fuser_mount_option));
    #[cfg(target_os = "macos")]
    {
        configuration
            .mount_options
            .push(fuser::MountOption::CUSTOM("noappledouble".to_owned()));
        configuration
            .mount_options
            .push(fuser::MountOption::CUSTOM("noapplexattr".to_owned()));
    }
    Ok(configuration)
}

fn fuser_session_access_control_list(
    session_access_control_list: SessionAccessControlList,
) -> fuser::SessionACL {
    match session_access_control_list {
        SessionAccessControlList::All => fuser::SessionACL::All,
        SessionAccessControlList::RootAndOwner => fuser::SessionACL::RootAndOwner,
        SessionAccessControlList::Owner => fuser::SessionACL::Owner,
    }
}

fn fuser_mount_option(option: &MountOption) -> fuser::MountOption {
    match option {
        MountOption::FilesystemName(value) => fuser::MountOption::FSName(value.clone()),
        MountOption::Subtype(value) => fuser::MountOption::Subtype(value.clone()),
        MountOption::Custom(value) => fuser::MountOption::CUSTOM(value.clone()),
        MountOption::AutoUnmount => fuser::MountOption::AutoUnmount,
        MountOption::DefaultPermissions => fuser::MountOption::DefaultPermissions,
        MountOption::Dev => fuser::MountOption::Dev,
        MountOption::NoDev => fuser::MountOption::NoDev,
        MountOption::Suid => fuser::MountOption::Suid,
        MountOption::NoSuid => fuser::MountOption::NoSuid,
        MountOption::ReadOnly => fuser::MountOption::RO,
        MountOption::ReadWrite => fuser::MountOption::RW,
        MountOption::Exec => fuser::MountOption::Exec,
        MountOption::NoExec => fuser::MountOption::NoExec,
        MountOption::Atime => fuser::MountOption::Atime,
        MountOption::NoAtime => fuser::MountOption::NoAtime,
        MountOption::DirSync => fuser::MountOption::DirSync,
        MountOption::Sync => fuser::MountOption::Sync,
        MountOption::Async => fuser::MountOption::Async,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::filesystem::FilesystemConfiguration;

    #[test]
    fn failed_blocking_mount_result_clears_mount_state() {
        let fixture = TestFilesystem::new("failed-blocking-mount");
        fixture
            .filesystem
            .mark_mounted()
            .expect("mount state is marked");

        assert_eq!(
            record_blocking_mount_result(
                &fixture.filesystem,
                Err(FilesystemError::FilesystemOperation)
            ),
            Err(FilesystemError::FilesystemOperation)
        );

        assert_unmounted(&fixture.filesystem);
    }

    #[test]
    fn failed_explicit_unmount_result_keeps_mount_state_conservative() {
        let fixture = TestFilesystem::new("failed-explicit-unmount");
        fixture
            .filesystem
            .mark_mounted()
            .expect("mount state is marked");

        assert_eq!(
            record_unmount_result(
                &fixture.filesystem,
                Err(FilesystemError::FilesystemOperation)
            ),
            Err(FilesystemError::FilesystemOperation)
        );
        assert_eq!(
            fixture.filesystem.switch_branch(
                fixture
                    .filesystem
                    .current_branch()
                    .expect("current branch is returned")
                    .name()
            ),
            Err(FilesystemError::FilesystemOperation)
        );

        record_unmount_result(&fixture.filesystem, Ok(())).expect("later unmount succeeds");
        assert_unmounted(&fixture.filesystem);
    }

    #[test]
    fn successful_unmount_result_releases_mount_state() {
        let fixture = TestFilesystem::new("successful-unmount");
        fixture
            .filesystem
            .mark_mounted()
            .expect("mount state is marked");

        record_unmount_result(&fixture.filesystem, Ok(())).expect("unmount succeeds");

        assert_unmounted(&fixture.filesystem);
    }

    #[test]
    fn mount_configuration_uses_default_filesystem_name_and_owner_access() {
        let fixture = TestFilesystem::new("default-mount-configuration");
        let configuration =
            mount_configuration(&fixture.filesystem).expect("mount configuration is built");

        assert_eq!(configuration.acl, fuser::SessionACL::Owner);
        assert!(
            matches!(
                configuration.mount_options.first(),
                Some(fuser::MountOption::FSName(_))
            ),
            "default filesystem name is supplied"
        );
    }

    #[test]
    fn mount_configuration_maps_session_access_control_list_and_mount_options() {
        let mount_options = vec![
            MountOption::FilesystemName("configured-name".to_owned()),
            MountOption::Subtype("eventfs".to_owned()),
            MountOption::Custom("debug".to_owned()),
            MountOption::AutoUnmount,
            MountOption::DefaultPermissions,
            MountOption::Dev,
            MountOption::NoDev,
            MountOption::Suid,
            MountOption::NoSuid,
            MountOption::ReadOnly,
            MountOption::ReadWrite,
            MountOption::Exec,
            MountOption::NoExec,
            MountOption::Atime,
            MountOption::NoAtime,
            MountOption::DirSync,
            MountOption::Sync,
            MountOption::Async,
        ];
        let expected_mount_options = vec![
            fuser::MountOption::FSName("configured-name".to_owned()),
            fuser::MountOption::Subtype("eventfs".to_owned()),
            fuser::MountOption::CUSTOM("debug".to_owned()),
            fuser::MountOption::AutoUnmount,
            fuser::MountOption::DefaultPermissions,
            fuser::MountOption::Dev,
            fuser::MountOption::NoDev,
            fuser::MountOption::Suid,
            fuser::MountOption::NoSuid,
            fuser::MountOption::RO,
            fuser::MountOption::RW,
            fuser::MountOption::Exec,
            fuser::MountOption::NoExec,
            fuser::MountOption::Atime,
            fuser::MountOption::NoAtime,
            fuser::MountOption::DirSync,
            fuser::MountOption::Sync,
            fuser::MountOption::Async,
        ];
        let fixture =
            TestFilesystem::new_with_configuration("configured-mount-options", |configuration| {
                configuration
                    .with_session_access_control_list(SessionAccessControlList::All)
                    .with_mount_options(mount_options)
            });
        let configuration =
            mount_configuration(&fixture.filesystem).expect("mount configuration is built");

        assert_eq!(configuration.acl, fuser::SessionACL::All);
        assert_eq!(
            &configuration.mount_options[..expected_mount_options.len()],
            expected_mount_options.as_slice()
        );
        assert_eq!(
            configuration
                .mount_options
                .iter()
                .filter(|option| matches!(option, fuser::MountOption::FSName(_)))
                .count(),
            1,
            "caller filesystem name replaces the default filesystem name"
        );
    }

    #[test]
    fn mount_configuration_maps_root_and_owner_access() {
        let fixture =
            TestFilesystem::new_with_configuration("root-and-owner-access", |configuration| {
                configuration
                    .with_session_access_control_list(SessionAccessControlList::RootAndOwner)
            });
        let configuration =
            mount_configuration(&fixture.filesystem).expect("mount configuration is built");

        assert_eq!(configuration.acl, fuser::SessionACL::RootAndOwner);
    }

    fn assert_unmounted(filesystem: &Filesystem) {
        let branch = filesystem
            .current_branch()
            .expect("current branch is returned");
        filesystem
            .switch_branch(branch.name())
            .expect("branch switch is allowed while unmounted");
    }

    struct TestFilesystem {
        filesystem: Filesystem,
        _root: tempfile::TempDir,
    }

    impl TestFilesystem {
        fn new(name: &str) -> Self {
            Self::new_with_configuration(name, |configuration| configuration)
        }

        fn new_with_configuration(
            name: &str,
            configure: impl FnOnce(FilesystemConfiguration) -> FilesystemConfiguration,
        ) -> Self {
            let root = tempfile::tempdir().expect("temporary directory is created");
            let database = root.path().join("database");
            let mount_point = root.path().join("mount");
            fs::create_dir(&mount_point).expect("mount point is created");
            let configuration = FilesystemConfiguration::new(database, mount_point)
                .unwrap_or_else(|_| panic!("{name} configuration is valid"));
            let filesystem = Filesystem::open(configure(configuration))
                .unwrap_or_else(|_| panic!("{name} filesystem opens"));
            Self {
                filesystem,
                _root: root,
            }
        }
    }
}
