use std::fs;
use std::path::Path;

use crate::filesystem::{Filesystem, FilesystemError};

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
    filesystem.mark_mounted()?;
    let result = fuser::mount2(
        filesystem.clone(),
        filesystem.configuration().mount_point(),
        &mount_configuration(),
    )
    .map_err(|_| FilesystemError::FilesystemOperation);
    record_blocking_mount_result(filesystem, result)
}

pub(crate) fn spawn_mount(filesystem: &Filesystem) -> Result<MountedFilesystem, FilesystemError> {
    validate_mount_point(filesystem.configuration().mount_point())?;
    filesystem.mark_mounted()?;
    match fuser::spawn_mount2(
        filesystem.clone(),
        filesystem.configuration().mount_point(),
        &mount_configuration(),
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

fn mount_configuration() -> fuser::Config {
    let mut configuration = fuser::Config::default();
    configuration
        .mount_options
        .push(fuser::MountOption::FSName("eventfs".to_owned()));
    #[cfg(target_os = "macos")]
    {
        configuration
            .mount_options
            .push(fuser::MountOption::CUSTOM("noappledouble".to_owned()));
        configuration
            .mount_options
            .push(fuser::MountOption::CUSTOM("noapplexattr".to_owned()));
    }
    configuration
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
            let root = tempfile::tempdir().expect("temporary directory is created");
            let database = root.path().join("database");
            let mount_point = root.path().join("mount");
            fs::create_dir(&mount_point).expect("mount point is created");
            let filesystem = Filesystem::open(
                FilesystemConfiguration::new(database, mount_point)
                    .unwrap_or_else(|_| panic!("{name} configuration is valid")),
            )
            .unwrap_or_else(|_| panic!("{name} filesystem opens"));
            Self {
                filesystem,
                _root: root,
            }
        }
    }
}
