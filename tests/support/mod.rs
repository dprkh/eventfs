#![allow(dead_code)]

use std::ffi::CString;
use std::fs;
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;

use eventfs::{
    BackupDirectory, BranchIdentifier, BranchPageLimit, EventKind, EventPageLimit, EventRecord,
    FileIdentifier, FileSnapshot, Filesystem, FilesystemConfiguration, MountedFilesystem,
};
use tempfile::TempDir;

pub struct TestDirectories {
    _root: TempDir,
    database_directory: PathBuf,
    mount_point: PathBuf,
}

impl TestDirectories {
    pub fn new() -> Self {
        let root = TempDir::new().expect("temporary directory is created");
        let database_directory = root.path().join("database");
        let mount_point = root.path().join("mount");
        fs::create_dir(&mount_point).expect("mount point is created");
        Self {
            _root: root,
            database_directory,
            mount_point,
        }
    }

    pub fn root_path(&self) -> &Path {
        self._root.path()
    }

    pub fn database_directory_path(&self) -> &Path {
        &self.database_directory
    }

    pub fn mount_point_path(&self) -> &Path {
        &self.mount_point
    }

    pub fn configuration(&self) -> FilesystemConfiguration {
        configuration_for(self.database_directory.clone(), self.mount_point.clone())
    }

    pub fn backup_directory(&self) -> BackupDirectory {
        BackupDirectory::new(self.root_path().join("backups"))
            .expect("backup directory path is valid")
    }
}

pub struct MountedGuard(Option<MountedFilesystem>);

impl MountedGuard {
    pub fn unmount(mut self) -> Result<(), eventfs::FilesystemError> {
        self.0
            .take()
            .expect("mounted guard contains session")
            .unmount()
    }
}

impl Drop for MountedGuard {
    fn drop(&mut self) {
        if let Some(mounted) = self.0.take() {
            let _ = mounted.unmount();
        }
    }
}

pub fn configuration_for(
    database_directory: PathBuf,
    mount_point: PathBuf,
) -> FilesystemConfiguration {
    FilesystemConfiguration::new(database_directory, mount_point).expect("configuration is valid")
}

pub fn open_test_filesystem(directories: &TestDirectories) -> Filesystem {
    Filesystem::open(directories.configuration()).expect("filesystem opens")
}

pub fn mount(filesystem: &Filesystem) -> MountedGuard {
    MountedGuard(Some(
        filesystem
            .spawn_mount()
            .expect("filesystem mounts in the background"),
    ))
}

pub fn event_page_limit(value: u64) -> EventPageLimit {
    EventPageLimit::new(value).expect("event page limit is valid")
}

pub fn branch_page_limit(value: u64) -> BranchPageLimit {
    BranchPageLimit::new(value).expect("branch page limit is valid")
}

pub fn list_all_events(filesystem: &Filesystem) -> Vec<EventRecord> {
    filesystem
        .events(event_page_limit(100))
        .collect::<Result<Vec<_>, _>>()
        .expect("events are listed")
}

pub fn list_all_file_events(
    filesystem: &Filesystem,
    file_identifier: FileIdentifier,
) -> Vec<EventRecord> {
    filesystem
        .file_events(file_identifier, event_page_limit(100))
        .collect::<Result<Vec<_>, _>>()
        .expect("file events are listed")
}

pub fn list_all_branch_events(
    filesystem: &Filesystem,
    branch_identifier: BranchIdentifier,
) -> Vec<EventRecord> {
    filesystem
        .branch_events(branch_identifier, event_page_limit(100))
        .collect::<Result<Vec<_>, _>>()
        .expect("branch events are listed")
}

pub fn list_all_branch_file_events(
    filesystem: &Filesystem,
    branch_identifier: BranchIdentifier,
    file_identifier: FileIdentifier,
) -> Vec<EventRecord> {
    filesystem
        .branch_file_events(branch_identifier, file_identifier, event_page_limit(100))
        .collect::<Result<Vec<_>, _>>()
        .expect("branch file events are listed")
}

pub fn read_snapshot_bytes(filesystem: &Filesystem, snapshot: &FileSnapshot) -> Vec<u8> {
    filesystem
        .read_file_snapshot_range(snapshot, 0, snapshot.file_size())
        .expect("snapshot bytes are read")
}

pub fn file_identifier_for_path(filesystem: &Filesystem, path: &str) -> FileIdentifier {
    list_all_events(filesystem)
        .into_iter()
        .find(|event| event.kind() == EventKind::FileCreated && event.path() == Some(path))
        .and_then(|event| event.file_identifier())
        .expect("file creation event has an identifier")
}

pub fn assert_event_sequences_increase(events: &[EventRecord]) {
    for pair in events.windows(2) {
        assert!(
            pair[0].sequence() < pair[1].sequence(),
            "event sequences increase"
        );
    }
}

pub fn assert_branch_positions_increase(events: &[EventRecord]) {
    for pair in events.windows(2) {
        let first = pair[0]
            .branch_position()
            .expect("first event has a branch position");
        let second = pair[1]
            .branch_position()
            .expect("second event has a branch position");
        assert_eq!(
            first.branch_identifier(),
            second.branch_identifier(),
            "branch positions stay on one branch"
        );
        assert!(
            first.ordinal() < second.ordinal(),
            "branch positions increase"
        );
    }
}

pub fn mkfifo(path: &Path) {
    let path = c_path(path);
    let result = unsafe { libc::mkfifo(path.as_ptr(), 0o644) };
    assert_eq!(result, 0, "mkfifo succeeds: {}", last_os_error());
}

pub fn access_path(path: &Path, mode: i32) -> i32 {
    let path = c_path(path);
    unsafe { libc::access(path.as_ptr(), mode) }
}

pub fn fsync_directory(path: &Path) {
    let path = c_path(path);
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
    assert!(fd >= 0, "directory opens for fsync: {}", last_os_error());
    let result = unsafe { libc::fsync(fd) };
    let close_result = unsafe { libc::close(fd) };
    assert_eq!(
        close_result,
        0,
        "directory closes after fsync: {}",
        last_os_error()
    );
    assert_eq!(result, 0, "directory fsync succeeds: {}", last_os_error());
}

pub fn statvfs(path: &Path) -> libc::statvfs {
    let path = c_path(path);
    let mut statistics = MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), statistics.as_mut_ptr()) };
    assert_eq!(result, 0, "statvfs succeeds: {}", last_os_error());
    unsafe { statistics.assume_init() }
}

pub fn assert_unsupported_xattrs(path: &Path) {
    assert_eq!(setxattr(path), -1, "setxattr fails as unsupported");
    assert_eq!(getxattr(path), -1, "getxattr fails as unsupported");
    assert_eq!(removexattr(path), -1, "removexattr fails as unsupported");
}

#[cfg(target_os = "linux")]
fn setxattr(path: &Path) -> i32 {
    let path = c_path(path);
    let name = CString::new("user.eventfs.unsupported").expect("xattr name is valid");
    let value = b"value";
    unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    }
}

#[cfg(target_os = "macos")]
fn setxattr(path: &Path) -> i32 {
    let path = c_path(path);
    let name = CString::new("user.eventfs.unsupported").expect("xattr name is valid");
    let value = b"value";
    unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
            0,
        )
    }
}

#[cfg(target_os = "linux")]
fn getxattr(path: &Path) -> isize {
    let path = c_path(path);
    let name = CString::new("user.eventfs.unsupported").expect("xattr name is valid");
    unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), ptr::null_mut(), 0) }
}

#[cfg(target_os = "macos")]
fn getxattr(path: &Path) -> isize {
    let path = c_path(path);
    let name = CString::new("user.eventfs.unsupported").expect("xattr name is valid");
    unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), ptr::null_mut(), 0, 0, 0) }
}

#[cfg(target_os = "linux")]
fn removexattr(path: &Path) -> i32 {
    let path = c_path(path);
    let name = CString::new("user.eventfs.unsupported").expect("xattr name is valid");
    unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) }
}

#[cfg(target_os = "macos")]
fn removexattr(path: &Path) -> i32 {
    let path = c_path(path);
    let name = CString::new("user.eventfs.unsupported").expect("xattr name is valid");
    unsafe { libc::removexattr(path.as_ptr(), name.as_ptr(), 0) }
}

fn c_path(path: &Path) -> CString {
    CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
