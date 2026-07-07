#![allow(dead_code)]

use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex};

use eventfs::{
    BackupDirectory, BranchIdentifier, BranchPageLimit, EventKind, EventPageLimit, EventRecord,
    FileIdentifier, FileSnapshot, Filesystem, FilesystemConfiguration, FilesystemError,
    FuseOperationError, MountedFilesystem,
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

pub fn filesystem_with_fuse_error_callback(
    directories: &TestDirectories,
    errors: &Arc<Mutex<Vec<FuseOperationError>>>,
) -> Filesystem {
    let errors = Arc::clone(errors);
    let configuration = directories
        .configuration()
        .with_fuse_error_callback(move |error| {
            errors
                .lock()
                .expect("callback error collection lock is available")
                .push(error);
        });
    Filesystem::open(configuration).expect("filesystem opens")
}

pub fn mount(filesystem: &Filesystem) -> MountedGuard {
    MountedGuard(Some(
        filesystem
            .spawn_mount()
            .expect("filesystem mounts in the background"),
    ))
}

pub fn create_empty_file(path: &Path) {
    drop(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .expect("empty file is created"),
    );
}

pub fn write_mounted_file(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    file.write_all(contents.as_ref())
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

pub fn event_count(filesystem: &Filesystem) -> usize {
    list_all_events(filesystem).len()
}

pub fn events_after(filesystem: &Filesystem, count: usize) -> Vec<EventRecord> {
    list_all_events(filesystem)
        .into_iter()
        .skip(count)
        .collect()
}

pub fn expect_no_events(events: &mut usize, filesystem: &Filesystem, message: &str) {
    assert_eq!(event_count(filesystem), *events, "{message}");
}

pub fn expect_event_count_delta(
    events: &mut usize,
    filesystem: &Filesystem,
    delta: usize,
    message: &str,
) -> Vec<EventRecord> {
    let new_events = events_after(filesystem, *events);
    assert_eq!(new_events.len(), delta, "{message}");
    *events += delta;
    new_events
}

pub fn expect_event_kinds(
    events: &mut usize,
    filesystem: &Filesystem,
    expected: &[EventKind],
    message: &str,
) -> Vec<EventRecord> {
    let new_events = expect_event_count_delta(events, filesystem, expected.len(), message);
    let actual = new_events.iter().map(EventRecord::kind).collect::<Vec<_>>();
    assert_eq!(actual, expected, "{message}");
    new_events
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

pub fn recorded_callback_errors(
    errors: &Arc<Mutex<Vec<FuseOperationError>>>,
) -> Vec<FuseOperationError> {
    errors
        .lock()
        .expect("callback error collection lock is available")
        .clone()
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

pub fn assert_callback_errors_include(
    errors: &[FuseOperationError],
    operation: &'static str,
    errno: i32,
    unsupported: bool,
) {
    assert!(
        errors.iter().any(|error| {
            error.operation() == operation
                && error.errno() == errno
                && error.filesystem_error() == FilesystemError::FilesystemOperation
                && error.is_unsupported() == unsupported
        }),
        "callback errors include operation {operation} with errno {errno}: {errors:?}"
    );
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

pub fn set_xattr(path: &Path, name: &str, value: &[u8], flags: i32) -> std::io::Result<()> {
    let path = c_path(path);
    let name = CString::new(name).expect("xattr name is valid");
    let result = unsafe { setxattr_raw(&path, &name, value, flags) };
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

pub fn get_xattr(path: &Path, name: &str) -> std::io::Result<Vec<u8>> {
    let path = c_path(path);
    let name = CString::new(name).expect("xattr name is valid");
    let size = unsafe { getxattr_raw(&path, &name, ptr::null_mut(), 0) };
    if size < 0 {
        return Err(last_os_error());
    }
    let mut value = vec![0; size as usize];
    let result = unsafe { getxattr_raw(&path, &name, value.as_mut_ptr().cast(), value.len()) };
    if result >= 0 {
        value.truncate(result as usize);
        Ok(value)
    } else {
        Err(last_os_error())
    }
}

pub fn get_xattr_into_buffer(path: &Path, name: &str, size: usize) -> std::io::Result<usize> {
    let path = c_path(path);
    let name = CString::new(name).expect("xattr name is valid");
    let mut value = vec![0; size];
    let result = unsafe { getxattr_raw(&path, &name, value.as_mut_ptr().cast(), value.len()) };
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(last_os_error())
    }
}

pub fn list_xattr(path: &Path) -> std::io::Result<Vec<u8>> {
    let path = c_path(path);
    let size = unsafe { listxattr_raw(&path, ptr::null_mut(), 0) };
    if size < 0 {
        return Err(last_os_error());
    }
    let mut value = vec![0; size as usize];
    let result = unsafe { listxattr_raw(&path, value.as_mut_ptr().cast(), value.len()) };
    if result >= 0 {
        value.truncate(result as usize);
        Ok(value)
    } else {
        Err(last_os_error())
    }
}

pub fn list_xattr_into_buffer(path: &Path, size: usize) -> std::io::Result<usize> {
    let path = c_path(path);
    let mut value = vec![0; size];
    let result = unsafe { listxattr_raw(&path, value.as_mut_ptr().cast(), value.len()) };
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(last_os_error())
    }
}

pub fn remove_xattr(path: &Path, name: &str) -> std::io::Result<()> {
    let path = c_path(path);
    let name = CString::new(name).expect("xattr name is valid");
    let result = unsafe { removexattr_raw(&path, &name) };
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

unsafe fn setxattr_raw(path: &CString, name: &CString, value: &[u8], flags: i32) -> i32 {
    unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            flags,
        )
    }
}

unsafe fn getxattr_raw(
    path: &CString,
    name: &CString,
    value: *mut libc::c_void,
    size: usize,
) -> isize {
    unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), value, size) }
}

unsafe fn listxattr_raw(path: &CString, value: *mut libc::c_char, size: usize) -> isize {
    unsafe { libc::listxattr(path.as_ptr(), value, size) }
}

unsafe fn removexattr_raw(path: &CString, name: &CString) -> i32 {
    unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) }
}

fn c_path(path: &Path) -> CString {
    CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
