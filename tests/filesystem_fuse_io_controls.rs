mod support;

use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};

use support::{
    TestDirectories, event_count, expect_no_events, filesystem_with_fuse_error_callback, mount,
    recorded_callback_errors, write_mounted_file,
};

#[test]
fn mounted_posix_locks_do_not_append_events_and_are_unsupported_when_routed() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("lock-file");

    write_mounted_file(&file_path, b"locks").expect("lock file is written");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("lock file opens");
    let mut events = event_count(&filesystem);

    let set_result = set_lock(file.as_raw_fd(), libc::F_WRLCK, 0, 3);
    let get_result = get_lock(file.as_raw_fd(), libc::F_WRLCK, 0, 3);
    let unlock_result = set_lock(file.as_raw_fd(), libc::F_UNLCK, 0, 3);
    let errors = recorded_callback_errors(&callback_errors);

    assert_removed_operation_result("setlk", set_result, &errors);
    assert_removed_operation_result("getlk", get_result, &errors);
    assert_removed_operation_result("setlk", unlock_result, &errors);
    expect_no_events(&mut events, &filesystem, "locks do not append events");
}

#[test]
fn mounted_ioctl_and_poll_do_not_append_events_and_are_unsupported_when_routed() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("ioctl-file");

    write_mounted_file(&file_path, b"ioctl").expect("ioctl file is written");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("ioctl file opens");
    let mut events = event_count(&filesystem);

    let ioctl_result = ioctl_rejection(file.as_raw_fd());
    let poll_result = poll_file(file.as_raw_fd());
    let errors = recorded_callback_errors(&callback_errors);

    assert_removed_operation_result("ioctl", ioctl_result, &errors);
    assert_removed_operation_result("poll", poll_result, &errors);
    expect_no_events(
        &mut events,
        &filesystem,
        "ioctl and poll do not append events",
    );
}

#[cfg(target_os = "linux")]
#[test]
fn mounted_linux_removed_data_operations_do_not_append_events() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let source_path = root.join("source");
    let destination_path = root.join("destination");
    let sparse_path = root.join("sparse");

    write_mounted_file(&source_path, b"copy source").expect("copy source file is written");
    write_mounted_file(&destination_path, b"destination").expect("destination file is written");
    write_mounted_file(&sparse_path, b"sparse").expect("sparse file is written");
    let source = OpenOptions::new()
        .read(true)
        .open(&source_path)
        .expect("source opens");
    let destination = OpenOptions::new()
        .write(true)
        .open(&destination_path)
        .expect("destination opens");
    let sparse = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&sparse_path)
        .expect("sparse file opens");
    let mut events = event_count(&filesystem);

    let fallocate_result = fallocate(sparse.as_raw_fd(), 0, 0, 4096);
    let errors = recorded_callback_errors(&callback_errors);
    assert_removed_operation_result("fallocate", fallocate_result, &errors);
    expect_no_events(&mut events, &filesystem, "fallocate does not append events");

    let lseek_data_result = seek(sparse.as_raw_fd(), 0, libc::SEEK_DATA);
    let errors = recorded_callback_errors(&callback_errors);
    assert_removed_operation_result("lseek", lseek_data_result, &errors);
    expect_no_events(&mut events, &filesystem, "SEEK_DATA does not append events");

    let lseek_hole_result = seek(sparse.as_raw_fd(), 0, libc::SEEK_HOLE);
    let errors = recorded_callback_errors(&callback_errors);
    assert_removed_operation_result("lseek", lseek_hole_result, &errors);
    expect_no_events(&mut events, &filesystem, "SEEK_HOLE does not append events");

    let copy_result = copy_file_range(source.as_raw_fd(), destination.as_raw_fd(), 0, 0, 4, 0);
    let errors = recorded_callback_errors(&callback_errors);
    assert_removed_operation_result("copy_file_range", copy_result, &errors);
    expect_no_events(
        &mut events,
        &filesystem,
        "copy_file_range does not append events",
    );
}

fn set_lock(
    fd: libc::c_int,
    typ: libc::c_int,
    start: libc::off_t,
    len: libc::off_t,
) -> std::io::Result<()> {
    let mut lock = file_lock(typ, start, len);
    syscall_zero(unsafe { libc::fcntl(fd, libc::F_SETLK, &mut lock) })
}

fn get_lock(
    fd: libc::c_int,
    typ: libc::c_int,
    start: libc::off_t,
    len: libc::off_t,
) -> std::io::Result<libc::flock> {
    let mut lock = file_lock(typ, start, len);
    syscall_zero(unsafe { libc::fcntl(fd, libc::F_GETLK, &mut lock) })?;
    Ok(lock)
}

fn file_lock(typ: libc::c_int, start: libc::off_t, len: libc::off_t) -> libc::flock {
    libc::flock {
        l_type: typ as libc::c_short,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: start,
        l_len: len,
        l_pid: 0,
    }
}

fn ioctl_rejection(fd: libc::c_int) -> std::io::Result<()> {
    let result = unsafe {
        libc::ioctl(
            fd,
            unsupported_ioctl_command(),
            std::ptr::null_mut::<libc::c_void>(),
        )
    };
    if result == -1 {
        Err(last_os_error())
    } else {
        Ok(())
    }
}

fn poll_file(fd: libc::c_int) -> std::io::Result<libc::c_short> {
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN | libc::POLLOUT,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
    if result >= 0 {
        Ok(descriptor.revents)
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn seek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> std::io::Result<libc::off_t> {
    let result = unsafe { libc::lseek(fd, offset, whence) };
    if result >= 0 {
        Ok(result)
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn fallocate(
    fd: libc::c_int,
    mode: libc::c_int,
    offset: libc::off_t,
    length: libc::off_t,
) -> std::io::Result<()> {
    syscall_zero(unsafe { libc::fallocate(fd, mode, offset, length) })
}

#[cfg(target_os = "linux")]
fn copy_file_range(
    source: libc::c_int,
    destination: libc::c_int,
    source_offset: libc::loff_t,
    destination_offset: libc::loff_t,
    length: usize,
    flags: libc::c_uint,
) -> std::io::Result<usize> {
    let mut source_offset = source_offset;
    let mut destination_offset = destination_offset;
    let result = unsafe {
        libc::syscall(
            libc::SYS_copy_file_range,
            source,
            &mut source_offset,
            destination,
            &mut destination_offset,
            length,
            flags,
        )
    };
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(last_os_error())
    }
}

fn assert_removed_operation_result<T>(
    operation: &'static str,
    result: std::io::Result<T>,
    errors: &[eventfs::FuseOperationError],
) {
    match result {
        Ok(_) => assert_callback_error_is_unsupported_when_present(operation, errors),
        Err(error) => {
            assert!(
                removed_operation_errno(error.raw_os_error()),
                "{operation} returned unexpected error: {error}"
            );
            assert_callback_error_is_unsupported_when_present(operation, errors);
        }
    }
}

fn assert_callback_error_is_unsupported_when_present(
    operation: &'static str,
    errors: &[eventfs::FuseOperationError],
) {
    if let Some(error) = errors.iter().find(|error| error.operation() == operation) {
        assert!(
            error.is_unsupported(),
            "{operation} callback error is marked unsupported"
        );
    }
}

fn removed_operation_errno(errno: Option<i32>) -> bool {
    matches!(
        errno,
        Some(
            libc::EACCES
                | libc::EINVAL
                | libc::ENOSYS
                | libc::ENOTSUP
                | libc::ENOTTY
                | libc::ENXIO
                | libc::EPERM
                | libc::EXDEV
        )
    )
}

fn syscall_zero(result: libc::c_int) -> std::io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn unsupported_ioctl_command() -> libc::Ioctl {
    0
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
