mod support;

use std::fs::{self, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use eventfs::EventKind;

use support::{
    TestDirectories, event_count, events_after, expect_event_kinds, expect_no_events,
    filesystem_with_fuse_error_callback, mount, open_test_filesystem, recorded_callback_errors,
    write_mounted_file,
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
    let lseek_data_result = seek(sparse.as_raw_fd(), 0, libc::SEEK_DATA);
    let lseek_hole_result = seek(sparse.as_raw_fd(), 0, libc::SEEK_HOLE);
    let copy_result = copy_file_range(source.as_raw_fd(), destination.as_raw_fd(), 0, 0, 4, 0);
    let errors = recorded_callback_errors(&callback_errors);

    assert_removed_operation_result("fallocate", fallocate_result, &errors);
    assert_removed_operation_result("lseek", lseek_data_result, &errors);
    assert_removed_operation_result("lseek", lseek_hole_result, &errors);
    assert_removed_operation_result("copy_file_range", copy_result, &errors);
    expect_no_events(
        &mut events,
        &filesystem,
        "removed Linux data operations do not append events",
    );
}

#[cfg(target_os = "macos")]
#[test]
fn mounted_macos_volume_name_exchange_and_extended_times_are_event_aware_when_supported() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let left_path = root.join("left");
    let right_path = root.join("right");
    let times_path = root.join("times");
    let mut events = event_count(&filesystem);

    match set_volume_name(root, "eventfs-mounted-test") {
        Ok(()) => {
            if events_after(&filesystem, events).is_empty() {
                eprintln!(
                    "skipping setvolname assertion because this mount accepted setattrlist without routing setvolname"
                );
            } else {
                let renamed = expect_event_kinds(
                    &mut events,
                    &filesystem,
                    &[EventKind::VolumeRenamed],
                    "setvolname appends one volume-renamed event",
                );
                assert_eq!(renamed[0].path(), Some("/"));
                assert_eq!(
                    get_volume_name(root).expect("volume name is readable"),
                    "eventfs-mounted-test"
                );
            }
        }
        Err(error) if macos_optional_operation_may_be_unrouted(error.raw_os_error()) => {
            eprintln!("skipping setvolname assertion because this macFUSE mount returned {error}");
        }
        Err(error) => panic!("setvolname failed: {error}"),
    }

    write_mounted_file(&left_path, b"left").expect("left file is written");
    write_mounted_file(&right_path, b"right").expect("right file is written");
    events = event_count(&filesystem);
    if let Err(error) = exchange_data(&left_path, &right_path) {
        if macos_optional_operation_may_be_unrouted(error.raw_os_error()) {
            eprintln!(
                "skipping exchangedata assertion because this macFUSE mount returned {error}"
            );
            return;
        }
        panic!("exchangedata failed: {error}");
    }

    let exchanged = expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileContentsExchanged],
        "exchangedata appends one file-contents-exchanged event",
    );
    assert_eq!(exchanged[0].path(), Some("/left"));
    assert_eq!(exchanged[0].secondary_path(), Some("/right"));
    assert!(exchanged[0].file_identifier().is_some());
    assert!(exchanged[0].secondary_file_identifier().is_some());
    assert_eq!(
        fs::read(&left_path).expect("left file is readable"),
        b"right"
    );
    assert_eq!(
        fs::read(&right_path).expect("right file is readable"),
        b"left"
    );

    write_mounted_file(&times_path, b"times").expect("times file is written");
    let crtime = UNIX_EPOCH + Duration::from_secs(1_800_000_001);
    let bkuptime = UNIX_EPOCH + Duration::from_secs(1_800_000_101);
    events = event_count(&filesystem);
    set_file_crtime_and_bkuptime(&times_path, crtime, bkuptime)
        .expect_err("creation and backup time updates are unsupported");
    expect_no_events(
        &mut events,
        &filesystem,
        "unsupported extended time update does not append events",
    );

    let (actual_crtime, actual_bkuptime) =
        get_file_xtimes(&times_path).expect("extended times are read");
    assert_ne!(actual_crtime, crtime);
    assert_ne!(actual_bkuptime, bkuptime);
    expect_no_events(&mut events, &filesystem, "getxtimes does not append events");
}

fn set_lock(
    fd: libc::c_int,
    typ: libc::c_short,
    start: libc::off_t,
    len: libc::off_t,
) -> std::io::Result<()> {
    let mut lock = file_lock(typ, start, len);
    syscall_zero(unsafe { libc::fcntl(fd, libc::F_SETLK, &mut lock) })
}

fn get_lock(
    fd: libc::c_int,
    typ: libc::c_short,
    start: libc::off_t,
    len: libc::off_t,
) -> std::io::Result<libc::flock> {
    let mut lock = file_lock(typ, start, len);
    syscall_zero(unsafe { libc::fcntl(fd, libc::F_GETLK, &mut lock) })?;
    Ok(lock)
}

fn file_lock(typ: libc::c_short, start: libc::off_t, len: libc::off_t) -> libc::flock {
    libc::flock {
        l_type: typ,
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
        libc::copy_file_range(
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

#[cfg(target_os = "macos")]
fn set_volume_name(path: &Path, name: &str) -> std::io::Result<()> {
    let path = c_path(path);
    let mut attributes = empty_attrlist();
    attributes.volattr = libc::ATTR_VOL_NAME;
    let name = std::ffi::CString::new(name).expect("volume name has no interior NUL bytes");
    let reference_size = std::mem::size_of::<libc::attrreference_t>();
    let mut buffer = vec![0; reference_size + name.as_bytes_with_nul().len()];
    let reference = libc::attrreference_t {
        attr_dataoffset: reference_size as i32,
        attr_length: name.as_bytes_with_nul().len() as u32,
    };
    unsafe {
        std::ptr::write_unaligned(
            buffer.as_mut_ptr().cast::<libc::attrreference_t>(),
            reference,
        );
        std::ptr::copy_nonoverlapping(
            name.as_ptr().cast::<u8>(),
            buffer.as_mut_ptr().add(reference_size),
            name.as_bytes_with_nul().len(),
        );
    }

    syscall_zero(unsafe {
        libc::setattrlist(
            path.as_ptr(),
            (&mut attributes as *mut libc::attrlist).cast(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            0,
        )
    })
}

#[cfg(target_os = "macos")]
fn get_volume_name(path: &Path) -> std::io::Result<String> {
    let path = c_path(path);
    let mut attributes = empty_attrlist();
    attributes.volattr = libc::ATTR_VOL_NAME;
    let mut buffer = vec![0u8; 512];
    syscall_zero(unsafe {
        libc::getattrlist(
            path.as_ptr(),
            (&mut attributes as *mut libc::attrlist).cast(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            0,
        )
    })?;

    let reference_offset = std::mem::size_of::<u32>();
    let reference = unsafe {
        std::ptr::read_unaligned(
            buffer
                .as_ptr()
                .add(reference_offset)
                .cast::<libc::attrreference_t>(),
        )
    };
    let start = reference_offset + reference.attr_dataoffset as usize;
    let end = start + reference.attr_length as usize;
    let bytes = &buffer[start..end.saturating_sub(1)];
    String::from_utf8(bytes.to_vec())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

#[cfg(target_os = "macos")]
fn exchange_data(left: &Path, right: &Path) -> std::io::Result<()> {
    let left = c_path(left);
    let right = c_path(right);
    syscall_zero(unsafe { libc::exchangedata(left.as_ptr(), right.as_ptr(), 0) })
}

#[cfg(target_os = "macos")]
fn set_file_crtime_and_bkuptime(
    path: &Path,
    crtime: std::time::SystemTime,
    bkuptime: std::time::SystemTime,
) -> std::io::Result<()> {
    let path = c_path(path);
    let mut attributes = empty_attrlist();
    attributes.commonattr = libc::ATTR_CMN_CRTIME | libc::ATTR_CMN_BKUPTIME;
    let mut times = PackedTimes {
        crtime: timespec_from_system_time(crtime),
        bkuptime: timespec_from_system_time(bkuptime),
    };
    syscall_zero(unsafe {
        libc::setattrlist(
            path.as_ptr(),
            (&mut attributes as *mut libc::attrlist).cast(),
            (&mut times as *mut PackedTimes).cast(),
            std::mem::size_of::<PackedTimes>(),
            0,
        )
    })
}

#[cfg(target_os = "macos")]
fn get_file_xtimes(path: &Path) -> std::io::Result<(std::time::SystemTime, std::time::SystemTime)> {
    let path = c_path(path);
    let mut attributes = empty_attrlist();
    attributes.commonattr = libc::ATTR_CMN_CRTIME | libc::ATTR_CMN_BKUPTIME;
    let mut times = PackedReturnedTimes {
        length: 0,
        crtime: zero_timespec(),
        bkuptime: zero_timespec(),
    };
    syscall_zero(unsafe {
        libc::getattrlist(
            path.as_ptr(),
            (&mut attributes as *mut libc::attrlist).cast(),
            (&mut times as *mut PackedReturnedTimes).cast(),
            std::mem::size_of::<PackedReturnedTimes>(),
            0,
        )
    })?;
    let crtime = times.crtime;
    let bkuptime = times.bkuptime;
    Ok((
        system_time_from_timespec(crtime),
        system_time_from_timespec(bkuptime),
    ))
}

#[cfg(target_os = "macos")]
#[repr(C, packed(4))]
struct PackedTimes {
    crtime: libc::timespec,
    bkuptime: libc::timespec,
}

#[cfg(target_os = "macos")]
#[repr(C, packed(4))]
struct PackedReturnedTimes {
    length: u32,
    crtime: libc::timespec,
    bkuptime: libc::timespec,
}

#[cfg(target_os = "macos")]
fn empty_attrlist() -> libc::attrlist {
    libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: 0,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    }
}

#[cfg(target_os = "macos")]
fn timespec_from_system_time(time: std::time::SystemTime) -> libc::timespec {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch");
    libc::timespec {
        tv_sec: duration.as_secs() as _,
        tv_nsec: duration.subsec_nanos() as _,
    }
}

#[cfg(target_os = "macos")]
fn system_time_from_timespec(time: libc::timespec) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::new(time.tv_sec as u64, time.tv_nsec as u32)
}

#[cfg(target_os = "macos")]
fn zero_timespec() -> libc::timespec {
    libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
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

#[cfg(target_os = "macos")]
fn unsupported_ioctl_command() -> libc::c_ulong {
    0
}

#[cfg(target_os = "macos")]
fn macos_optional_operation_may_be_unrouted(errno: Option<i32>) -> bool {
    matches!(errno, Some(libc::ENOSYS | libc::ENOTSUP | libc::EINVAL))
}

fn c_path(path: &Path) -> std::ffi::CString {
    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
