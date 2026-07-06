mod support;

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use eventfs::EventKind;

use support::{
    TestDirectories, assert_callback_errors_include, create_empty_file, event_count, events_after,
    expect_event_kinds, expect_no_events, filesystem_with_fuse_error_callback, mount,
    open_test_filesystem, recorded_callback_errors,
};

#[test]
fn mounted_posix_locks_succeed_and_do_not_append_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("lock-file");

    fs::write(&file_path, b"locks").expect("lock file is written");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("lock file opens");
    let mut events = event_count(&filesystem);

    set_lock(file.as_raw_fd(), libc::F_WRLCK, 0, 3).expect("write lock is set");
    expect_no_events(&mut events, &filesystem, "setlk does not append events");
    let queried = get_lock(file.as_raw_fd(), libc::F_WRLCK, 0, 3).expect("lock is queried");
    assert_eq!(queried.l_type, libc::F_UNLCK as libc::c_short);
    expect_no_events(&mut events, &filesystem, "getlk does not append events");
    set_lock(file.as_raw_fd(), libc::F_UNLCK, 0, 3).expect("lock is released");
    expect_no_events(&mut events, &filesystem, "unlock does not append events");

    drop(file);
    expect_no_events(
        &mut events,
        &filesystem,
        "release after locking does not append events",
    );
}

#[test]
fn mounted_ioctl_rejection_reports_enotty_and_poll_reports_ready_without_events() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("ioctl-file");

    fs::write(&file_path, b"ioctl").expect("ioctl file is written");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("ioctl file opens");
    let mut events = event_count(&filesystem);

    let error = ioctl_rejection(file.as_raw_fd()).expect_err("unsupported ioctl is rejected");
    assert_eq!(error.raw_os_error(), Some(libc::ENOTTY));
    let errors = recorded_callback_errors(&callback_errors);
    if cfg!(target_os = "macos") && !errors.iter().any(|error| error.operation() == "ioctl") {
        eprintln!("skipping ioctl callback assertion because this mount did not route ioctl");
    } else {
        assert_callback_errors_include(&errors, "ioctl", libc::ENOTTY, false);
    }
    expect_no_events(
        &mut events,
        &filesystem,
        "ioctl rejection does not append events",
    );

    let ready = poll_file(file.as_raw_fd()).expect("poll succeeds");
    assert_ne!(ready & libc::POLLIN, 0);
    assert_ne!(ready & libc::POLLOUT, 0);
    expect_no_events(&mut events, &filesystem, "poll does not append events");
}

#[cfg(target_os = "linux")]
#[test]
fn mounted_bmap_returns_the_requested_logical_block_without_events_when_driveable() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("bmap-file");

    fs::write(&file_path, b"bmap").expect("bmap file is written");
    let file = OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("bmap file opens");
    let mut events = event_count(&filesystem);

    match bmap(file.as_raw_fd(), 2) {
        Ok(block) => assert_eq!(block, 2),
        Err(error) if error.raw_os_error() == Some(libc::EPERM) => {
            eprintln!("skipping bmap assertion because the host denied FIBMAP");
            return;
        }
        Err(error) => panic!("bmap failed: {error}"),
    }
    expect_no_events(&mut events, &filesystem, "bmap does not append events");
}

#[cfg(target_os = "linux")]
#[test]
fn mounted_linux_fallocate_modes_append_events_only_for_logical_changes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();

    let extend_path = root.join("extend");
    create_empty_file(&extend_path);
    let extend_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&extend_path)
        .expect("extend file opens");
    let mut events = event_count(&filesystem);
    fallocate(extend_file.as_raw_fd(), 0, 0, 4096).expect("fallocate extends file");
    assert_eq!(
        fs::metadata(&extend_path)
            .expect("extended file metadata is readable")
            .len(),
        4096
    );
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileTruncated],
        "size-growing fallocate appends one truncate event",
    );

    let keep_size_path = root.join("keep-size");
    fs::write(&keep_size_path, b"keep").expect("keep-size file is written");
    let keep_size_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&keep_size_path)
        .expect("keep-size file opens");
    events = event_count(&filesystem);
    fallocate(
        keep_size_file.as_raw_fd(),
        libc::FALLOC_FL_KEEP_SIZE,
        0,
        4096,
    )
    .expect("keep-size fallocate succeeds");
    assert_eq!(
        fs::metadata(&keep_size_path)
            .expect("keep-size file metadata is readable")
            .len(),
        4
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "keep-size allocation does not append events",
    );

    let punch_path = root.join("punch");
    fs::write(&punch_path, b"abcdefghi").expect("punch file is written");
    let punch_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&punch_path)
        .expect("punch file opens");
    events = event_count(&filesystem);
    fallocate(
        punch_file.as_raw_fd(),
        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
        2,
        3,
    )
    .expect("punch-hole fallocate succeeds");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileRangeZeroed],
        "punch-hole allocation appends one zero-range event",
    );
    assert_eq!(
        fs::read(&punch_path).expect("punched file is readable"),
        b"ab\0\0\0fghi"
    );

    let zero_path = root.join("zero");
    fs::write(&zero_path, b"abcdefghi").expect("zero-range file is written");
    let zero_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&zero_path)
        .expect("zero-range file opens");
    events = event_count(&filesystem);
    fallocate(zero_file.as_raw_fd(), libc::FALLOC_FL_ZERO_RANGE, 4, 3)
        .expect("zero-range fallocate succeeds");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileRangeZeroed],
        "zero-range allocation appends one zero-range event",
    );
    assert_eq!(
        fs::read(&zero_path).expect("zero-range file is readable"),
        b"abcd\0\0\0hi"
    );

    for mode in [
        libc::FALLOC_FL_COLLAPSE_RANGE,
        libc::FALLOC_FL_INSERT_RANGE,
        libc::FALLOC_FL_UNSHARE_RANGE,
        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_ZERO_RANGE,
        0x4000_0000,
    ] {
        events = event_count(&filesystem);
        let error = fallocate(zero_file.as_raw_fd(), mode, 0, 1)
            .expect_err("unsupported fallocate mode is rejected");
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        expect_no_events(
            &mut events,
            &filesystem,
            "unsupported fallocate mode does not append events",
        );
    }
}

#[test]
fn mounted_sparse_seek_finds_data_and_holes_without_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("sparse");

    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("sparse file is created");
    file.seek(SeekFrom::Start(4096)).expect("sparse file seeks");
    file.write_all(b"data").expect("sparse data is written");
    file.set_len(8192).expect("sparse file length is set");
    let mut events = event_count(&filesystem);

    let data_from_start = match seek(file.as_raw_fd(), 0, libc::SEEK_DATA) {
        Ok(offset) => offset,
        Err(error) if seek_may_be_unrouted(error.raw_os_error()) => {
            eprintln!("skipping sparse seek assertion because the mount did not route lseek");
            return;
        }
        Err(error) => panic!("SEEK_DATA failed: {error}"),
    };
    assert_eq!(data_from_start, 4096);
    assert_eq!(
        seek(file.as_raw_fd(), 0, libc::SEEK_HOLE).expect("SEEK_HOLE from start succeeds"),
        0
    );
    assert_eq!(
        seek(file.as_raw_fd(), 4096, libc::SEEK_HOLE).expect("SEEK_HOLE after data succeeds"),
        4100
    );
    let error =
        seek(file.as_raw_fd(), 8192, libc::SEEK_DATA).expect_err("SEEK_DATA at EOF is rejected");
    assert_eq!(error.raw_os_error(), Some(libc::ENXIO));
    expect_no_events(&mut events, &filesystem, "lseek does not append events");
}

#[cfg(target_os = "linux")]
#[test]
fn mounted_copy_file_range_writes_destination_once_and_rejects_flags_without_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let source_path = root.join("source");
    let destination_path = root.join("destination");

    fs::write(&source_path, b"copy source").expect("copy source file is written");
    fs::write(&destination_path, b"destination").expect("destination file is written");
    let source = OpenOptions::new()
        .read(true)
        .open(&source_path)
        .expect("source opens");
    let destination = OpenOptions::new()
        .write(true)
        .open(&destination_path)
        .expect("destination opens");
    let mut events = event_count(&filesystem);

    let copied = copy_file_range(source.as_raw_fd(), destination.as_raw_fd(), 0, 0, 4, 0)
        .expect("copy_file_range succeeds");

    assert_eq!(copied, 4);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "copy_file_range appends one destination file-written event",
    );
    assert_eq!(
        fs::read(&destination_path).expect("destination is readable"),
        b"copyination"
    );

    let error = copy_file_range(source.as_raw_fd(), destination.as_raw_fd(), 0, 0, 1, 1)
        .expect_err("copy_file_range rejects non-empty flags");
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
    expect_no_events(
        &mut events,
        &filesystem,
        "copy_file_range flag rejection does not append events",
    );
}

#[cfg(target_os = "macos")]
#[test]
fn mounted_macos_preallocate_and_punch_hole_have_expected_events_when_supported() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let preallocate_path = root.join("preallocate");
    let punch_path = root.join("punch");

    create_empty_file(&preallocate_path);
    let mut events = event_count(&filesystem);
    if let Err(error) = preallocate(&preallocate_path) {
        eprintln!("skipping preallocate assertion because this macFUSE mount returned {error}");
    } else if fs::metadata(&preallocate_path)
        .expect("preallocated file metadata is readable")
        .len()
        > 0
    {
        expect_event_kinds(
            &mut events,
            &filesystem,
            &[EventKind::FileTruncated],
            "preallocate size extension appends one truncate event",
        );
    } else {
        expect_no_events(
            &mut events,
            &filesystem,
            "preallocate no-op does not append events",
        );
    }

    fs::write(&punch_path, b"contents").expect("punch file is written");
    events = event_count(&filesystem);
    if let Err(error) = punch_hole(&punch_path) {
        eprintln!("skipping punch-hole assertion because this macFUSE mount returned {error}");
        return;
    }

    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileRangeZeroed],
        "punch-hole appends one file-range-zeroed event",
    );
    assert_eq!(
        fs::read(&punch_path).expect("file contents remain readable after punch hole"),
        b"co\0\0\0ts"
    );
    assert_eq!(
        fs::metadata(&punch_path)
            .expect("file metadata remains readable after punch hole")
            .len(),
        8
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

    fs::write(&left_path, b"left").expect("left file is written");
    fs::write(&right_path, b"right").expect("right file is written");
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

    fs::write(&times_path, b"times").expect("times file is written");
    let crtime = UNIX_EPOCH + Duration::from_secs(1_800_000_001);
    let bkuptime = UNIX_EPOCH + Duration::from_secs(1_800_000_101);
    events = event_count(&filesystem);
    set_file_crtime_and_bkuptime(&times_path, crtime, bkuptime)
        .expect("creation and backup times are set");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::MetadataChanged],
        "setting extended times appends one metadata event",
    );

    let (actual_crtime, actual_bkuptime) =
        get_file_xtimes(&times_path).expect("extended times are read");
    assert_eq!(actual_crtime, crtime);
    assert_eq!(actual_bkuptime, bkuptime);
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

fn seek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> std::io::Result<libc::off_t> {
    let result = unsafe { libc::lseek(fd, offset, whence) };
    if result >= 0 {
        Ok(result)
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn bmap(fd: libc::c_int, block: libc::c_int) -> std::io::Result<libc::c_int> {
    let mut block = block;
    syscall_zero(unsafe {
        libc::ioctl(fd, linux_raw_sys::ioctl::FIBMAP as libc::Ioctl, &mut block)
    })?;
    Ok(block)
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
fn preallocate(path: &Path) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("preallocate file opens");
    let mut allocation = libc::fstore_t {
        fst_flags: libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: 4096,
        fst_bytesalloc: 0,
    };
    syscall_zero(unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut allocation) })
}

#[cfg(target_os = "macos")]
fn punch_hole(path: &Path) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("punch-hole file opens");
    let mut hole = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: 2,
        fp_length: 3,
    };
    syscall_zero(unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &mut hole) })
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

fn seek_may_be_unrouted(errno: Option<i32>) -> bool {
    matches!(
        errno,
        Some(libc::ENOSYS | libc::EINVAL | libc::ENOTSUP | libc::ENOTTY)
    )
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
