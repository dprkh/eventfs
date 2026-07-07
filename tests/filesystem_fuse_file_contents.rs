mod support;

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventfs::EventKind;

use support::{
    TestDirectories, create_empty_file, event_count, expect_event_kinds, expect_no_events, mount,
    open_test_filesystem, write_mounted_file,
};

#[test]
fn mounted_file_create_open_read_write_flush_sync_release_and_set_len_have_expected_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("file");
    let mut events = event_count(&filesystem);

    create_empty_file(&file_path);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileCreated],
        "create appends one file-created event",
    );

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("file opens");
    expect_no_events(&mut events, &filesystem, "open does not append events");

    assert_eq!(file.write(b"abcdef").expect("file is written"), 6);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "write appends one file-written event",
    );

    file.flush().expect("file is flushed");
    expect_no_events(&mut events, &filesystem, "flush does not append events");
    file.sync_all().expect("file is synchronized");
    expect_no_events(&mut events, &filesystem, "fsync does not append events");

    file.set_len(3).expect("set_len shrinks the file");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "shrinking set_len appends one file-written event",
    );

    file.set_len(8).expect("set_len extends the file");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "extending set_len appends one file-written event",
    );
    drop(file);
    expect_no_events(&mut events, &filesystem, "release does not append events");

    let mut bytes = Vec::new();
    fs::File::open(&file_path)
        .expect("file reopens")
        .read_to_end(&mut bytes)
        .expect("file is read");
    assert_eq!(bytes, b"abc\0\0\0\0\0");
    expect_no_events(&mut events, &filesystem, "read does not append events");

    truncate_file(&file_path, 2).expect("path truncate shrinks the file");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "path truncate appends one file-written event",
    );
    assert_eq!(fs::read(&file_path).expect("truncated file is read"), b"ab");

    fs::remove_file(&file_path).expect("file is unlinked");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeUnlinked],
        "unlink appends one node-unlinked event",
    );
}

#[test]
fn mounted_file_read_write_and_eof_edges_project_contents() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let mut events = event_count(&filesystem);
    let file_path = root.join("contents");

    create_empty_file(&file_path);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileCreated],
        "file create appends one event",
    );
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("file opens");
        file.seek(SeekFrom::Start(4)).expect("file seek succeeds");
        assert_eq!(file.write(b"xy").expect("sparse write succeeds"), 2);
        expect_event_kinds(
            &mut events,
            &filesystem,
            &[EventKind::FileWritten],
            "sparse write appends one event",
        );
    }
    assert_eq!(
        fs::read(&file_path).expect("sparse file is read"),
        b"\0\0\0\0xy"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "sparse read does not append events",
    );

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("file reopens");
    file.seek(SeekFrom::Start(20))
        .expect("seek past EOF succeeds");
    let mut bytes = [0; 4];
    assert_eq!(file.read(&mut bytes).expect("EOF read succeeds"), 0);
    expect_no_events(&mut events, &filesystem, "EOF read does not append events");
    drop(file);
    assert_eq!(
        fs::read(&file_path).expect("file is read after EOF checks"),
        b"\0\0\0\0xy"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "post-EOF read does not append events",
    );
}

#[test]
fn mounted_open_truncate_and_append_flags_match_sftp_expectations() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("open-flags");

    write_mounted_file(&file_path, b"abcdef").expect("flag file is written");
    let mut events = event_count(&filesystem);

    let mut truncated = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&file_path)
        .expect("existing file opens with truncate");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "open truncate appends one file-written event",
    );
    truncated
        .write_all(b"xy")
        .expect("write after truncate succeeds");
    let write_events = expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "post-truncate write appends one file-written event",
    );
    assert_eq!(write_events[0].offset(), Some(0));
    drop(truncated);
    assert_eq!(
        fs::read(&file_path).expect("truncated file is readable"),
        b"xy"
    );

    let mut appended = OpenOptions::new()
        .append(true)
        .open(&file_path)
        .expect("file opens for append");
    appended.write_all(b"zz").expect("append write succeeds");
    let append_events = expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileWritten],
        "append write appends one file-written event",
    );
    assert_eq!(append_events[0].offset(), Some(2));
    drop(appended);
    assert_eq!(
        fs::read(&file_path).expect("appended file is readable"),
        b"xyzz"
    );
}

#[test]
fn mounted_file_timestamp_updates_append_metadata_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("timestamps");
    let atime = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mtime = UNIX_EPOCH + Duration::from_secs(1_700_000_123);

    write_mounted_file(&file_path, b"contents").expect("timestamp file is written");
    let mut events = event_count(&filesystem);

    set_file_times(&file_path, atime, mtime).expect("timestamp update succeeds");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::MetadataChanged],
        "timestamp update appends one metadata-changed event",
    );
    let metadata = fs::metadata(&file_path).expect("timestamp metadata is readable");
    assert_eq!(
        system_time_seconds(metadata.accessed().expect("access time exists")),
        system_time_seconds(atime),
    );
    assert_eq!(
        system_time_seconds(metadata.modified().expect("modified time exists")),
        system_time_seconds(mtime),
    );

    set_file_times(&file_path, atime, mtime).expect("unchanged timestamp update succeeds");
    expect_no_events(
        &mut events,
        &filesystem,
        "unchanged timestamp update does not append events",
    );
}

fn set_file_times(path: &Path, atime: SystemTime, mtime: SystemTime) -> std::io::Result<()> {
    let path = c_path(path);
    let times = [
        timespec_from_system_time(atime),
        timespec_from_system_time(mtime),
    ];
    let result = unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

fn truncate_file(path: &Path, size: libc::off_t) -> std::io::Result<()> {
    let path = c_path(path);
    let result = unsafe { libc::truncate(path.as_ptr(), size) };
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

fn timespec_from_system_time(time: SystemTime) -> libc::timespec {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch");
    libc::timespec {
        tv_sec: duration.as_secs() as _,
        tv_nsec: duration.subsec_nanos() as _,
    }
}

fn system_time_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch")
        .as_secs()
}

fn c_path(path: &Path) -> std::ffi::CString {
    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
