mod support;

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventfs::EventKind;

use support::{
    TestDirectories, create_empty_file, event_count, expect_event_kinds, expect_no_events, mount,
    open_test_filesystem,
};

#[test]
fn mounted_file_create_open_read_write_truncate_flush_sync_and_release_have_expected_events() {
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

    file.set_len(3).expect("file is truncated");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileTruncated],
        "truncate appends one file-truncated event",
    );
    drop(file);
    expect_no_events(&mut events, &filesystem, "release does not append events");

    let mut bytes = Vec::new();
    fs::File::open(&file_path)
        .expect("file reopens")
        .read_to_end(&mut bytes)
        .expect("file is read");
    assert_eq!(bytes, b"abc");
    expect_no_events(&mut events, &filesystem, "read does not append events");

    fs::remove_file(&file_path).expect("file is unlinked");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeUnlinked],
        "unlink appends one node-unlinked event",
    );
}

#[test]
fn mounted_file_read_write_and_truncate_edges_project_contents() {
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
    file.set_len(3).expect("file is shrunk");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileTruncated],
        "shrink truncate appends one event",
    );
    assert_eq!(
        fs::read(&file_path).expect("shrunk file is read"),
        b"\0\0\0"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "shrunk read does not append events",
    );

    file.set_len(6).expect("file is grown");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileTruncated],
        "grow truncate appends one event",
    );
    assert_eq!(
        fs::read(&file_path).expect("grown file is read"),
        b"\0\0\0\0\0\0"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "grown read does not append events",
    );

    file.seek(SeekFrom::Start(20))
        .expect("seek past EOF succeeds");
    let mut bytes = [0; 4];
    assert_eq!(file.read(&mut bytes).expect("EOF read succeeds"), 0);
    expect_no_events(&mut events, &filesystem, "EOF read does not append events");

    file.set_len(0).expect("file is truncated to zero");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileTruncated],
        "zero truncate appends one event",
    );
    drop(file);
    assert!(
        fs::read(&file_path)
            .expect("zero-length file is read")
            .is_empty()
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "zero-length read does not append events",
    );
}

#[test]
fn mounted_file_timestamps_update_metadata_once() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("timestamps");
    let atime = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mtime = UNIX_EPOCH + Duration::from_secs(1_700_000_123);

    fs::write(&file_path, b"contents").expect("timestamp file is written");
    let ctime_before = fs::metadata(&file_path)
        .expect("timestamp file metadata is readable")
        .ctime();
    let mut events = event_count(&filesystem);

    set_file_times(&file_path, atime, mtime);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::MetadataChanged],
        "setting atime and mtime appends one metadata event",
    );

    let metadata = fs::metadata(&file_path).expect("timestamp file metadata is reread");
    assert_eq!(metadata_atime(&metadata), system_time_parts(atime));
    assert_eq!(metadata_mtime(&metadata), system_time_parts(mtime));
    assert!(metadata.ctime() >= ctime_before);
}

fn set_file_times(path: &Path, atime: SystemTime, mtime: SystemTime) {
    let path = c_path(path);
    let times = [
        timespec_from_system_time(atime),
        timespec_from_system_time(mtime),
    ];
    let result = unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(result, 0, "utimensat succeeds: {}", last_os_error());
}

fn metadata_atime(metadata: &fs::Metadata) -> (i64, i64) {
    (metadata.atime(), metadata.atime_nsec())
}

fn metadata_mtime(metadata: &fs::Metadata) -> (i64, i64) {
    (metadata.mtime(), metadata.mtime_nsec())
}

fn system_time_parts(time: SystemTime) -> (i64, i64) {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch");
    (
        duration.as_secs() as i64,
        i64::from(duration.subsec_nanos()),
    )
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

fn c_path(path: &Path) -> std::ffi::CString {
    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
