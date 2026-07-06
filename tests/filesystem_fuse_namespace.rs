mod support;

use std::fs::{self, OpenOptions};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

use eventfs::EventKind;

use support::{
    TestDirectories, access_path, create_empty_file, event_count, expect_event_kinds,
    expect_no_events, mkfifo, mount, open_test_filesystem, write_mounted_file,
};

#[test]
fn mounted_nodes_links_renames_and_readlink_have_expected_event_kinds() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let fifo_path = root.join("fifo");
    let file_path = root.join("file");
    let renamed_path = root.join("renamed");
    let hard_link_path = root.join("hard-link");
    let symlink_path = root.join("symlink");
    let mut events = event_count(&filesystem);

    mkfifo(&fifo_path);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeCreated],
        "mknod appends one node-created event",
    );
    assert_eq!(access_path(&fifo_path, libc::R_OK), 0);
    expect_no_events(&mut events, &filesystem, "access does not append events");
    fs::remove_file(&fifo_path).expect("fifo is unlinked");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeUnlinked],
        "unlinking a special node appends one node-unlinked event",
    );

    write_mounted_file(&file_path, b"contents").expect("file is written");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileCreated, EventKind::FileWritten],
        "writing a new file appends create and write events",
    );
    let inode_before_rename = fs::metadata(&file_path)
        .expect("file metadata is readable")
        .ino();

    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o600))
        .expect_err("chmod is unsupported");
    expect_no_events(
        &mut events,
        &filesystem,
        "unsupported chmod does not append events",
    );

    fs::hard_link(&file_path, &hard_link_path).expect("hard link is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::HardLinkCreated],
        "hard link appends one hard-link event",
    );
    let hard_link_metadata = fs::metadata(&hard_link_path).expect("hard link metadata is readable");
    assert_eq!(hard_link_metadata.ino(), inode_before_rename);
    assert_eq!(hard_link_metadata.nlink(), 2);

    symlink(&file_path, &symlink_path).expect("symbolic link is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::SymbolicLinkCreated],
        "symlink appends one symbolic-link event",
    );
    let symlink_metadata =
        fs::symlink_metadata(&symlink_path).expect("symbolic link metadata is readable");
    assert!(symlink_metadata.file_type().is_symlink());
    assert_eq!(
        fs::read_link(&symlink_path).expect("symbolic link target is read"),
        file_path
    );
    expect_no_events(&mut events, &filesystem, "readlink does not append events");

    fs::rename(&file_path, &renamed_path).expect("node is renamed");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeRenamed],
        "rename appends one node-renamed event",
    );
    let renamed_metadata = fs::metadata(&renamed_path).expect("renamed metadata is readable");
    assert_eq!(renamed_metadata.ino(), inode_before_rename);
    assert_eq!(renamed_metadata.nlink(), 2);
    assert_eq!(renamed_metadata.permissions().mode() & 0o777, 0o777);

    fs::remove_file(&renamed_path).expect("renamed file is unlinked");
    fs::remove_file(&hard_link_path).expect("hard link is unlinked");
    fs::remove_file(&symlink_path).expect("symbolic link is unlinked");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[
            EventKind::NodeUnlinked,
            EventKind::NodeUnlinked,
            EventKind::NodeUnlinked,
        ],
        "unlinking the visible nodes appends unlink events",
    );
}

#[test]
fn mounted_namespace_edge_case_failures_do_not_append_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let file_path = root.join("file");
    let directory = root.join("directory");
    let mut events = event_count(&filesystem);

    create_empty_file(&file_path);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileCreated],
        "file create appends one event",
    );
    assert!(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&file_path)
            .is_err(),
        "duplicate file create is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "duplicate create does not append events",
    );

    assert!(
        fs::read(root.join("missing")).is_err(),
        "missing file read is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "missing read does not append events",
    );

    fs::create_dir(&directory).expect("directory is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "directory create appends one event",
    );

    assert!(
        fs::hard_link(&directory, root.join("directory-hard-link")).is_err(),
        "directory hard link is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed hard link does not append events",
    );
    assert!(
        fs::read_link(&file_path).is_err(),
        "regular file readlink is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed readlink does not append events",
    );
}

#[cfg(target_os = "linux")]
#[test]
fn mounted_linux_rename_noreplace_succeeds_or_fails_without_extra_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let source_path = root.join("source");
    let renamed_path = root.join("renamed");
    let collision_source = root.join("collision-source");
    let collision_target = root.join("collision-target");

    write_mounted_file(&source_path, b"source").expect("source file is written");
    let mut events = event_count(&filesystem);

    rename_noreplace(&source_path, &renamed_path).expect("rename_noreplace succeeds");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeRenamed],
        "rename_noreplace appends one event when the target is absent",
    );
    assert_eq!(
        fs::read(&renamed_path).expect("renamed file is readable"),
        b"source"
    );

    write_mounted_file(&collision_source, b"collision source")
        .expect("collision source is written");
    write_mounted_file(&collision_target, b"collision target")
        .expect("collision target is written");
    events = event_count(&filesystem);

    let error = rename_noreplace(&collision_source, &collision_target)
        .expect_err("rename_noreplace denies replacement");

    assert_eq!(error.raw_os_error(), Some(libc::EEXIST));
    expect_no_events(
        &mut events,
        &filesystem,
        "rename_noreplace collision does not append events",
    );
    assert_eq!(
        fs::read(&collision_source).expect("source remains readable after collision"),
        b"collision source"
    );
    assert_eq!(
        fs::read(&collision_target).expect("target remains readable after collision"),
        b"collision target"
    );
}

#[cfg(target_os = "linux")]
fn rename_noreplace(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    let from = c_path(from);
    let to = c_path(to);
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn c_path(path: &std::path::Path) -> std::ffi::CString {
    use std::os::unix::ffi::OsStrExt;

    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

#[cfg(target_os = "linux")]
fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
