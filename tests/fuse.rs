mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsString};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventfs::{Filesystem, FilesystemError, FuseOperationError};

use support::{
    TestDirectories, access_path, assert_event_sequences_increase, configuration_for,
    event_page_limit, file_identifier_for_path, fsync_directory, get_xattr, list_all_events,
    list_xattr, mkfifo, mount, open_test_filesystem, remove_xattr, set_xattr, statvfs,
};

#[test]
fn spawn_mount_exposes_the_filesystem_at_the_configured_mount_point() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");

    fs::metadata(directories.mount_point_path()).expect("mounted root attributes are readable");

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_supports_directory_operations_and_filesystem_statistics() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let directory_path = directories.mount_point_path().join("directory");

    fs::create_dir(&directory_path).expect("directory is created");
    assert!(
        fs::metadata(&directory_path)
            .expect("directory attributes are readable")
            .is_dir()
    );
    assert!(
        fs::read_dir(directories.mount_point_path())
            .expect("directory is readable")
            .any(|entry| entry.expect("directory entry is readable").file_name() == "directory")
    );
    let events_before_statistics = list_all_events(&filesystem).len();
    let mount_statistics = statvfs(directories.mount_point_path());
    let backing_statistics = statvfs(directories.database_directory_path());
    assert_backing_statfs_matches(&mount_statistics, &backing_statistics);
    assert_ne!(mount_statistics.f_bsize, 0);
    assert_eq!(mount_statistics.f_namemax, 255);
    assert_eq!(list_all_events(&filesystem).len(), events_before_statistics);

    fs::remove_dir(&directory_path).expect("directory is removed");

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_directory_rename_updates_parent_link_counts() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let root = directories.mount_point_path();

    let old_parent = root.join("old-parent");
    let new_parent = root.join("new-parent");
    let moved = old_parent.join("moved");
    let moved_target = new_parent.join("moved");
    fs::create_dir(&old_parent).expect("old parent is created");
    fs::create_dir(&new_parent).expect("new parent is created");
    fs::create_dir(&moved).expect("moved directory is created");
    fs::write(moved.join("child"), b"contents").expect("moved child is written");

    let moved_inode = fs::metadata(&moved)
        .expect("moved directory metadata is readable")
        .ino();
    let old_parent_links = fs::metadata(&old_parent)
        .expect("old parent metadata is readable")
        .nlink();
    let new_parent_links = fs::metadata(&new_parent)
        .expect("new parent metadata is readable")
        .nlink();
    let mut events = list_all_events(&filesystem).len();

    fs::rename(&moved, &moved_target).expect("directory is renamed across parents");

    expect_events(
        &mut events,
        &filesystem,
        1,
        "directory rename appends one event",
    );
    assert!(
        fs::metadata(&moved).is_err(),
        "old directory path is removed"
    );
    let moved_metadata =
        fs::metadata(&moved_target).expect("renamed directory metadata is readable");
    assert!(moved_metadata.is_dir());
    assert_eq!(moved_metadata.ino(), moved_inode);
    assert_eq!(
        fs::read(moved_target.join("child")).expect("renamed child is readable"),
        b"contents"
    );
    assert_eq!(
        fs::metadata(&old_parent)
            .expect("old parent metadata is reread")
            .nlink(),
        old_parent_links - 1
    );
    assert_eq!(
        fs::metadata(&new_parent)
            .expect("new parent metadata is reread")
            .nlink(),
        new_parent_links + 1
    );

    let replacement_source_parent = root.join("replacement-source-parent");
    let replacement_target_parent = root.join("replacement-target-parent");
    let replacement_source = replacement_source_parent.join("directory");
    let replacement_target = replacement_target_parent.join("directory");
    fs::create_dir(&replacement_source_parent).expect("replacement source parent is created");
    fs::create_dir(&replacement_target_parent).expect("replacement target parent is created");
    fs::create_dir(&replacement_source).expect("replacement source directory is created");
    fs::write(replacement_source.join("child"), b"replacement")
        .expect("replacement source child is written");
    fs::create_dir(&replacement_target).expect("replacement target directory is created");

    let replacement_source_inode = fs::metadata(&replacement_source)
        .expect("replacement source metadata is readable")
        .ino();
    let source_parent_links = fs::metadata(&replacement_source_parent)
        .expect("replacement source parent metadata is readable")
        .nlink();
    let target_parent_links = fs::metadata(&replacement_target_parent)
        .expect("replacement target parent metadata is readable")
        .nlink();
    events = list_all_events(&filesystem).len();

    fs::rename(&replacement_source, &replacement_target)
        .expect("directory rename replaces an empty directory");

    expect_events(
        &mut events,
        &filesystem,
        1,
        "directory replacement rename appends one event",
    );
    assert!(
        fs::metadata(&replacement_source).is_err(),
        "replacement source path is removed"
    );
    assert_eq!(
        fs::metadata(&replacement_target)
            .expect("replacement target metadata is readable")
            .ino(),
        replacement_source_inode
    );
    assert_eq!(
        fs::read(replacement_target.join("child")).expect("replacement child is readable"),
        b"replacement"
    );
    assert_eq!(
        fs::metadata(&replacement_source_parent)
            .expect("replacement source parent metadata is reread")
            .nlink(),
        source_parent_links - 1
    );
    assert_eq!(
        fs::metadata(&replacement_target_parent)
            .expect("replacement target parent metadata is reread")
            .nlink(),
        target_parent_links
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_statfs_reports_logical_inode_capacity() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let root = directories.mount_point_path();
    let file_path = root.join("file");
    let hard_link_path = root.join("file-link");
    let renamed_path = root.join("renamed");

    let before = statvfs(root);
    let backing = statvfs(directories.database_directory_path());
    assert_backing_statfs_matches(&before, &backing);
    assert_eq!(before.f_files as u64, expected_inode_capacity());
    assert_eq!(before.f_ffree as u64 + 1, before.f_files as u64);

    fs::write(&file_path, b"contents").expect("file is written");
    let after_create = statvfs(root);
    assert_backing_statfs_matches(
        &after_create,
        &statvfs(directories.database_directory_path()),
    );
    assert_eq!(after_create.f_files, before.f_files);
    assert_eq!(after_create.f_ffree as u64 + 1, before.f_ffree as u64);

    fs::hard_link(&file_path, &hard_link_path).expect("hard link is created");
    let after_hard_link = statvfs(root);
    assert_backing_statfs_matches(
        &after_hard_link,
        &statvfs(directories.database_directory_path()),
    );
    assert_eq!(after_hard_link.f_files, after_create.f_files);
    assert_eq!(after_hard_link.f_ffree, after_create.f_ffree);

    fs::rename(&file_path, &renamed_path).expect("file is renamed");
    let after_rename = statvfs(root);
    assert_backing_statfs_matches(
        &after_rename,
        &statvfs(directories.database_directory_path()),
    );
    assert_eq!(after_rename.f_files, after_create.f_files);
    assert_eq!(after_rename.f_ffree, after_create.f_ffree);

    fs::remove_file(&renamed_path).expect("renamed file is removed");
    fs::remove_file(&hard_link_path).expect("hard link is removed");

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_readdir_lists_large_entry_sets() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let directory_path = directories.mount_point_path().join("large-directory");
    let mut expected_names = BTreeSet::new();

    fs::create_dir(&directory_path).expect("large directory is created");
    for index in 0..128 {
        let name = format!("entry-{index:03}");
        fs::write(directory_path.join(&name), format!("value-{index}"))
            .expect("directory entry is created");
        expected_names.insert(name);
    }

    let events_before_listing = event_count(&filesystem);
    let listed_names = fs::read_dir(&directory_path)
        .expect("large directory is read")
        .map(|entry| {
            entry
                .expect("directory entry is readable")
                .file_name()
                .into_string()
                .expect("directory entry name is utf-8")
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(listed_names, expected_names);
    assert_eq!(
        event_count(&filesystem),
        events_before_listing,
        "readdir of a large directory does not append events"
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_accepts_name_edges_and_rejects_names_longer_than_255_bytes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let root = directories.mount_point_path();
    let maximum_name = "a".repeat(255);
    let maximum_utf8_name = "é".repeat(127);
    let too_long_name = "b".repeat(256);
    let too_long_utf8_name = "é".repeat(128);

    fs::write(root.join(".hidden"), b"ok").expect("leading dot name is accepted");
    fs::write(root.join("name with spaces"), b"ok").expect("space name is accepted");
    fs::write(root.join(&maximum_name), b"ok").expect("maximum length name is accepted");
    fs::write(root.join(&maximum_utf8_name), b"ok")
        .expect("maximum utf-8 byte length below 255 is accepted");
    assert!(
        fs::write(root.join(&too_long_name), b"rejected").is_err(),
        "overlong name is rejected"
    );
    assert!(
        fs::write(root.join(&too_long_utf8_name), b"rejected").is_err(),
        "overlong utf-8 byte length is rejected"
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_supports_file_create_open_read_write_truncate_flush_sync_and_release() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("file");

    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&file_path)
        .expect("file is created");

    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("file opens");

    file.write_all(b"abcdef").expect("file is written");
    file.flush().expect("file is flushed");
    file.sync_all().expect("file is synchronized");

    file.set_len(3).expect("file is truncated");
    drop(file);

    let mut bytes = Vec::new();
    fs::File::open(&file_path)
        .expect("file reopens")
        .read_to_end(&mut bytes)
        .expect("file is read");
    assert_eq!(bytes, b"abc");

    fs::remove_file(&file_path).expect("file is unlinked");

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_supports_metadata_change_rename_hard_link_symlink_and_readlink() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("file");
    let renamed_path = directories.mount_point_path().join("renamed");
    let hard_link_path = directories.mount_point_path().join("hard-link");
    let symlink_path = directories.mount_point_path().join("symlink");

    fs::write(&file_path, b"contents").expect("file is written");
    let inode_before_rename = fs::metadata(&file_path)
        .expect("file metadata is readable")
        .ino();

    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o600))
        .expect("file metadata is changed");
    fs::hard_link(&file_path, &hard_link_path).expect("hard link is created");
    let hard_link_metadata = fs::metadata(&hard_link_path).expect("hard link metadata is readable");
    assert_eq!(hard_link_metadata.ino(), inode_before_rename);
    assert_eq!(hard_link_metadata.nlink(), 2);
    symlink(&file_path, &symlink_path).expect("symbolic link is created");
    let symlink_metadata =
        fs::symlink_metadata(&symlink_path).expect("symbolic link metadata is readable");
    assert!(symlink_metadata.file_type().is_symlink());
    assert_eq!(
        fs::read_link(&symlink_path).expect("symbolic link target is read"),
        file_path
    );
    fs::rename(&file_path, &renamed_path).expect("node is renamed");
    let renamed_metadata = fs::metadata(&renamed_path).expect("renamed metadata is readable");
    assert_eq!(renamed_metadata.ino(), inode_before_rename);
    assert_eq!(renamed_metadata.nlink(), 2);
    assert_eq!(renamed_metadata.permissions().mode() & 0o777, 0o600);

    fs::remove_file(&renamed_path).expect("renamed file is unlinked");
    fs::remove_file(&hard_link_path).expect("hard link is unlinked");
    fs::remove_file(&symlink_path).expect("symbolic link is unlinked");

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_updates_atime_and_mtime_metadata() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("timestamps");
    let atime = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mtime = UNIX_EPOCH + Duration::from_secs(1_700_000_123);

    fs::write(&file_path, b"contents").expect("timestamp file is written");
    let ctime_before = fs::metadata(&file_path)
        .expect("timestamp file metadata is readable")
        .ctime();
    let mut events = event_count(&filesystem);

    set_file_times(&file_path, atime, mtime);
    expect_events(
        &mut events,
        &filesystem,
        1,
        "setting atime and mtime appends one metadata event",
    );

    let metadata = fs::metadata(&file_path).expect("timestamp file metadata is reread");
    assert_eq!(metadata_atime(&metadata), system_time_parts(atime));
    assert_eq!(metadata_mtime(&metadata), system_time_parts(mtime));
    assert!(metadata.ctime() >= ctime_before);

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_filesystem_supports_node_creation_and_access_checks() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let node_path = directories.mount_point_path().join("fifo");

    assert_command_succeeds(
        Command::new("mkfifo")
            .arg(&node_path)
            .status()
            .expect("node creation command runs"),
    );
    assert_command_succeeds(
        Command::new("test")
            .arg("-r")
            .arg(&node_path)
            .status()
            .expect("access check command runs"),
    );
    fs::remove_file(&node_path).expect("node is unlinked");

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn inode_numbers_are_stable_across_process_restarts() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("stable-inode");

    fs::write(&file_path, b"contents").expect("file is written");
    let inode_before = fs::metadata(&file_path)
        .expect("file metadata is readable")
        .ino();
    mounted.unmount().expect("filesystem unmounts");
    drop(filesystem);

    let filesystem = Filesystem::open(directories.configuration()).expect("filesystem reopens");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem remounts in the background");
    let inode_after = fs::metadata(&file_path)
        .expect("file metadata is readable after remount")
        .ino();

    assert_eq!(inode_after, inode_before);

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mounted_extended_attributes_round_trip() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("file");
    let name = "user.eventfs.supported";

    fs::write(&file_path, b"contents").expect("file is written");
    set_xattr(&file_path, name, b"value", libc::XATTR_CREATE).expect("xattr is created");
    assert_eq!(
        get_xattr(&file_path, name).expect("xattr value is read"),
        b"value"
    );
    assert!(
        list_xattr(&file_path)
            .expect("xattr list is read")
            .windows(name.len())
            .any(|window| window == name.as_bytes()),
        "xattr list includes the created attribute"
    );
    set_xattr(&file_path, name, b"replacement", libc::XATTR_REPLACE).expect("xattr is replaced");
    assert_eq!(
        get_xattr(&file_path, name).expect("replacement xattr value is read"),
        b"replacement"
    );
    remove_xattr(&file_path, name).expect("xattr is removed");
    assert!(
        get_xattr(&file_path, name).is_err(),
        "removed xattr is no longer readable"
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn fuse_error_callback_receives_supported_operation_failures() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");

    fs::metadata(directories.mount_point_path().join("missing"))
        .expect_err("missing path metadata fails");

    assert_callback_errors_include(
        &recorded_callback_errors(&callback_errors),
        "lookup",
        libc::ENOENT,
        false,
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn fuse_error_callback_panic_does_not_change_fuse_operation_error() {
    let directories = TestDirectories::new();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_for_configuration = Arc::clone(&callback_count);
    let configuration = directories
        .configuration()
        .with_fuse_error_callback(move |_error| {
            callback_count_for_configuration.fetch_add(1, Ordering::SeqCst);
            panic!("callback panic is isolated");
        });
    let filesystem = Filesystem::open(configuration).expect("filesystem opens");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");

    let error = fs::metadata(directories.mount_point_path().join("missing"))
        .expect_err("missing path metadata fails");

    assert_eq!(error.raw_os_error(), Some(libc::ENOENT));
    assert!(
        callback_count.load(Ordering::SeqCst) > 0,
        "callback runs before its panic is isolated"
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn fuse_error_callback_receives_supported_xattr_failures() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("file");
    let name = "user.eventfs.missing";

    fs::write(&file_path, b"contents").expect("file is written");
    let mut events = list_all_events(&filesystem).len();
    let error = set_xattr(&file_path, name, b"value", libc::XATTR_REPLACE)
        .expect_err("replacing a missing xattr fails");

    let errors = recorded_callback_errors(&callback_errors);
    assert_callback_errors_include(&errors, "setxattr", error.raw_os_error().unwrap(), false);
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed xattr replace does not append events",
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn fuse_error_callback_is_not_called_for_successful_operations() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let root = directories.mount_point_path();

    fs::metadata(root).expect("root metadata is read");
    statvfs(root);

    assert!(
        recorded_callback_errors(&callback_errors).is_empty(),
        "successful FUSE operations do not invoke the error callback"
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn mount_failures_map_to_filesystem_operation_errors() {
    let directories = TestDirectories::new();
    let configuration = configuration_for(
        directories.root_path().join("mount-failure-database"),
        directories.root_path().join("missing-mount-point"),
    );
    let filesystem = Filesystem::open(configuration).expect("filesystem opens");

    let error = filesystem
        .spawn_mount()
        .expect_err("missing mount point is rejected");

    assert_eq!(error, FilesystemError::FilesystemOperation);
}

#[test]
fn mounted_supported_fuse_operations_have_expected_event_behavior() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let mut events = event_count(&filesystem);

    fs::metadata(root).expect("root lookup and attributes are readable");
    expect_events(
        &mut events,
        &filesystem,
        0,
        "lookup and getattr do not append events",
    );

    let directory = root.join("directory");
    fs::create_dir(&directory).expect("directory is created");
    expect_events(&mut events, &filesystem, 1, "mkdir appends one event");
    assert!(
        fs::read_dir(root)
            .expect("directory is opened and read")
            .any(|entry| entry.expect("directory entry is readable").file_name() == "directory")
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "opendir, readdir, and releasedir do not append events",
    );
    fsync_directory(&directory);
    expect_events(
        &mut events,
        &filesystem,
        0,
        "fsyncdir does not append events",
    );
    fs::remove_dir(&directory).expect("directory is removed");
    expect_events(&mut events, &filesystem, 1, "rmdir appends one event");

    let fifo = root.join("fifo");
    mkfifo(&fifo);
    expect_events(&mut events, &filesystem, 1, "mknod appends one event");
    assert_eq!(access_path(&fifo, libc::R_OK), 0);
    expect_events(&mut events, &filesystem, 0, "access does not append events");
    fs::remove_file(&fifo).expect("fifo is unlinked");
    expect_events(&mut events, &filesystem, 1, "unlink appends one event");

    let file_path = root.join("file");
    create_empty_file(&file_path);
    expect_events(&mut events, &filesystem, 1, "create appends one event");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("file opens");
    expect_events(&mut events, &filesystem, 0, "open does not append events");
    assert_eq!(file.write(b"abcdef").expect("file is written"), 6);
    expect_events(&mut events, &filesystem, 1, "write appends one event");
    file.flush().expect("file is flushed");
    expect_events(&mut events, &filesystem, 0, "flush does not append events");
    file.sync_all().expect("file is synchronized");
    expect_events(&mut events, &filesystem, 0, "fsync does not append events");
    file.set_len(3).expect("file is truncated");
    expect_events(&mut events, &filesystem, 1, "truncate appends one event");
    drop(file);
    expect_events(
        &mut events,
        &filesystem,
        0,
        "release does not append events",
    );

    let mut contents = Vec::new();
    fs::File::open(&file_path)
        .expect("file reopens")
        .read_to_end(&mut contents)
        .expect("file is read");
    assert_eq!(contents, b"abc");
    expect_events(&mut events, &filesystem, 0, "read does not append events");

    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o600))
        .expect("file metadata is updated");
    expect_events(&mut events, &filesystem, 1, "setattr appends one event");

    let replacement_source = root.join("replacement-source");
    let replacement_target = root.join("replacement-target");
    create_empty_file(&replacement_source);
    expect_events(
        &mut events,
        &filesystem,
        1,
        "replacement source create appends one event",
    );
    create_empty_file(&replacement_target);
    expect_events(
        &mut events,
        &filesystem,
        1,
        "replacement target create appends one event",
    );
    fs::rename(&replacement_source, &replacement_target).expect("rename replaces a file");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "rename replacement appends one event",
    );

    let hard_link_path = root.join("hard-link");
    fs::hard_link(&file_path, &hard_link_path).expect("hard link is created");
    expect_events(&mut events, &filesystem, 1, "hard link appends one event");
    let symlink_path = root.join("symlink");
    symlink(&file_path, &symlink_path).expect("symbolic link is created");
    expect_events(&mut events, &filesystem, 1, "symlink appends one event");
    assert_eq!(
        fs::read_link(&symlink_path).expect("symbolic link is read"),
        file_path
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "readlink does not append events",
    );

    let renamed_path = root.join("renamed");
    fs::rename(&file_path, &renamed_path).expect("file is renamed");
    expect_events(&mut events, &filesystem, 1, "rename appends one event");
    fs::remove_file(&hard_link_path).expect("hard link is unlinked");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "hard link unlink appends one event",
    );
    fs::remove_file(&symlink_path).expect("symbolic link is unlinked");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "symlink unlink appends one event",
    );
    fs::remove_file(&renamed_path).expect("file is unlinked");
    expect_events(&mut events, &filesystem, 1, "file unlink appends one event");
    fs::remove_file(&replacement_target).expect("replacement target is unlinked");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "replacement unlink appends one event",
    );

    let _statistics = statvfs(root);
    expect_events(&mut events, &filesystem, 0, "statfs does not append events");
}

#[test]
fn mounted_fuse_edge_cases_fail_without_appending_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let mut events = event_count(&filesystem);

    let file_path = root.join("file");
    create_empty_file(&file_path);
    expect_events(&mut events, &filesystem, 1, "file create appends one event");
    assert!(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&file_path)
            .is_err(),
        "duplicate file create is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "duplicate create does not append events",
    );

    assert!(
        fs::write(root.join("x".repeat(256)), b"rejected").is_err(),
        "overlong name is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "overlong name does not append events",
    );

    let invalid_utf8 = root.join(OsString::from_vec(vec![0xff]));
    assert!(
        fs::write(invalid_utf8, b"rejected").is_err(),
        "non-utf8 name is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "non-utf8 name does not append events",
    );

    assert!(
        fs::read(root.join("missing")).is_err(),
        "missing file read is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "missing read does not append events",
    );

    let directory = root.join("directory");
    fs::create_dir(&directory).expect("directory is created");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "directory create appends one event",
    );
    create_empty_file(&directory.join("child"));
    expect_events(
        &mut events,
        &filesystem,
        1,
        "child create appends one event",
    );
    assert!(
        fs::remove_dir(&directory).is_err(),
        "non-empty directory removal is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed rmdir does not append events",
    );
    assert!(
        fs::remove_file(&directory).is_err(),
        "directory unlink as file is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed directory unlink does not append events",
    );
    assert!(
        fs::remove_dir(&file_path).is_err(),
        "file rmdir is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed file rmdir does not append events",
    );

    let destination_directory = root.join("destination-directory");
    fs::create_dir(&destination_directory).expect("destination directory is created");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "destination directory create appends one event",
    );
    assert!(
        fs::rename(&file_path, &destination_directory).is_err(),
        "file rename over directory is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed file-over-directory rename does not append events",
    );

    let destination_file = root.join("destination-file");
    create_empty_file(&destination_file);
    expect_events(
        &mut events,
        &filesystem,
        1,
        "destination file create appends one event",
    );
    assert!(
        fs::rename(&directory, &destination_file).is_err(),
        "directory rename over file is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed directory-over-file rename does not append events",
    );

    let non_empty_target = root.join("non-empty-target");
    fs::create_dir(&non_empty_target).expect("non-empty target is created");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "target directory create appends one event",
    );
    create_empty_file(&non_empty_target.join("child"));
    expect_events(
        &mut events,
        &filesystem,
        1,
        "target child create appends one event",
    );
    let empty_source = root.join("empty-source");
    fs::create_dir(&empty_source).expect("empty source is created");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "empty source create appends one event",
    );
    assert!(
        fs::rename(&empty_source, &non_empty_target).is_err(),
        "directory rename over non-empty directory is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed non-empty-directory rename does not append events",
    );

    let cycle_source = root.join("cycle-source");
    let cycle_child = cycle_source.join("child");
    fs::create_dir(&cycle_source).expect("cycle source directory is created");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "cycle source create appends one event",
    );
    fs::create_dir(&cycle_child).expect("cycle child directory is created");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "cycle child create appends one event",
    );
    assert!(
        fs::rename(&cycle_source, cycle_child.join("moved")).is_err(),
        "directory rename into its descendant is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed descendant rename does not append events",
    );

    assert!(
        fs::hard_link(&directory, root.join("directory-hard-link")).is_err(),
        "directory hard link is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed hard link does not append events",
    );
    assert!(
        fs::read_link(&file_path).is_err(),
        "regular file readlink is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "failed readlink does not append events",
    );

    let xattr_name = "user.eventfs.event-count";
    set_xattr(&file_path, xattr_name, b"value", 0).expect("xattr is set");
    expect_events(&mut events, &filesystem, 1, "setxattr appends one event");
    assert_eq!(
        get_xattr(&file_path, xattr_name).expect("xattr is read"),
        b"value"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "getxattr does not append events",
    );
    list_xattr(&file_path).expect("xattrs are listed");
    expect_events(
        &mut events,
        &filesystem,
        0,
        "listxattr does not append events",
    );
    remove_xattr(&file_path, xattr_name).expect("xattr is removed");
    expect_events(&mut events, &filesystem, 1, "removexattr appends one event");
}

#[test]
fn mounted_permission_failures_do_not_append_events_when_not_running_as_root() {
    if running_as_root() {
        eprintln!(
            "skipping permission-denial assertions because uid 0 bypasses the mounted access checks"
        );
        return;
    }

    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let file_path = root.join("locked-file");
    let locked_directory = root.join("locked-directory");

    fs::write(&file_path, b"contents").expect("locked file is written");
    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o000))
        .expect("locked file permissions are restricted");
    fs::create_dir(&locked_directory).expect("locked directory is created");
    fs::set_permissions(&locked_directory, fs::Permissions::from_mode(0o555))
        .expect("locked directory permissions are restricted");
    let mut events = event_count(&filesystem);

    assert!(
        OpenOptions::new().write(true).open(&file_path).is_err(),
        "write open on a mode-restricted file is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "file-mode write denial does not append events",
    );
    assert!(
        fs::read(&file_path).is_err(),
        "read on a mode-restricted file is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "file-mode read denial does not append events",
    );
    assert_eq!(access_path(&file_path, libc::R_OK), -1);
    expect_events(
        &mut events,
        &filesystem,
        0,
        "file-mode access denial does not append events",
    );
    assert!(
        fs::write(locked_directory.join("child"), b"denied").is_err(),
        "create under a non-writable parent directory is rejected"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "parent-directory write denial does not append events",
    );
}

#[cfg(target_os = "macos")]
#[test]
fn mounted_preallocate_operation_succeeds() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let file_path = root.join("file");

    create_empty_file(&file_path);
    let mut events = event_count(&filesystem);

    if let Err(error) = preallocate(&file_path) {
        eprintln!("skipping preallocate assertion because this macFUSE mount returned {error}");
        return;
    }

    let length = fs::metadata(&file_path)
        .expect("file metadata remains readable after preallocate")
        .len();
    let delta = usize::from(length > 0);
    expect_events(&mut events, &filesystem, delta, "preallocate event count");
}

#[cfg(target_os = "macos")]
#[test]
fn mounted_punch_hole_operation_zeroes_range_and_appends_event() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let file_path = root.join("file");

    fs::write(&file_path, b"contents").expect("file is written");
    let mut events = event_count(&filesystem);

    if let Err(error) = punch_hole(&file_path) {
        eprintln!("skipping punch-hole assertion because this macFUSE mount returned {error}");
        return;
    }

    expect_events(&mut events, &filesystem, 1, "punch hole appends one event");
    assert_eq!(
        fs::read(&file_path).expect("file contents remain readable after punch hole"),
        b"co\0\0\0ts"
    );
    assert_eq!(
        fs::metadata(&file_path)
            .expect("file metadata remains readable after punch hole")
            .len(),
        8
    );
}

#[cfg(target_os = "macos")]
#[test]
fn mounted_exchange_operation_swaps_file_contents_and_appends_event() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let left_path = root.join("left");
    let right_path = root.join("right");

    fs::write(&left_path, b"left").expect("left file is written");
    fs::write(&right_path, b"right").expect("right file is written");
    let mut events = event_count(&filesystem);

    if let Err(error) = exchange_data(&left_path, &right_path) {
        eprintln!("skipping exchangedata assertion because this macFUSE mount returned {error}");
        return;
    }

    expect_events(
        &mut events,
        &filesystem,
        1,
        "exchangedata appends one event",
    );
    assert_eq!(
        fs::read(&left_path).expect("left file is readable"),
        b"right"
    );
    assert_eq!(
        fs::read(&right_path).expect("right file is readable"),
        b"left"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn mounted_seek_hole_operation_succeeds_without_appending_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let file_path = root.join("file");

    fs::write(&file_path, b"contents").expect("file is written");
    let mut events = event_count(&filesystem);

    let Ok(offset) = seek_hole(&file_path) else {
        eprintln!("skipping SEEK_HOLE assertion because this macFUSE mount did not route lseek");
        return;
    };

    assert_eq!(offset, 8);
    expect_events(
        &mut events,
        &filesystem,
        0,
        "seek-hole does not append events",
    );
}

#[cfg(target_os = "linux")]
#[test]
fn mounted_copy_file_range_copies_bytes_and_appends_event() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let source_path = root.join("source");
    let destination_path = root.join("destination");

    fs::write(&source_path, b"copy source").expect("copy source file is written");
    fs::write(&destination_path, b"destination").expect("destination file is written");
    let mut events = event_count(&filesystem);

    let copied =
        copy_file_range(&source_path, &destination_path).expect("copy_file_range succeeds");

    assert_eq!(copied, 4);
    expect_events(
        &mut events,
        &filesystem,
        1,
        "copy_file_range appends one event",
    );
    assert_eq!(
        fs::read(&destination_path).expect("destination is readable"),
        b"copyination"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn mounted_filesystem_supports_linux_rename_noreplace() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let source_path = root.join("source");
    let renamed_path = root.join("renamed");
    let collision_source = root.join("collision-source");
    let collision_target = root.join("collision-target");

    fs::write(&source_path, b"source").expect("source file is written");
    let mut events = event_count(&filesystem);

    rename_noreplace(&source_path, &renamed_path).expect("rename_noreplace succeeds");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "rename_noreplace appends one event when the target is absent",
    );
    assert_eq!(
        fs::read(&renamed_path).expect("renamed file is readable"),
        b"source"
    );

    fs::write(&collision_source, b"collision source").expect("collision source is written");
    fs::write(&collision_target, b"collision target").expect("collision target is written");
    events = event_count(&filesystem);

    let error = rename_noreplace(&collision_source, &collision_target)
        .expect_err("rename_noreplace denies replacement");

    assert_eq!(error.raw_os_error(), Some(libc::EEXIST));
    expect_events(
        &mut events,
        &filesystem,
        0,
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

#[test]
fn mounted_file_read_write_and_truncate_edges_project_contents() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let mut events = event_count(&filesystem);
    let file_path = root.join("contents");

    create_empty_file(&file_path);
    expect_events(&mut events, &filesystem, 1, "file create appends one event");
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("file opens");
        file.seek(SeekFrom::Start(4)).expect("file seek succeeds");
        assert_eq!(file.write(b"xy").expect("sparse write succeeds"), 2);
        expect_events(
            &mut events,
            &filesystem,
            1,
            "sparse write appends one event",
        );
    }
    assert_eq!(
        fs::read(&file_path).expect("sparse file is read"),
        b"\0\0\0\0xy"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "sparse read does not append events",
    );

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("file reopens");
    file.set_len(3).expect("file is shrunk");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "shrink truncate appends one event",
    );
    assert_eq!(
        fs::read(&file_path).expect("shrunk file is read"),
        b"\0\0\0"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "shrunk read does not append events",
    );
    file.set_len(6).expect("file is grown");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "grow truncate appends one event",
    );
    assert_eq!(
        fs::read(&file_path).expect("grown file is read"),
        b"\0\0\0\0\0\0"
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "grown read does not append events",
    );
    file.seek(SeekFrom::Start(20))
        .expect("seek past EOF succeeds");
    let mut bytes = [0; 4];
    assert_eq!(file.read(&mut bytes).expect("EOF read succeeds"), 0);
    expect_events(
        &mut events,
        &filesystem,
        0,
        "EOF read does not append events",
    );
    file.set_len(0).expect("file is truncated to zero");
    expect_events(
        &mut events,
        &filesystem,
        1,
        "zero truncate appends one event",
    );
    drop(file);
    assert!(
        fs::read(&file_path)
            .expect("zero-length file is read")
            .is_empty()
    );
    expect_events(
        &mut events,
        &filesystem,
        0,
        "zero-length read does not append events",
    );
}

#[test]
fn concurrent_mounted_writes_and_event_listing_preserve_final_contents() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let watched_path = directories.mount_point_path().join("listed-file");
    fs::write(&watched_path, b"seed").expect("listed file is created");
    let watched_file_identifier = file_identifier_for_path(&filesystem, "/listed-file");
    let barrier = Arc::new(Barrier::new(5));
    let mut writers = Vec::new();

    for thread_index in 0..4 {
        let barrier = Arc::clone(&barrier);
        let root = directories.mount_point_path().to_path_buf();
        let watched_path = watched_path.clone();
        writers.push(thread::spawn(move || {
            barrier.wait();
            for file_index in 0..6 {
                let path = root.join(format!("thread-{thread_index}-file-{file_index}"));
                let initial = initial_bytes(thread_index, file_index);
                fs::write(&path, &initial).expect("concurrent file is written");
                let replacement = replacement_bytes(thread_index, file_index);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("concurrent file opens");
                file.seek(SeekFrom::Start(0))
                    .expect("concurrent file seeks");
                file.write_all(&replacement)
                    .expect("concurrent file is overwritten");
                file.set_len(12).expect("concurrent file is truncated");
                file.flush().expect("concurrent file flushes");
                if thread_index == 0 {
                    fs::write(&watched_path, format!("watched-{file_index}"))
                        .expect("listed file is updated under load");
                }
            }
        }));
    }

    let listing_filesystem = filesystem.clone();
    let listing_barrier = Arc::clone(&barrier);
    let lister = thread::spawn(move || {
        listing_barrier.wait();
        for _ in 0..40 {
            let events = list_all_events(&listing_filesystem);
            assert_event_sequences_increase(&events);
            let file_events = listing_filesystem
                .list_file_events(watched_file_identifier, None, event_page_limit(100))
                .expect("file events are listed under load");
            assert_event_sequences_increase(file_events.records());
            thread::yield_now();
        }
    });

    for writer in writers {
        writer.join().expect("writer thread joins");
    }
    lister.join().expect("listing thread joins");

    for thread_index in 0..4 {
        for file_index in 0..6 {
            let path = directories
                .mount_point_path()
                .join(format!("thread-{thread_index}-file-{file_index}"));
            assert_eq!(
                fs::read(path).expect("concurrent final file is read"),
                expected_bytes(thread_index, file_index)
            );
        }
    }
    assert_eq!(
        fs::read(&watched_path).expect("listed file final contents are read"),
        b"watched-5"
    );
    assert!(
        filesystem
            .list_file_events(watched_file_identifier, None, event_page_limit(100))
            .expect("listed file events are available after load")
            .records()
            .len()
            > 1,
        "file event listing observed the updated file"
    );
    mounted.unmount().expect("filesystem unmounts");
    assert_event_sequences_increase(&list_all_events(&filesystem));
}

#[test]
fn mounted_fuse_stress_repeats_supported_operation_combinations() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let work = root.join("work");
    let archive = root.join("archive");
    let links = root.join("links");
    let mut expected = BTreeMap::new();

    fs::create_dir(&work).expect("work directory is created");
    fs::create_dir(&archive).expect("archive directory is created");
    fs::create_dir(&links).expect("links directory is created");
    for index in 0..3 {
        let directory = work.join(format!("dir-{index}"));
        fs::create_dir(&directory).expect("nested directory is created");
        fsync_directory(&directory);
    }

    for index in 0..12 {
        let file_name = format!("file-{index}");
        let source = work.join(format!("dir-{}", index % 3)).join(&file_name);
        let hard_link = links.join(format!("hard-{index}"));
        let symbolic_link = links.join(format!("sym-{index}"));
        let fifo = links.join(format!("fifo-{index}"));
        let renamed = archive.join(format!("renamed-{index}"));
        let mut bytes = format!("stress-file-{index}-initial").into_bytes();

        fs::write(&source, &bytes).expect("stress file is written");
        mutate_file(&source, index, &mut bytes);
        fs::set_permissions(&source, fs::Permissions::from_mode(0o600))
            .expect("stress file metadata is updated");
        assert_eq!(access_path(&source, libc::R_OK), 0);
        assert_eq!(statvfs(root).f_namemax, 255);
        fs::hard_link(&source, &hard_link).expect("hard link is created");
        assert_eq!(fs::read(&hard_link).expect("hard link is read"), bytes);
        symlink(&source, &symbolic_link).expect("symbolic link is created");
        assert_eq!(
            fs::read_link(&symbolic_link).expect("symbolic link target is read"),
            source
        );
        mkfifo(&fifo);
        assert_eq!(access_path(&fifo, libc::R_OK), 0);
        fs::rename(&source, &renamed).expect("file is renamed into archive");
        assert_eq!(fs::read(&renamed).expect("renamed file is read"), bytes);
        assert_eq!(
            fs::read(&hard_link).expect("hard link remains readable after rename"),
            bytes
        );
        fs::remove_file(&symbolic_link).expect("symbolic link is removed");
        fs::remove_file(&hard_link).expect("hard link is removed");
        fs::remove_file(&fifo).expect("fifo is removed");
        fsync_directory(&archive);
        fsync_directory(&links);
        expected.insert(
            renamed.file_name().expect("file name exists").to_owned(),
            bytes,
        );
    }

    for index in 0..3 {
        fs::remove_dir(work.join(format!("dir-{index}"))).expect("nested directory is removed");
    }
    fs::remove_dir(&work).expect("work directory is removed");
    fs::remove_dir(&links).expect("links directory is removed");
    assert_event_sequences_increase(&list_all_events(&filesystem));
    mounted.unmount().expect("filesystem unmounts");

    let mounted = mount(&filesystem);
    for (file_name, bytes) in expected {
        assert_eq!(
            fs::read(archive.join(file_name)).expect("archived file is readable after remount"),
            bytes
        );
    }
    assert_event_sequences_increase(&list_all_events(&filesystem));
    mounted.unmount().expect("filesystem unmounts");
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

fn running_as_root() -> bool {
    (unsafe { libc::geteuid() }) == 0
}

#[cfg(target_os = "macos")]
fn expected_inode_capacity() -> u64 {
    u64::from(u32::MAX - 1)
}

#[cfg(not(target_os = "macos"))]
fn expected_inode_capacity() -> u64 {
    u64::MAX - 1
}

fn c_path(path: &Path) -> CString {
    CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}

#[cfg(target_os = "macos")]
fn preallocate(path: &Path) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

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
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut allocation) };
    if result != -1 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn punch_hole(path: &Path) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

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
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &mut hole) };
    if result != -1 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn exchange_data(left: &Path, right: &Path) -> std::io::Result<()> {
    let left = c_path(left);
    let right = c_path(right);
    let result = unsafe { libc::exchangedata(left.as_ptr(), right.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn seek_hole(path: &Path) -> std::io::Result<u64> {
    use std::os::fd::AsRawFd;

    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .expect("seek-hole file opens");
    let result = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_HOLE) };
    if result >= 0 {
        Ok(result as u64)
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn copy_file_range(source: &Path, destination: &Path) -> std::io::Result<usize> {
    use std::os::fd::AsRawFd;

    let source = OpenOptions::new()
        .read(true)
        .open(source)
        .expect("copy source file opens");
    let destination = OpenOptions::new()
        .write(true)
        .open(destination)
        .expect("copy destination file opens");
    let mut source_offset: libc::loff_t = 0;
    let mut destination_offset: libc::loff_t = 0;
    let result = unsafe {
        libc::copy_file_range(
            source.as_raw_fd(),
            &mut source_offset,
            destination.as_raw_fd(),
            &mut destination_offset,
            4,
            0,
        )
    };
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
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

fn filesystem_with_fuse_error_callback(
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

fn recorded_callback_errors(
    errors: &Arc<Mutex<Vec<FuseOperationError>>>,
) -> Vec<FuseOperationError> {
    errors
        .lock()
        .expect("callback error collection lock is available")
        .clone()
}

fn assert_callback_errors_include(
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

fn assert_command_succeeds(status: std::process::ExitStatus) {
    assert!(status.success(), "command exits successfully");
}

fn create_empty_file(path: &Path) {
    drop(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .expect("empty file is created"),
    );
}

fn event_count(filesystem: &Filesystem) -> usize {
    list_all_events(filesystem).len()
}

fn expect_events(events: &mut usize, filesystem: &Filesystem, delta: usize, message: &str) {
    *events += delta;
    assert_eq!(event_count(filesystem), *events, "{message}");
}

fn initial_bytes(thread_index: usize, file_index: usize) -> Vec<u8> {
    format!("thread-{thread_index}-file-{file_index}-initial").into_bytes()
}

fn replacement_bytes(thread_index: usize, file_index: usize) -> Vec<u8> {
    format!("T{thread_index}F{file_index}").into_bytes()
}

fn expected_bytes(thread_index: usize, file_index: usize) -> Vec<u8> {
    let mut bytes = initial_bytes(thread_index, file_index);
    let replacement = replacement_bytes(thread_index, file_index);
    bytes[..replacement.len()].copy_from_slice(&replacement);
    bytes.truncate(12);
    bytes
}

fn assert_backing_statfs_matches(
    mount_statistics: &libc::statvfs,
    backing_statistics: &libc::statvfs,
) {
    assert_eq!(mount_statistics.f_blocks, backing_statistics.f_blocks);
    assert_eq!(mount_statistics.f_bfree, backing_statistics.f_bfree);
    assert_eq!(mount_statistics.f_bavail, backing_statistics.f_bavail);
    assert_eq!(mount_statistics.f_frsize, backing_statistics.f_frsize);
}

fn mutate_file(path: &Path, index: usize, bytes: &mut Vec<u8>) {
    let patch = format!("P{index:02}").into_bytes();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("stress file opens");
    file.seek(SeekFrom::Start(3)).expect("stress file seeks");
    file.write_all(&patch).expect("stress file is overwritten");
    bytes[3..3 + patch.len()].copy_from_slice(&patch);
    if index.is_multiple_of(2) {
        file.set_len((bytes.len() + 5) as u64)
            .expect("stress file grows");
        bytes.resize(bytes.len() + 5, 0);
    } else {
        file.set_len((bytes.len() - 2) as u64)
            .expect("stress file shrinks");
        bytes.truncate(bytes.len() - 2);
    }
    file.flush().expect("stress file flushes");
    file.sync_all().expect("stress file syncs");
}
