mod support;

use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use eventfs::{Filesystem, FilesystemError};

use support::{
    TestDirectories, assert_callback_errors_include, event_count,
    filesystem_with_fuse_error_callback, open_test_filesystem, recorded_callback_errors, statvfs,
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
fn mounted_filesystem_statistics_report_backing_blocks_and_logical_inode_capacity_without_events() {
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
    let events_before_statistics = event_count(&filesystem);
    assert_backing_statfs_matches(&before, &backing);
    assert_ne!(before.f_bsize, 0);
    assert_eq!(before.f_namemax, 255);
    assert_eq!(before.f_files as u64, expected_inode_capacity());
    assert_eq!(before.f_ffree as u64 + 1, before.f_files as u64);
    assert_eq!(event_count(&filesystem), events_before_statistics);

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
    assert_eq!(after_hard_link.f_files, after_create.f_files);
    assert_eq!(after_hard_link.f_ffree, after_create.f_ffree);

    fs::rename(&file_path, &renamed_path).expect("file is renamed");
    let after_rename = statvfs(root);
    assert_eq!(after_rename.f_files, after_create.f_files);
    assert_eq!(after_rename.f_ffree, after_create.f_ffree);

    fs::remove_file(&renamed_path).expect("renamed file is removed");
    fs::remove_file(&hard_link_path).expect("hard link is removed");

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
    let inode_before = std::os::unix::fs::MetadataExt::ino(
        &fs::metadata(&file_path).expect("file metadata is readable"),
    );
    mounted.unmount().expect("filesystem unmounts");
    drop(filesystem);

    let filesystem = Filesystem::open(directories.configuration()).expect("filesystem reopens");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem remounts in the background");
    let inode_after = std::os::unix::fs::MetadataExt::ino(
        &fs::metadata(&file_path).expect("file metadata is readable after remount"),
    );

    assert_eq!(inode_after, inode_before);

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
    let configuration = support::configuration_for(
        directories.root_path().join("mount-failure-database"),
        directories.root_path().join("missing-mount-point"),
    );
    let filesystem = Filesystem::open(configuration).expect("filesystem opens");

    let error = filesystem
        .spawn_mount()
        .expect_err("missing mount point is rejected");

    assert_eq!(error, FilesystemError::FilesystemOperation);
}

#[cfg(target_os = "macos")]
fn expected_inode_capacity() -> u64 {
    u64::from(u32::MAX - 1)
}

#[cfg(not(target_os = "macos"))]
fn expected_inode_capacity() -> u64 {
    u64::MAX - 1
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
