mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;

use eventfs::EventKind;

use support::{
    TestDirectories, create_empty_file, event_count, expect_event_kinds, expect_no_events,
    fsync_directory, mount, open_test_filesystem,
};

#[test]
fn mounted_directory_lifecycle_and_listing_operations_have_expected_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let directory = root.join("directory");
    let mut events = event_count(&filesystem);

    fs::create_dir(&directory).expect("directory is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "mkdir appends a directory-created event",
    );

    assert!(
        fs::read_dir(root)
            .expect("directory is opened and read")
            .any(|entry| entry.expect("directory entry is readable").file_name() == "directory")
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "opendir, readdir, and releasedir do not append events",
    );

    fsync_directory(&directory);
    expect_no_events(&mut events, &filesystem, "fsyncdir does not append events");

    fs::remove_dir(&directory).expect("directory is removed");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryRemoved],
        "rmdir appends a directory-removed event",
    );
}

#[test]
fn mounted_readdir_and_readdirplus_return_large_entry_sets_without_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let directory_path = directories.mount_point_path().join("large-directory");
    let mut expected_names = BTreeSet::new();
    let mut expected_lengths = BTreeMap::new();

    fs::create_dir(&directory_path).expect("large directory is created");
    for index in 0..128 {
        let name = format!("entry-{index:03}");
        let bytes = format!("value-{index}");
        fs::write(directory_path.join(&name), &bytes).expect("directory entry is created");
        expected_names.insert(name.clone());
        expected_lengths.insert(name, bytes.len() as u64);
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

    let events_before_readdirplus = event_count(&filesystem);
    let listed_with_attributes = read_directory_entries_with_attributes(&directory_path);
    assert_eq!(listed_with_attributes, expected_lengths);
    assert_eq!(
        event_count(&filesystem),
        events_before_readdirplus,
        "directory listing with attributes does not append events"
    );
}

#[test]
fn mounted_directory_rename_updates_parent_link_counts_and_appends_one_event() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
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
    let mut events = event_count(&filesystem);

    fs::rename(&moved, &moved_target).expect("directory is renamed across parents");

    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeRenamed],
        "directory rename appends one node-renamed event",
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
    events = event_count(&filesystem);

    fs::rename(&replacement_source, &replacement_target)
        .expect("directory rename replaces an empty directory");

    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::NodeRenamed],
        "directory replacement rename appends one node-renamed event",
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
}

#[test]
fn mounted_filesystem_accepts_realistic_name_edges_and_rejects_invalid_names_without_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let maximum_name = "a".repeat(255);
    let maximum_utf8_name = "é".repeat(127);
    let too_long_name = "b".repeat(256);
    let too_long_utf8_name = "é".repeat(128);
    let mut events = event_count(&filesystem);

    fs::write(root.join(".hidden"), b"ok").expect("leading dot name is accepted");
    fs::write(root.join("name with spaces"), b"ok").expect("space name is accepted");
    fs::write(root.join(&maximum_name), b"ok").expect("maximum length name is accepted");
    fs::write(root.join(&maximum_utf8_name), b"ok")
        .expect("maximum utf-8 byte length below 255 is accepted");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[
            EventKind::FileCreated,
            EventKind::FileWritten,
            EventKind::FileCreated,
            EventKind::FileWritten,
            EventKind::FileCreated,
            EventKind::FileWritten,
            EventKind::FileCreated,
            EventKind::FileWritten,
        ],
        "accepted edge-case names append create and write events",
    );

    assert!(
        fs::write(root.join(&too_long_name), b"rejected").is_err(),
        "overlong name is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "overlong ascii name does not append events",
    );

    assert!(
        fs::write(root.join(&too_long_utf8_name), b"rejected").is_err(),
        "overlong utf-8 byte length is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "overlong utf-8 name does not append events",
    );

    let invalid_utf8 = root.join(std::ffi::OsString::from_vec(vec![0xff]));
    assert!(
        fs::write(invalid_utf8, b"rejected").is_err(),
        "non-utf8 name is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "non-utf8 name does not append events",
    );
}

#[test]
fn mounted_directory_edge_case_failures_do_not_append_events() {
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
        "file create appends a file-created event",
    );
    fs::create_dir(&directory).expect("directory is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "directory create appends one event",
    );
    create_empty_file(&directory.join("child"));
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileCreated],
        "child create appends one event",
    );

    assert!(
        fs::remove_dir(&directory).is_err(),
        "non-empty directory removal is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed rmdir does not append events",
    );
    assert!(
        fs::remove_file(&directory).is_err(),
        "directory unlink as file is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed directory unlink does not append events",
    );
    assert!(
        fs::remove_dir(&file_path).is_err(),
        "file rmdir is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed file rmdir does not append events",
    );

    let destination_directory = root.join("destination-directory");
    fs::create_dir(&destination_directory).expect("destination directory is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "destination directory create appends one event",
    );
    assert!(
        fs::rename(&file_path, &destination_directory).is_err(),
        "file rename over directory is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed file-over-directory rename does not append events",
    );

    let destination_file = root.join("destination-file");
    create_empty_file(&destination_file);
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileCreated],
        "destination file create appends one event",
    );
    assert!(
        fs::rename(&directory, &destination_file).is_err(),
        "directory rename over file is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed directory-over-file rename does not append events",
    );

    let non_empty_target = root.join("non-empty-target");
    fs::create_dir(&non_empty_target).expect("non-empty target is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "target directory create appends one event",
    );
    create_empty_file(&non_empty_target.join("child"));
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::FileCreated],
        "target child create appends one event",
    );
    let empty_source = root.join("empty-source");
    fs::create_dir(&empty_source).expect("empty source is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "empty source create appends one event",
    );
    assert!(
        fs::rename(&empty_source, &non_empty_target).is_err(),
        "directory rename over non-empty directory is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed non-empty-directory rename does not append events",
    );

    let cycle_source = root.join("cycle-source");
    let cycle_child = cycle_source.join("child");
    fs::create_dir(&cycle_source).expect("cycle source directory is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "cycle source create appends one event",
    );
    fs::create_dir(&cycle_child).expect("cycle child directory is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::DirectoryCreated],
        "cycle child create appends one event",
    );
    assert!(
        fs::rename(&cycle_source, cycle_child.join("moved")).is_err(),
        "directory rename into its descendant is rejected"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed descendant rename does not append events",
    );
}

fn read_directory_entries_with_attributes(path: &std::path::Path) -> BTreeMap<String, u64> {
    fs::read_dir(path)
        .expect("directory is read with attributes")
        .map(|entry| {
            let entry = entry.expect("directory entry is readable");
            let name = entry
                .file_name()
                .into_string()
                .expect("directory entry name is utf-8");
            let length = entry
                .metadata()
                .expect("directory entry attributes are readable")
                .len();
            (name, length)
        })
        .collect()
}
