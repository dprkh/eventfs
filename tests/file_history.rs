mod support;

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;

use eventfs::{
    BranchName, EventKind, EventRecord, EventSequence, FileEventPayloadPart, FileIdentifier,
    Filesystem,
};

use support::{
    TestDirectories, event_page_limit, file_identifier_for_path, list_all_events,
    open_test_filesystem,
};

#[test]
fn event_records_expose_file_write_and_truncate_payload_sizes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("payload-file");

    fs::write(&file_path, b"hello").expect("file is written through mounted filesystem");
    fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("file opens")
        .set_len(2)
        .expect("file truncates");

    let events = list_all_events(&filesystem);
    let write = events
        .iter()
        .find(|event| event.kind() == EventKind::FileWritten)
        .expect("write event exists");
    let truncate = events
        .iter()
        .find(|event| event.kind() == EventKind::FileTruncated)
        .expect("truncate event exists");

    assert_eq!(write.old_file_size(), Some(0));
    assert_eq!(write.new_file_size(), Some(5));
    assert_eq!(write.offset(), Some(0));
    assert_eq!(write.byte_length(), Some(5));
    assert_eq!(write.overwritten_byte_length(), Some(0));
    assert_eq!(write.written_byte_length(), Some(5));
    assert_eq!(write.removed_byte_length(), None);

    assert_eq!(truncate.old_file_size(), Some(5));
    assert_eq!(truncate.new_file_size(), Some(2));
    assert_eq!(truncate.offset(), Some(2));
    assert_eq!(truncate.byte_length(), Some(3));
    assert_eq!(truncate.overwritten_byte_length(), None);
    assert_eq!(truncate.written_byte_length(), None);
    assert_eq!(truncate.removed_byte_length(), Some(3));

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn file_event_payload_range_reads_expose_write_and_truncate_bytes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("payload-file");

    fs::write(&file_path, b"hello").expect("file is written through mounted filesystem");
    fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("file opens")
        .set_len(2)
        .expect("file truncates");

    let events = list_all_events(&filesystem);
    let write = events
        .iter()
        .find(|event| event.kind() == EventKind::FileWritten)
        .expect("write event exists");
    let truncate = events
        .iter()
        .find(|event| event.kind() == EventKind::FileTruncated)
        .expect("truncate event exists");

    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                write.sequence(),
                FileEventPayloadPart::Overwritten,
                0,
                100
            )
            .expect("overwritten bytes are read"),
        b""
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(write.sequence(), FileEventPayloadPart::Written, 0, 100)
            .expect("written bytes are read"),
        b"hello"
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(write.sequence(), FileEventPayloadPart::Removed, 0, 100)
            .expect("missing removed bytes read as empty"),
        b""
    );

    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                truncate.sequence(),
                FileEventPayloadPart::Overwritten,
                0,
                100
            )
            .expect("missing overwritten bytes read as empty"),
        b""
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                truncate.sequence(),
                FileEventPayloadPart::Written,
                0,
                100
            )
            .expect("missing written bytes read as empty"),
        b""
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                truncate.sequence(),
                FileEventPayloadPart::Removed,
                0,
                100
            )
            .expect("removed bytes are read"),
        b"llo"
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                EventSequence::new(0),
                FileEventPayloadPart::Written,
                0,
                100
            )
            .expect("non-payload read succeeds"),
        b""
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn overwrite_write_events_expose_overwritten_payload_bytes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("overwrite-payload");

    fs::write(&file_path, b"hello").expect("file is written through mounted filesystem");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("file opens");
    file.seek(SeekFrom::Start(1)).expect("file seek succeeds");
    file.write_all(b"XY").expect("file overwrite succeeds");
    drop(file);

    let events = list_all_events(&filesystem);
    let file_identifier = file_identifier_for_path(&filesystem, "/overwrite-payload");
    let overwrite = events
        .iter()
        .find(|event| {
            event.kind() == EventKind::FileWritten
                && event.file_identifier() == Some(file_identifier)
                && event
                    .overwritten_byte_length()
                    .is_some_and(|length| length > 0)
        })
        .expect("overwrite event with overwritten bytes exists");
    let offset = usize::try_from(overwrite.offset().expect("write offset exists"))
        .expect("write offset fits usize");
    let overwritten_length = overwrite
        .overwritten_byte_length()
        .expect("overwritten length exists");
    let written_length = overwrite
        .written_byte_length()
        .expect("written length exists");
    let before_snapshot = filesystem
        .file_snapshot_at_or_before(
            file_identifier,
            EventSequence::new(overwrite.sequence().get() - 1),
        )
        .expect("pre-overwrite snapshot lookup succeeds")
        .expect("pre-overwrite snapshot exists");
    let before_bytes = filesystem
        .read_file_snapshot_range(&before_snapshot, 0, before_snapshot.file_size())
        .expect("pre-overwrite snapshot bytes are read");
    let after_snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, overwrite.sequence())
        .expect("post-overwrite snapshot lookup succeeds")
        .expect("post-overwrite snapshot exists");
    let after_bytes = filesystem
        .read_file_snapshot_range(&after_snapshot, 0, after_snapshot.file_size())
        .expect("post-overwrite snapshot bytes are read");
    let overwritten = filesystem
        .read_file_event_payload_range(
            overwrite.sequence(),
            FileEventPayloadPart::Overwritten,
            0,
            100,
        )
        .expect("overwritten bytes are read");
    let overwritten_tail = filesystem
        .read_file_event_payload_range(
            overwrite.sequence(),
            FileEventPayloadPart::Overwritten,
            1,
            100,
        )
        .expect("overwritten bytes clamp to available payload");
    let written = filesystem
        .read_file_event_payload_range(overwrite.sequence(), FileEventPayloadPart::Written, 0, 100)
        .expect("replacement bytes are read");

    assert_eq!(
        overwrite.byte_length(),
        Some(overwritten_length.max(written_length))
    );
    assert_eq!(
        overwrite.overwritten_byte_length(),
        Some(overwritten_length)
    );
    assert_eq!(overwrite.written_byte_length(), Some(written_length));
    assert_eq!(overwrite.removed_byte_length(), None);
    assert_eq!(
        overwritten.len(),
        usize::try_from(overwritten_length).expect("overwritten length fits usize"),
    );
    assert_eq!(
        written.len(),
        usize::try_from(written_length).expect("written length fits usize"),
    );
    assert!(!overwritten.is_empty(), "overwrite payload is not empty");
    assert_eq!(overwritten_tail, overwritten[1..]);
    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                overwrite.sequence(),
                FileEventPayloadPart::Removed,
                0,
                100,
            )
            .expect("missing removed bytes read as empty"),
        b""
    );
    assert_eq!(
        &before_bytes[offset..offset + overwritten.len()],
        overwritten.as_slice()
    );

    let mut expected_after = before_bytes.clone();
    let end = offset + written.len();
    if expected_after.len() < end {
        expected_after.resize(end, 0);
    }
    expected_after[offset..end].copy_from_slice(&written);
    expected_after.resize(
        usize::try_from(overwrite.new_file_size().expect("new size exists"))
            .expect("new size fits usize"),
        0,
    );
    assert_eq!(expected_after, after_bytes);

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn per_file_event_listing_paginates_in_active_branch_order_with_cursors() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let first_path = directories.mount_point_path().join("first");
    let second_path = directories.mount_point_path().join("second");

    fs::write(&first_path, b"alpha").expect("first file is written through mounted filesystem");
    fs::write(&second_path, b"other").expect("second file is written through mounted filesystem");

    let mut first = fs::OpenOptions::new()
        .write(true)
        .open(&first_path)
        .expect("first file opens");
    first
        .seek(SeekFrom::Start(5))
        .expect("first file seek to end succeeds");
    first.write_all(b"!").expect("first file append succeeds");
    first
        .seek(SeekFrom::Start(1))
        .expect("first file seek to overwrite succeeds");
    first
        .write_all(b"ZZ")
        .expect("first file overwrite succeeds");
    first.set_len(4).expect("first file truncates");
    drop(first);

    let first_identifier = file_identifier_for_path(&filesystem, "/first");
    let first_page = filesystem
        .list_file_events(first_identifier, None, event_page_limit(2))
        .expect("first file events are listed");
    let first_cursor = first_page
        .next_after()
        .expect("first file page exposes a cursor");

    let second_page = filesystem
        .list_file_events(first_identifier, Some(first_cursor), event_page_limit(2))
        .expect("second file page is listed");
    let second_cursor = second_page
        .next_after()
        .expect("second file page exposes a cursor");

    let third_page = filesystem
        .list_file_events(first_identifier, Some(second_cursor), event_page_limit(2))
        .expect("third file page is listed");

    let combined = first_page
        .records()
        .iter()
        .chain(second_page.records())
        .chain(third_page.records())
        .cloned()
        .collect::<Vec<_>>();

    assert_eq!(first_page.records().len(), 2);
    assert_eq!(second_page.records().len(), 2);
    assert_eq!(third_page.records().len(), 1);
    assert_eq!(third_page.next_after(), None);
    assert_eq!(first_cursor, first_page.records()[1].sequence());
    assert_eq!(second_cursor, second_page.records()[1].sequence());
    assert!(
        second_page
            .records()
            .iter()
            .all(|event| event.sequence() > first_cursor)
    );
    assert!(
        third_page
            .records()
            .iter()
            .all(|event| event.sequence() > second_cursor)
    );
    assert_eq!(
        combined.iter().map(EventRecord::kind).collect::<Vec<_>>(),
        vec![
            EventKind::FileCreated,
            EventKind::FileWritten,
            EventKind::FileWritten,
            EventKind::FileWritten,
            EventKind::FileTruncated,
        ]
    );
    assert!(combined.iter().all(|event| {
        event.file_identifier() == Some(first_identifier) && event.path() == Some("/first")
    }));
    for pair in combined.windows(2) {
        assert!(
            pair[0].sequence() < pair[1].sequence(),
            "file event sequences increase"
        );
        assert!(
            pair[0]
                .branch_position()
                .expect("file event has a branch position")
                .ordinal()
                < pair[1]
                    .branch_position()
                    .expect("file event has a branch position")
                    .ordinal(),
            "file event branch positions increase",
        );
    }
    let iterator_events = filesystem
        .file_events(first_identifier, event_page_limit(2))
        .collect::<Result<Vec<_>, _>>()
        .expect("file event iterator succeeds");
    assert_eq!(iterator_events, combined);

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn file_event_listing_accepts_non_file_cursors_without_restarting() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let first = directories.mount_point_path().join("first");
    let marker = directories.mount_point_path().join("marker");

    fs::write(&first, b"one").expect("first file is written");
    fs::write(&marker, b"marker").expect("marker file is written");
    fs::write(&first, b"two").expect("first file is overwritten");

    let first_identifier = file_identifier_for_path(&filesystem, "/first");
    let marker_cursor = find_event_sequence(&filesystem, |event| {
        event.kind() == EventKind::FileCreated && event.path() == Some("/marker")
    });
    let page = filesystem
        .list_file_events(first_identifier, Some(marker_cursor), event_page_limit(100))
        .expect("first file events after marker cursor are listed");

    assert!(!page.records().is_empty());
    assert!(
        page.records().iter().all(|event| {
            event.file_identifier() == Some(first_identifier) && event.sequence() > marker_cursor
        }),
        "non-file cursor does not restart active-branch file listing",
    );
    assert!(
        page.records()
            .iter()
            .any(|event| event.kind() == EventKind::FileWritten && event.path() == Some("/first"))
    );

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn file_history_and_snapshot_lookups_follow_renamed_and_unlinked_files() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let original_path = directories.mount_point_path().join("draft");
    let renamed_path = directories.mount_point_path().join("published");

    fs::write(&original_path, b"hello").expect("file is written through mounted filesystem");
    fs::rename(&original_path, &renamed_path).expect("file is renamed through mounted filesystem");
    fs::remove_file(&renamed_path).expect("renamed file is removed through mounted filesystem");
    mounted.unmount().expect("filesystem unmounts");

    let file_identifier = file_identifier_for_path(&filesystem, "/draft");
    let file_events = filesystem
        .list_file_events(file_identifier, None, event_page_limit(100))
        .expect("file history is listed");
    let records = file_events.records();

    assert_eq!(file_events.next_after(), None);
    assert_eq!(records.len(), 4);
    assert_eq!(
        records.iter().map(EventRecord::kind).collect::<Vec<_>>(),
        vec![
            EventKind::FileCreated,
            EventKind::FileWritten,
            EventKind::NodeRenamed,
            EventKind::NodeUnlinked,
        ]
    );
    assert_eq!(
        records.iter().map(EventRecord::path).collect::<Vec<_>>(),
        vec![
            Some("/draft"),
            Some("/draft"),
            Some("/published"),
            Some("/published"),
        ]
    );
    assert!(
        records
            .iter()
            .all(|event| event.file_identifier() == Some(file_identifier))
    );
    for pair in records.windows(2) {
        assert!(
            pair[0].sequence() < pair[1].sequence(),
            "file event sequences increase",
        );
        assert!(
            pair[0]
                .branch_position()
                .expect("file event has a branch position")
                .ordinal()
                < pair[1]
                    .branch_position()
                    .expect("file event has a branch position")
                    .ordinal(),
            "file event branch positions increase",
        );
    }

    let write = records
        .iter()
        .find(|event| event.kind() == EventKind::FileWritten)
        .expect("write event exists");
    let renamed = records
        .iter()
        .find(|event| event.kind() == EventKind::NodeRenamed)
        .expect("rename event exists");
    let unlinked = records
        .iter()
        .find(|event| event.kind() == EventKind::NodeUnlinked)
        .expect("unlink event exists");

    for sequence in [
        renamed.sequence(),
        unlinked.sequence(),
        EventSequence::new(u64::MAX),
    ] {
        let snapshot = filesystem
            .file_snapshot_at_or_before(file_identifier, sequence)
            .expect("snapshot lookup succeeds")
            .expect("snapshot exists");
        assert_eq!(snapshot.file_identifier(), file_identifier);
        assert_eq!(snapshot.sequence(), write.sequence());
        assert_eq!(snapshot.file_size(), 5);
        assert_eq!(
            filesystem
                .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
                .expect("snapshot bytes are read"),
            b"hello"
        );
    }
}

#[test]
fn file_history_and_snapshot_lookups_follow_metadata_changes_and_hard_links() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("document");
    let hard_link_path = directories.mount_point_path().join("document-link");

    fs::write(&file_path, b"hello").expect("file is written through mounted filesystem");
    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o600))
        .expect("file metadata is changed through mounted filesystem");
    fs::hard_link(&file_path, &hard_link_path)
        .expect("hard link is created through mounted filesystem");
    mounted.unmount().expect("filesystem unmounts");

    let file_identifier = file_identifier_for_path(&filesystem, "/document");
    let file_events = filesystem
        .list_file_events(file_identifier, None, event_page_limit(100))
        .expect("file history is listed");
    let records = file_events.records();

    assert_eq!(file_events.next_after(), None);
    assert_eq!(records.len(), 4);
    assert_eq!(
        records.iter().map(EventRecord::kind).collect::<Vec<_>>(),
        vec![
            EventKind::FileCreated,
            EventKind::FileWritten,
            EventKind::MetadataChanged,
            EventKind::HardLinkCreated,
        ]
    );
    assert_eq!(
        records.iter().map(EventRecord::path).collect::<Vec<_>>(),
        vec![
            Some("/document"),
            Some("/document"),
            Some("/document"),
            Some("/document-link"),
        ]
    );
    assert!(
        records
            .iter()
            .all(|event| event.file_identifier() == Some(file_identifier))
    );
    for pair in records.windows(2) {
        assert!(
            pair[0].sequence() < pair[1].sequence(),
            "file event sequences increase",
        );
        assert!(
            pair[0]
                .branch_position()
                .expect("file event has a branch position")
                .ordinal()
                < pair[1]
                    .branch_position()
                    .expect("file event has a branch position")
                    .ordinal(),
            "file event branch positions increase",
        );
    }

    let write = records
        .iter()
        .find(|event| event.kind() == EventKind::FileWritten)
        .expect("write event exists");
    let metadata_changed = records
        .iter()
        .find(|event| event.kind() == EventKind::MetadataChanged)
        .expect("metadata event exists");
    let hard_link_created = records
        .iter()
        .find(|event| event.kind() == EventKind::HardLinkCreated)
        .expect("hard-link event exists");

    for sequence in [
        metadata_changed.sequence(),
        hard_link_created.sequence(),
        EventSequence::new(u64::MAX),
    ] {
        let snapshot = filesystem
            .file_snapshot_at_or_before(file_identifier, sequence)
            .expect("snapshot lookup succeeds")
            .expect("snapshot exists");
        assert_eq!(snapshot.file_identifier(), file_identifier);
        assert_eq!(snapshot.sequence(), write.sequence());
        assert_eq!(snapshot.file_size(), 5);
        assert_eq!(
            filesystem
                .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
                .expect("snapshot bytes are read"),
            b"hello"
        );
    }
}

#[test]
fn active_branch_snapshots_and_file_events_ignore_other_branch_sequences() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main one").expect("file is written on main");
    mounted.unmount().expect("filesystem unmounts");

    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let feature_name = BranchName::new("snapshot-cursors").expect("branch name is valid");
    let feature = filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");

    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature branch");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"feature").expect("file is changed on feature");
    mounted.unmount().expect("filesystem unmounts");

    let feature_write = list_all_events(&filesystem)
        .into_iter()
        .find(|event| {
            event.kind() == EventKind::FileWritten
                && event.branch_identifier() == Some(feature.branch_identifier())
                && event.file_identifier() == Some(file_identifier)
        })
        .expect("feature write event exists");

    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main later").expect("file is changed on main");
    mounted.unmount().expect("filesystem unmounts");

    let main_later_write = list_all_events(&filesystem)
        .into_iter()
        .find(|event| {
            event.kind() == EventKind::FileWritten
                && event.branch_identifier() == Some(main.branch_identifier())
                && event.file_identifier() == Some(file_identifier)
                && event.new_file_size() == Some(10)
        })
        .expect("main later write event exists");
    let main_future_snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, EventSequence::new(u64::MAX))
        .expect("future main snapshot lookup succeeds")
        .expect("future main snapshot exists");
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&main_future_snapshot, 0, main_future_snapshot.file_size())
            .expect("future main snapshot is read"),
        b"main later"
    );

    filesystem
        .switch_branch(&feature_name)
        .expect("switches back to feature branch");
    let feature_snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, main_later_write.sequence())
        .expect("cross-branch snapshot lookup succeeds")
        .expect("feature snapshot exists");
    assert_eq!(feature_snapshot.sequence(), feature_write.sequence());
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&feature_snapshot, 0, feature_snapshot.file_size())
            .expect("feature snapshot is read"),
        b"feature"
    );

    let after_other_branch = filesystem
        .list_file_events(
            file_identifier,
            Some(main_later_write.sequence()),
            event_page_limit(100),
        )
        .expect("cross-branch cursor is accepted");
    assert!(after_other_branch.records().is_empty());

    let after_future = filesystem
        .list_file_events(
            file_identifier,
            Some(EventSequence::new(u64::MAX)),
            event_page_limit(100),
        )
        .expect("future cursor is accepted");
    assert!(after_future.records().is_empty());
}

#[test]
fn file_snapshots_and_events_reconstruct_before_and_after_file_changes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("message");

    fs::write(&file_path, b"hello").expect("file is written through mounted filesystem");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("file opens");
    file.seek(SeekFrom::Start(1)).expect("file seek succeeds");
    file.write_all(b"XY").expect("file overwrite succeeds");
    file.set_len(1).expect("file truncates");
    drop(file);

    let events = list_all_events(&filesystem);
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let initial_write = write_event_with_sizes(&events, 0, 5);
    let overwrite = write_event_with_sizes(&events, 5, 5);
    let truncate = events
        .iter()
        .find(|event| event.kind() == EventKind::FileTruncated)
        .expect("truncate event exists");

    let initial_snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, initial_write.sequence())
        .expect("initial snapshot lookup succeeds")
        .expect("initial snapshot exists");
    assert_eq!(initial_snapshot.file_identifier(), file_identifier);
    assert_eq!(initial_snapshot.sequence(), initial_write.sequence());
    assert_eq!(initial_snapshot.file_size(), 5);
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&initial_snapshot, 0, initial_snapshot.file_size())
            .expect("initial snapshot bytes are read"),
        b"hello"
    );

    let before_overwrite = reconstruct_file_at(
        &filesystem,
        file_identifier,
        EventSequence::new(overwrite.sequence().get() - 1),
    );
    let after_overwrite = reconstruct_file_at(&filesystem, file_identifier, overwrite.sequence());
    let before_truncate = reconstruct_file_at(
        &filesystem,
        file_identifier,
        EventSequence::new(truncate.sequence().get() - 1),
    );
    let after_truncate = reconstruct_file_at(&filesystem, file_identifier, truncate.sequence());

    assert_eq!(before_overwrite, b"hello");
    assert_eq!(after_overwrite, b"hXYlo");
    assert_eq!(before_truncate, b"hXYlo");
    assert_eq!(after_truncate, b"h");

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn public_snapshot_and_payload_ranges_cross_content_chunks_and_sparse_zeroes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("large");
    let initial_range_offset = 32 * 1024;
    let initial_range_length = 150 * 1024;
    let initial_write_chunk_size = 24 * 1024;
    let overwrite_offset = 96 * 1024;
    let sparse_offset = 420 * 1024;
    let tail_range_offset = 280 * 1024;
    let tail_range_length = 220 * 1024;
    let original = patterned_bytes(300 * 1024, 11);
    let replacement = patterned_bytes(24 * 1024, 91);
    let sparse = patterned_bytes(32 * 1024, 53);

    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&file_path)
        .expect("large file opens");
    for chunk in original.chunks(initial_write_chunk_size) {
        file.write_all(chunk)
            .expect("large file chunk is written through mounted filesystem");
    }
    file.seek(SeekFrom::Start(overwrite_offset as u64))
        .expect("large file seek succeeds");
    file.write_all(&replacement)
        .expect("large file overwrite succeeds");
    file.seek(SeekFrom::Start(sparse_offset as u64))
        .expect("large file sparse seek succeeds");
    file.write_all(&sparse)
        .expect("large file sparse write succeeds");
    drop(file);
    mounted.unmount().expect("filesystem unmounts");

    let file_identifier = file_identifier_for_path(&filesystem, "/large");

    let mut expected_after_overwrite = original.clone();
    expected_after_overwrite[overwrite_offset..overwrite_offset + replacement.len()]
        .copy_from_slice(&replacement);
    let mut expected_after_sparse = expected_after_overwrite.clone();
    expected_after_sparse.resize(sparse_offset, 0);
    expected_after_sparse.extend_from_slice(&sparse);

    let snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, EventSequence::new(u64::MAX))
        .expect("large snapshot lookup succeeds")
        .expect("large snapshot exists");
    let cross_chunk_range = filesystem
        .read_file_snapshot_range(
            &snapshot,
            initial_range_offset as u64,
            initial_range_length as u64,
        )
        .expect("large snapshot cross-chunk range is read");
    let overwritten_range = filesystem
        .read_file_snapshot_range(&snapshot, overwrite_offset as u64, replacement.len() as u64)
        .expect("large overwritten snapshot range is read");
    let tail_range = filesystem
        .read_file_snapshot_range(
            &snapshot,
            tail_range_offset as u64,
            tail_range_length as u64,
        )
        .expect("large snapshot tail range is read");
    let sparse_boundary = filesystem
        .read_file_snapshot_range(&snapshot, (sparse_offset - 8) as u64, 24)
        .expect("sparse boundary range is read");

    assert_eq!(snapshot.file_size(), expected_after_sparse.len() as u64);
    assert_eq!(
        cross_chunk_range,
        expected_after_sparse[initial_range_offset..initial_range_offset + initial_range_length]
    );
    assert_eq!(overwritten_range, replacement);
    assert_eq!(
        tail_range,
        expected_after_sparse[tail_range_offset..].to_vec()
    );
    assert_eq!(&sparse_boundary[..8], &[0; 8]);
    assert_eq!(&sparse_boundary[8..], &sparse[..16]);
}

#[test]
fn snapshot_and_payload_ranges_clamp_to_available_bytes_and_return_empty_beyond_end() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("ranges");

    fs::write(&file_path, b"abcdef").expect("file is written through mounted filesystem");
    fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("file opens")
        .set_len(2)
        .expect("file truncates");
    mounted.unmount().expect("filesystem unmounts");

    let events = list_all_events(&filesystem);
    let file_identifier = file_identifier_for_path(&filesystem, "/ranges");
    let write = write_event_with_sizes(&events, 0, 6);
    let truncate = events
        .iter()
        .find(|event| event.kind() == EventKind::FileTruncated)
        .expect("truncate event exists");
    let snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, EventSequence::new(u64::MAX))
        .expect("latest snapshot lookup succeeds")
        .expect("latest snapshot exists");

    assert_eq!(
        filesystem
            .read_file_snapshot_range(&snapshot, 1, 10)
            .expect("snapshot range clamps to file size"),
        b"b"
    );
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&snapshot, snapshot.file_size(), 10)
            .expect("snapshot range at end is empty"),
        b""
    );
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&snapshot, snapshot.file_size() + 5, 10)
            .expect("snapshot range past end is empty"),
        b""
    );

    assert_eq!(
        filesystem
            .read_file_event_payload_range(write.sequence(), FileEventPayloadPart::Written, 2, 10)
            .expect("written payload range clamps to payload length"),
        b"cdef"
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(write.sequence(), FileEventPayloadPart::Written, 6, 10,)
            .expect("written payload range at end is empty"),
        b""
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                truncate.sequence(),
                FileEventPayloadPart::Removed,
                1,
                10,
            )
            .expect("removed payload range clamps to payload length"),
        b"def"
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                truncate.sequence(),
                FileEventPayloadPart::Removed,
                4,
                10,
            )
            .expect("removed payload range at end is empty"),
        b""
    );
}

fn find_event_sequence(
    filesystem: &Filesystem,
    mut predicate: impl FnMut(&EventRecord) -> bool,
) -> EventSequence {
    let mut after = None;

    loop {
        let page = filesystem
            .list_events(after, event_page_limit(2))
            .expect("events are listed");
        if let Some(event) = page.records().iter().find(|event| predicate(event)) {
            return event.sequence();
        }
        after = page.next_after();
        assert!(after.is_some(), "matching event exists");
    }
}

fn write_event_with_sizes(
    events: &[EventRecord],
    old_file_size: u64,
    new_file_size: u64,
) -> &EventRecord {
    events
        .iter()
        .find(|event| {
            event.kind() == EventKind::FileWritten
                && event.old_file_size() == Some(old_file_size)
                && event.new_file_size() == Some(new_file_size)
        })
        .expect("write event exists")
}

fn reconstruct_file_at(
    filesystem: &Filesystem,
    file_identifier: FileIdentifier,
    target: EventSequence,
) -> Vec<u8> {
    let snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, target)
        .expect("snapshot lookup succeeds")
        .expect("snapshot exists");
    let mut bytes = filesystem
        .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
        .expect("snapshot bytes are read");
    let mut after = Some(snapshot.sequence());

    loop {
        let page = filesystem
            .list_file_events(file_identifier, after, event_page_limit(100))
            .expect("file events are listed");
        for event in page.records() {
            if event.sequence() > target {
                return bytes;
            }
            apply_file_event(filesystem, &mut bytes, event);
        }
        match page.next_after() {
            Some(next_after) if next_after <= target => after = Some(next_after),
            Some(_) | None => return bytes,
        }
    }
}

fn apply_file_event(filesystem: &Filesystem, bytes: &mut Vec<u8>, event: &EventRecord) {
    match event.kind() {
        EventKind::FileCreated => bytes.clear(),
        EventKind::FileWritten => {
            let offset = usize::try_from(event.offset().expect("write offset exists"))
                .expect("write offset fits usize");
            let written = filesystem
                .read_file_event_payload_range(
                    event.sequence(),
                    FileEventPayloadPart::Written,
                    0,
                    event.written_byte_length().expect("written length exists"),
                )
                .expect("written bytes are read");
            let end = offset + written.len();
            if bytes.len() < offset {
                bytes.resize(offset, 0);
            }
            if bytes.len() < end {
                bytes.resize(end, 0);
            }
            bytes[offset..end].copy_from_slice(&written);
            bytes.resize(
                usize::try_from(event.new_file_size().expect("new size exists"))
                    .expect("new size fits usize"),
                0,
            );
        }
        EventKind::FileTruncated => bytes.resize(
            usize::try_from(event.new_file_size().expect("new size exists"))
                .expect("new size fits usize"),
            0,
        ),
        _ => {}
    }
}

fn patterned_bytes(length: usize, salt: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index * 31 + salt) % 251) as u8)
        .collect()
}
