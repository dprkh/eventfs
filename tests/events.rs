mod support;

use std::fs;
use std::os::unix::fs::symlink;

use eventfs::{BranchName, EventKind, EventSequence};
use time::UtcDateTime;

use support::{TestDirectories, event_page_limit, mkfifo, open_test_filesystem};

#[test]
fn event_listing_returns_initial_event_with_branch_metadata_and_utc_creation_time() {
    let directories = TestDirectories::new();
    let before_open = UtcDateTime::now();
    let filesystem = open_test_filesystem(&directories);
    let after_open = UtcDateTime::now();

    let current_branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let events = filesystem
        .list_events(None, event_page_limit(10))
        .expect("events are listed");

    assert_eq!(current_branch.name().as_str(), "main");
    assert_eq!(current_branch.head_sequence(), EventSequence::new(0));
    assert_eq!(events.records().len(), 1);

    let event = &events.records()[0];
    assert_eq!(event.sequence(), EventSequence::new(0));
    assert_eq!(event.kind(), EventKind::FilesystemInitialized);
    assert!(event.created_at() >= before_open);
    assert!(event.created_at() <= after_open);
    assert_eq!(
        event.branch_identifier(),
        Some(current_branch.branch_identifier())
    );
    assert_eq!(
        event.branch_position(),
        Some(current_branch.head_position())
    );
    assert_eq!(
        event.first_parent_sequence(),
        Some(current_branch.head_sequence())
    );
    assert_eq!(event.file_identifier(), None);
    assert_eq!(event.path(), None);
    assert_eq!(event.offset(), None);
    assert_eq!(event.byte_length(), None);
    assert_eq!(events.next_after(), None);
}

#[test]
fn event_listing_paginates_across_multiple_pages_without_overlap() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");

    fs::write(directories.mount_point_path().join("first"), b"first")
        .expect("first file is written through mounted filesystem");
    fs::write(directories.mount_point_path().join("second"), b"second")
        .expect("second file is written through mounted filesystem");
    fs::write(directories.mount_point_path().join("third"), b"third")
        .expect("third file is written through mounted filesystem");

    let first_page = filesystem
        .list_events(None, event_page_limit(2))
        .expect("first event page is listed");
    let first_cursor = first_page
        .next_after()
        .expect("first page exposes a cursor when more events exist");

    let second_page = filesystem
        .list_events(Some(first_cursor), event_page_limit(2))
        .expect("second event page is listed");
    let second_cursor = second_page
        .next_after()
        .expect("second page exposes a cursor when more events exist");

    let third_page = filesystem
        .list_events(Some(second_cursor), event_page_limit(2))
        .expect("third event page is listed");
    let third_cursor = third_page
        .next_after()
        .expect("third page exposes a cursor when more events exist");

    let fourth_page = filesystem
        .list_events(Some(third_cursor), event_page_limit(2))
        .expect("fourth event page is listed");

    assert_eq!(first_page.records().len(), 2);
    assert_eq!(second_page.records().len(), 2);
    assert_eq!(third_page.records().len(), 2);
    assert_eq!(fourth_page.records().len(), 1);
    assert_eq!(fourth_page.next_after(), None);
    assert_eq!(first_cursor, first_page.records()[1].sequence());
    assert_eq!(second_cursor, second_page.records()[1].sequence());
    assert_eq!(third_cursor, third_page.records()[1].sequence());
    assert!(
        second_page
            .records()
            .iter()
            .all(|record| record.sequence() > first_cursor)
    );
    assert!(
        third_page
            .records()
            .iter()
            .all(|record| record.sequence() > second_cursor)
    );
    assert!(
        fourth_page
            .records()
            .iter()
            .all(|record| record.sequence() > third_cursor)
    );

    let sequences = first_page
        .records()
        .iter()
        .chain(second_page.records())
        .chain(third_page.records())
        .chain(fourth_page.records())
        .map(|record| record.sequence().get())
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![0, 1, 2, 3, 4, 5, 6]);

    let last_sequence = fourth_page.records()[0].sequence();
    let after_last_page = filesystem
        .list_events(Some(last_sequence), event_page_limit(2))
        .expect("listing after the last event succeeds");
    let overflow_page = filesystem
        .list_events(Some(EventSequence::new(u64::MAX)), event_page_limit(2))
        .expect("listing after an overflow cursor succeeds");

    assert!(after_last_page.records().is_empty());
    assert_eq!(after_last_page.next_after(), None);
    assert!(overflow_page.records().is_empty());
    assert_eq!(overflow_page.next_after(), None);

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn global_event_listing_and_get_event_preserve_branch_metadata_across_divergence() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main base").expect("file is written on main");
    mounted.unmount().expect("filesystem unmounts");

    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let feature_name = BranchName::new("global-events-feature").expect("branch name is valid");
    let feature = filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");

    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature branch");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"feature one").expect("first feature write succeeds");
    fs::write(&file_path, b"feature two").expect("second feature write succeeds");
    mounted.unmount().expect("filesystem unmounts");

    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main later").expect("later main write succeeds");
    mounted.unmount().expect("filesystem unmounts");

    let events = filesystem
        .list_events(None, event_page_limit(100))
        .expect("events are listed");
    assert!(events.records().iter().all(|event| {
        event.branch_identifier().is_some()
            && event.first_parent_sequence().is_some()
            && event.branch_position().is_some_and(|position| {
                Some(position.branch_identifier()) == event.branch_identifier()
            })
    }));

    let feature_events = events
        .records()
        .iter()
        .filter(|event| {
            event.path() == Some("/message")
                && event.branch_identifier() == Some(feature.branch_identifier())
                && event.sequence() > feature.head_sequence()
        })
        .collect::<Vec<_>>();
    assert!(feature_events.len() >= 2);
    assert_eq!(
        feature_events[0].first_parent_sequence(),
        Some(feature.head_sequence())
    );
    assert_eq!(
        feature_events[1].first_parent_sequence(),
        Some(feature_events[0].sequence())
    );
    assert!(
        feature_events[0]
            .branch_position()
            .is_some_and(|position| position > feature.head_position())
    );
    assert!(feature_events.windows(2).all(|pair| {
        pair[0]
            .branch_position()
            .zip(pair[1].branch_position())
            .is_some_and(|(first, second)| first < second)
    }));
    let feature_writes = feature_events
        .iter()
        .copied()
        .filter(|event| event.kind() == EventKind::FileWritten)
        .collect::<Vec<_>>();
    assert_eq!(feature_writes.len(), 2);

    let later_main_events = events
        .records()
        .iter()
        .filter(|event| {
            event.path() == Some("/message")
                && event.branch_identifier() == Some(main.branch_identifier())
                && event.sequence() > main.head_sequence()
        })
        .collect::<Vec<_>>();
    assert!(!later_main_events.is_empty());
    assert_eq!(
        later_main_events[0].first_parent_sequence(),
        Some(main.head_sequence())
    );
    assert_ne!(
        later_main_events[0].first_parent_sequence(),
        Some(feature_events[1].sequence())
    );
    assert!(
        later_main_events
            .iter()
            .any(|event| event.kind() == EventKind::FileWritten)
    );

    let fetched = filesystem
        .get_event(feature_writes[0].sequence())
        .expect("feature event lookup succeeds");
    assert_eq!(fetched.as_ref(), Some(feature_writes[0]));
}

#[test]
fn get_event_matches_listed_records_and_debug_redacts_payload_bytes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let file_path = directories.mount_point_path().join("payload-log");
    let written_secret = b"written-secret-12345removed-secret-67890";

    fs::write(&file_path, written_secret).expect("payload file is written");
    fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("payload file opens")
        .set_len(20)
        .expect("payload file truncates");

    let events = filesystem
        .list_events(None, event_page_limit(100))
        .expect("events are listed");
    assert_eq!(
        filesystem
            .get_event(EventSequence::new(u64::MAX))
            .expect("missing event lookup succeeds"),
        None
    );

    for record in events.records() {
        let fetched = filesystem
            .get_event(record.sequence())
            .expect("existing event lookup succeeds");
        assert_eq!(fetched.as_ref(), Some(record));
    }

    let debug = format!("{events:?}");
    assert!(debug.contains("EventKind::FileWritten") || debug.contains("FileWritten"));
    assert!(!debug.contains("written-secret-12345"));
    assert!(!debug.contains("removed-secret-67890"));

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn global_event_listing_and_get_event_cover_non_file_event_kinds() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let directory_path = directories.mount_point_path().join("directory");
    let fifo_path = directories.mount_point_path().join("fifo");
    let target_path = directories.mount_point_path().join("target");
    let symlink_path = directories.mount_point_path().join("link");

    fs::create_dir(&directory_path).expect("directory is created through mounted filesystem");
    fs::remove_dir(&directory_path).expect("directory is removed through mounted filesystem");
    mkfifo(&fifo_path);
    fs::write(&target_path, b"target").expect("symlink target file is written");
    symlink(&target_path, &symlink_path)
        .expect("symbolic link is created through mounted filesystem");

    let events = filesystem
        .list_events(None, event_page_limit(100))
        .expect("events are listed");

    for (kind, path) in [
        (EventKind::DirectoryCreated, "/directory"),
        (EventKind::DirectoryRemoved, "/directory"),
        (EventKind::NodeCreated, "/fifo"),
        (EventKind::SymbolicLinkCreated, "/link"),
    ] {
        let listed = events
            .records()
            .iter()
            .find(|event| event.kind() == kind && event.path() == Some(path))
            .expect("listed event exists");
        let fetched = filesystem
            .get_event(listed.sequence())
            .expect("listed event lookup succeeds");

        assert_eq!(fetched.as_ref(), Some(listed));
        assert_eq!(listed.file_identifier(), None);
        assert_eq!(listed.offset(), None);
        assert_eq!(listed.byte_length(), None);
        assert_eq!(listed.old_file_size(), None);
        assert_eq!(listed.new_file_size(), None);
        assert_eq!(listed.overwritten_byte_length(), None);
        assert_eq!(listed.written_byte_length(), None);
        assert_eq!(listed.removed_byte_length(), None);
        assert!(listed.branch_identifier().is_some());
        assert!(listed.first_parent_sequence().is_some());
        assert!(listed.branch_position().is_some_and(|position| {
            Some(position.branch_identifier()) == listed.branch_identifier()
        }));
    }

    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn event_listing_is_page_size_invariant_and_get_event_round_trips_each_record() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main base").expect("file is written on main");
    mounted.unmount().expect("filesystem unmounts");

    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let feature_name = BranchName::new("page-size-invariant").expect("branch name is valid");
    filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");

    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature branch");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"feature one").expect("first feature write succeeds");
    fs::write(&file_path, b"feature two").expect("second feature write succeeds");
    mounted.unmount().expect("filesystem unmounts");

    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main later").expect("later main write succeeds");
    mounted.unmount().expect("filesystem unmounts");

    let expected_page = filesystem
        .list_events(None, event_page_limit(100))
        .expect("full event log is listed");
    assert_eq!(expected_page.next_after(), None);
    let expected_records = expected_page.into_records();
    assert!(expected_records.len() >= 6);

    let max_limit = u64::try_from(expected_records.len()).expect("event count fits u64") + 1;
    for limit in 1..=max_limit {
        let mut paged_records = Vec::new();
        let mut after = None;

        loop {
            let page = filesystem
                .list_events(after, event_page_limit(limit))
                .expect("event page is listed");
            assert!(page.records().len() <= limit as usize);

            if let Some(cursor) = page.next_after() {
                assert_eq!(page.records().len(), limit as usize);
                assert_eq!(
                    page.records().last().map(|record| record.sequence()),
                    Some(cursor)
                );
            }

            for record in page.records() {
                let fetched = filesystem
                    .get_event(record.sequence())
                    .expect("listed event can be fetched by sequence");
                assert_eq!(fetched.as_ref(), Some(record));
            }

            paged_records.extend_from_slice(page.records());
            match page.next_after() {
                Some(next_after) => after = Some(next_after),
                None => break,
            }
        }

        assert_eq!(paged_records, expected_records, "limit {limit}");
        let iterator_records = filesystem
            .events(event_page_limit(limit))
            .collect::<Result<Vec<_>, _>>()
            .expect("event iterator succeeds");
        assert_eq!(iterator_records, expected_records, "iterator limit {limit}");
    }
}
