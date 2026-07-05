mod support;

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::thread;

use eventfs::{BranchName, BranchStatus, EventKind, FilesystemError};

use support::{
    TestDirectories, branch_page_limit, event_page_limit, file_identifier_for_path,
    list_all_events, open_test_filesystem,
};

#[test]
fn branch_switching_divergence_and_snapshot_reads_preserve_independent_file_contents() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"hello").expect("file is written on main");
    mounted.unmount().expect("filesystem unmounts");

    let main = filesystem
        .current_branch()
        .expect("current branch is returned");
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let feature_name = BranchName::new("feature").expect("branch name is valid");
    let feature = filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");
    assert_eq!(feature.status(), BranchStatus::Open);
    assert_eq!(feature.head_sequence(), main.head_sequence());

    let fork_snapshot = filesystem
        .file_snapshot_on_branch_at_or_before(
            feature.branch_identifier(),
            file_identifier,
            feature.head_position(),
        )
        .expect("fork snapshot lookup succeeds")
        .expect("fork snapshot exists");
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&fork_snapshot, 0, fork_snapshot.file_size())
            .expect("fork snapshot bytes are read"),
        b"hello"
    );

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert_eq!(
        fs::read(&file_path).expect("mounted file is readable before switch attempt"),
        b"hello"
    );
    assert_eq!(
        filesystem.switch_branch(&feature_name),
        Err(FilesystemError::FilesystemOperation)
    );
    mounted.unmount().expect("filesystem unmounts");

    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"hi mom").expect("feature branch file is changed");
    assert_eq!(
        fs::read(&file_path).expect("feature branch file is read"),
        b"hi mom"
    );
    mounted.unmount().expect("filesystem unmounts");

    let feature_head = filesystem
        .current_branch()
        .expect("feature branch head is returned");
    assert_eq!(
        feature_head.branch_identifier(),
        feature.branch_identifier()
    );
    assert!(feature_head.head_position().ordinal() > feature.head_position().ordinal());
    let feature_events = filesystem
        .list_branch_file_events(
            feature.branch_identifier(),
            file_identifier,
            Some(feature.head_position()),
            event_page_limit(100),
        )
        .expect("feature branch file events are listed");
    assert!(feature_events.records().iter().any(|event| {
        event.kind() == EventKind::FileWritten
            && event.branch_identifier() == Some(feature.branch_identifier())
            && event.path() == Some("/message")
    }));

    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert_eq!(
        fs::read(&file_path).expect("main branch file is read"),
        b"hello"
    );
    mounted.unmount().expect("filesystem unmounts");

    filesystem
        .switch_branch(&feature_name)
        .expect("switches back to feature");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert_eq!(
        fs::read(&file_path).expect("feature branch file is read again"),
        b"hi mom"
    );
    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn branch_creation_after_directory_rename_preserves_parent_link_counts() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let root = directories.mount_point_path();
    let old_parent = root.join("old-parent");
    let new_parent = root.join("new-parent");
    let moved = old_parent.join("moved");
    let moved_target = new_parent.join("moved");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::create_dir(&old_parent).expect("old parent is created");
    fs::create_dir(&new_parent).expect("new parent is created");
    fs::create_dir(&moved).expect("moved directory is created");
    fs::write(moved.join("child"), b"branch contents").expect("moved child is written");
    fs::rename(&moved, &moved_target).expect("directory is renamed across parents");

    let moved_inode = fs::metadata(&moved_target)
        .expect("renamed directory metadata is readable")
        .ino();
    let old_parent_links = fs::metadata(&old_parent)
        .expect("old parent metadata is readable")
        .nlink();
    let new_parent_links = fs::metadata(&new_parent)
        .expect("new parent metadata is readable")
        .nlink();
    mounted.unmount().expect("filesystem unmounts");

    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let branch_name = BranchName::new("directory-rename-history").expect("branch name is valid");
    filesystem
        .create_branch(&branch_name, main.head_position())
        .expect("branch is created from renamed directory history");
    filesystem
        .switch_branch(&branch_name)
        .expect("switches to branch");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert!(
        fs::metadata(&moved).is_err(),
        "old directory path is absent on branch"
    );
    let moved_metadata =
        fs::metadata(&moved_target).expect("renamed directory metadata is readable on branch");
    assert!(moved_metadata.is_dir());
    assert_eq!(moved_metadata.ino(), moved_inode);
    assert_eq!(
        fs::read(moved_target.join("child")).expect("renamed child is readable on branch"),
        b"branch contents"
    );
    assert_eq!(
        fs::metadata(&old_parent)
            .expect("old parent metadata is readable on branch")
            .nlink(),
        old_parent_links
    );
    assert_eq!(
        fs::metadata(&new_parent)
            .expect("new parent metadata is readable on branch")
            .nlink(),
        new_parent_links
    );
    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn branch_creation_from_prior_file_event_uses_snapshot_contents_at_that_position() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"hello").expect("file is written on main");
    fs::write(&file_path, b"main later").expect("file is changed on main");
    mounted.unmount().expect("filesystem unmounts");

    let events = list_all_events(&filesystem);
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let first_write = events
        .iter()
        .find(|event| {
            event.kind() == EventKind::FileWritten
                && event.file_identifier() == Some(file_identifier)
                && event.old_file_size() == Some(0)
        })
        .expect("first write exists");
    let historical_name = BranchName::new("from-first-write").expect("branch name is valid");
    let historical = filesystem
        .create_branch(
            &historical_name,
            first_write
                .branch_position()
                .expect("first write has a branch position"),
        )
        .expect("historical branch is created");

    filesystem
        .switch_branch(&historical_name)
        .expect("switches to historical branch");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert_eq!(
        fs::read(&file_path).expect("historical branch file is read"),
        b"hello"
    );
    fs::write(&file_path, b"hi mom").expect("historical branch diverges");
    mounted.unmount().expect("filesystem unmounts");

    let branch_snapshot = filesystem
        .file_snapshot_on_branch_at_or_before(
            historical.branch_identifier(),
            file_identifier,
            historical.head_position(),
        )
        .expect("historical branch snapshot lookup succeeds")
        .expect("historical branch snapshot exists");
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&branch_snapshot, 0, branch_snapshot.file_size())
            .expect("historical branch snapshot bytes are read"),
        b"hello"
    );

    let main = filesystem
        .list_branches(None, branch_page_limit(10))
        .expect("branches are listed")
        .records()
        .iter()
        .find(|branch| branch.name().as_str() == "main")
        .expect("main branch exists")
        .clone();
    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert_eq!(
        fs::read(&file_path).expect("main branch file is read"),
        b"main later"
    );
    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn branch_creation_from_prior_position_restores_historical_namespace() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let docs = directories.mount_point_path().join("docs");
    let old_path = docs.join("old.txt");
    let new_path = docs.join("new.txt");
    let later_path = docs.join("later.txt");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::create_dir(&docs).expect("directory is created on main");
    fs::write(&old_path, b"old").expect("historical file is written");
    mounted.unmount().expect("filesystem unmounts");

    let historical_position = filesystem
        .current_branch()
        .expect("main branch head is returned")
        .head_position();

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::rename(&old_path, &new_path).expect("historical file is renamed on main");
    fs::write(&later_path, b"later").expect("later file is written on main");
    fs::remove_file(&new_path).expect("renamed file is removed on main");
    mounted.unmount().expect("filesystem unmounts");

    let historical_name = BranchName::new("historical-namespace").expect("branch name is valid");
    filesystem
        .create_branch(&historical_name, historical_position)
        .expect("historical namespace branch is created");
    filesystem
        .switch_branch(&historical_name)
        .expect("switches to historical namespace branch");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert_eq!(
        fs::read(&old_path).expect("historical path exists on branch"),
        b"old"
    );
    assert!(
        !new_path.exists(),
        "renamed main path is absent from historical branch"
    );
    assert!(
        !later_path.exists(),
        "later main path is absent from historical branch"
    );
    mounted.unmount().expect("filesystem unmounts");
}

#[test]
fn branch_file_history_and_snapshots_handle_files_absent_on_branch_or_at_position() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("late.txt");
    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let before_file_position = main.head_position();

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"late").expect("late file is written on main");
    mounted.unmount().expect("filesystem unmounts");

    let file_identifier = file_identifier_for_path(&filesystem, "/late.txt");
    let main_file_events = filesystem
        .list_branch_file_events(
            main.branch_identifier(),
            file_identifier,
            Some(before_file_position),
            event_page_limit(10),
        )
        .expect("main branch file events after the earlier position are listed");
    assert!(!main_file_events.records().is_empty());
    assert_eq!(main_file_events.next_after(), None);
    assert!(main_file_events.records().iter().all(|event| {
        event.file_identifier() == Some(file_identifier)
            && event.branch_identifier() == Some(main.branch_identifier())
            && event
                .branch_position()
                .is_some_and(|position| position.ordinal() > before_file_position.ordinal())
    }));
    assert!(main_file_events.records().iter().any(|event| {
        event.kind() == EventKind::FileCreated && event.path() == Some("/late.txt")
    }));

    assert_eq!(
        filesystem
            .file_snapshot_on_branch_at_or_before(
                main.branch_identifier(),
                file_identifier,
                before_file_position,
            )
            .expect("main branch pre-creation snapshot lookup succeeds"),
        None
    );

    let historical_name = BranchName::new("before-late-file").expect("branch name is valid");
    let historical = filesystem
        .create_branch(&historical_name, before_file_position)
        .expect("historical branch is created");
    filesystem
        .switch_branch(&historical_name)
        .expect("switches to historical branch");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    assert!(
        !file_path.exists(),
        "late main file is absent from the historical branch"
    );
    mounted.unmount().expect("filesystem unmounts");

    let historical_events = filesystem
        .list_branch_file_events(
            historical.branch_identifier(),
            file_identifier,
            None,
            event_page_limit(10),
        )
        .expect("historical branch file history is listed");
    assert!(historical_events.records().is_empty());
    assert_eq!(historical_events.next_after(), None);

    assert_eq!(
        filesystem
            .file_snapshot_on_branch_at_or_before(
                historical.branch_identifier(),
                file_identifier,
                historical.head_position(),
            )
            .expect("historical branch missing snapshot lookup succeeds"),
        None
    );
}

#[test]
fn newly_created_branch_has_no_branch_events_before_divergence() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let feature = filesystem
        .create_branch(
            &BranchName::new("empty-branch-events").expect("branch name is valid"),
            main.head_position(),
        )
        .expect("feature branch is created");

    let page = filesystem
        .list_branch_events(feature.branch_identifier(), None, event_page_limit(10))
        .expect("new branch events are listed");
    assert!(page.records().is_empty());
    assert_eq!(page.next_after(), None);
}

#[test]
fn branch_event_listing_uses_branch_positions_and_rejects_cross_branch_cursors() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"hello").expect("file is written on main");
    mounted.unmount().expect("filesystem unmounts");

    let main = filesystem
        .current_branch()
        .expect("current branch is returned");
    let feature_name = BranchName::new("event-listing").expect("branch name is valid");
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

    let page = filesystem
        .list_branch_events(
            feature.branch_identifier(),
            Some(feature.head_position()),
            event_page_limit(1),
        )
        .expect("feature branch events are listed");
    assert_eq!(page.records().len(), 1);
    let cursor = page
        .next_after()
        .expect("feature branch event listing exposes a cursor");
    let remaining = filesystem
        .list_branch_events(
            feature.branch_identifier(),
            Some(cursor),
            event_page_limit(10),
        )
        .expect("remaining feature branch events are listed");
    let records = page
        .records()
        .iter()
        .chain(remaining.records())
        .collect::<Vec<_>>();
    assert!(records.iter().all(|event| {
        event.branch_identifier() == Some(feature.branch_identifier())
            && event
                .branch_position()
                .is_some_and(|position| position.ordinal() > feature.head_position().ordinal())
    }));
    assert!(records.windows(2).all(|pair| {
        pair[0]
            .branch_position()
            .expect("branch event has a branch position")
            < pair[1]
                .branch_position()
                .expect("branch event has a branch position")
    }));
    let iterator_records = filesystem
        .branch_events(feature.branch_identifier(), event_page_limit(1))
        .collect::<Result<Vec<_>, _>>()
        .expect("feature branch event iterator succeeds");
    let paginated_records = page
        .clone()
        .into_records()
        .into_iter()
        .chain(remaining.clone().into_records())
        .collect::<Vec<_>>();
    assert_eq!(iterator_records, paginated_records);

    assert_eq!(
        filesystem.list_branch_events(
            feature.branch_identifier(),
            Some(main.head_position()),
            event_page_limit(10),
        ),
        Err(FilesystemError::Integrity)
    );
}

#[test]
fn global_events_remain_branch_aware_and_branch_file_listing_paginates_per_branch() {
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
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let feature_name = BranchName::new("branch-file-pages").expect("branch name is valid");
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

    let first_page = filesystem
        .list_events(None, event_page_limit(2))
        .expect("first global event page is listed");
    let cursor = first_page
        .next_after()
        .expect("global event pagination exposes a cursor");
    let remaining = filesystem
        .list_events(Some(cursor), event_page_limit(100))
        .expect("remaining global events are listed");
    let global_events = first_page
        .records()
        .iter()
        .chain(remaining.records())
        .collect::<Vec<_>>();
    assert!(global_events.iter().all(|event| {
        event.branch_identifier().is_some()
            && event.branch_position().is_some()
            && event.first_parent_sequence().is_some()
            && event
                .branch_position()
                .map(|position| position.branch_identifier())
                == event.branch_identifier()
    }));

    let branch_file_first_page = filesystem
        .list_branch_file_events(
            feature.branch_identifier(),
            file_identifier,
            Some(feature.head_position()),
            event_page_limit(1),
        )
        .expect("first feature branch file page is listed");
    assert_eq!(branch_file_first_page.records().len(), 1);
    let branch_file_cursor = branch_file_first_page
        .next_after()
        .expect("feature branch file pagination exposes a cursor");
    let branch_file_remaining = filesystem
        .list_branch_file_events(
            feature.branch_identifier(),
            file_identifier,
            Some(branch_file_cursor),
            event_page_limit(100),
        )
        .expect("remaining feature branch file events are listed");
    let branch_file_events = branch_file_first_page
        .records()
        .iter()
        .chain(branch_file_remaining.records())
        .collect::<Vec<_>>();
    assert!(branch_file_events.len() >= 2);
    assert!(branch_file_events.iter().all(|event| {
        event.file_identifier() == Some(file_identifier)
            && event.branch_identifier() == Some(feature.branch_identifier())
            && event
                .branch_position()
                .is_some_and(|position| position.ordinal() > feature.head_position().ordinal())
    }));
    assert!(branch_file_events.windows(2).all(|pair| {
        pair[0]
            .branch_position()
            .expect("branch file event has a branch position")
            < pair[1]
                .branch_position()
                .expect("branch file event has a branch position")
    }));
    let iterator_branch_file_events = filesystem
        .branch_file_events(
            feature.branch_identifier(),
            file_identifier,
            event_page_limit(1),
        )
        .collect::<Result<Vec<_>, _>>()
        .expect("feature branch file event iterator succeeds");
    let paginated_branch_file_events = branch_file_first_page
        .clone()
        .into_records()
        .into_iter()
        .chain(branch_file_remaining.clone().into_records())
        .collect::<Vec<_>>();
    assert_eq!(iterator_branch_file_events, paginated_branch_file_events);

    assert_eq!(
        filesystem.list_branch_file_events(
            feature.branch_identifier(),
            file_identifier,
            Some(main.head_position()),
            event_page_limit(10),
        ),
        Err(FilesystemError::Integrity)
    );
}

#[test]
fn branch_listing_orders_branch_identifiers_and_exhausts_cursors() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    for name in ["alpha", "beta", "gamma"] {
        filesystem
            .create_branch(
                &BranchName::new(name).expect("branch name is valid"),
                main.head_position(),
            )
            .expect("branch is created");
    }

    let first_page = filesystem
        .list_branches(None, branch_page_limit(2))
        .expect("first branch page is listed");
    assert_eq!(first_page.records().len(), 2);
    assert!(
        first_page.records()[0].branch_identifier() < first_page.records()[1].branch_identifier()
    );
    let cursor = first_page
        .next_after()
        .expect("first branch page exposes a cursor");
    assert_eq!(
        cursor,
        first_page.records()[1].branch_identifier(),
        "branch listing cursor follows the last returned identifier"
    );

    let second_page = filesystem
        .list_branches(Some(cursor), branch_page_limit(2))
        .expect("second branch page is listed");
    assert_eq!(second_page.records().len(), 2);
    assert!(
        cursor < second_page.records()[0].branch_identifier(),
        "later page starts after the previous cursor"
    );
    assert!(
        second_page.records()[0].branch_identifier() < second_page.records()[1].branch_identifier()
    );
    assert_eq!(
        second_page.next_after(),
        None,
        "branch listing stops exposing cursors after the last page"
    );
    let iterator_records = filesystem
        .branches(branch_page_limit(2))
        .collect::<Result<Vec<_>, _>>()
        .expect("branch iterator succeeds");
    let paginated_records = first_page
        .clone()
        .into_records()
        .into_iter()
        .chain(second_page.clone().into_records())
        .collect::<Vec<_>>();
    assert_eq!(iterator_records, paginated_records);
}

#[test]
fn branch_event_first_parent_sequences_follow_each_branch_head_chain() {
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
    let feature_name = BranchName::new("first-parent").expect("branch name is valid");
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

    let feature_events = filesystem
        .list_branch_events(
            feature.branch_identifier(),
            Some(feature.head_position()),
            event_page_limit(10),
        )
        .expect("feature branch events are listed");
    assert!(feature_events.records().len() >= 2);
    let first_feature = &feature_events.records()[0];
    let second_feature = &feature_events.records()[1];
    assert_eq!(
        first_feature.first_parent_sequence(),
        Some(feature.head_sequence())
    );
    assert_eq!(
        second_feature.first_parent_sequence(),
        Some(first_feature.sequence())
    );

    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main later").expect("later main write succeeds");
    mounted.unmount().expect("filesystem unmounts");

    let main_later = list_all_events(&filesystem)
        .into_iter()
        .find(|event| {
            event.branch_identifier() == Some(main.branch_identifier())
                && event.path() == Some("/message")
                && event.sequence() > main.head_sequence()
        })
        .expect("later main branch event exists");
    assert_eq!(
        main_later.first_parent_sequence(),
        Some(main.head_sequence())
    );
    assert_ne!(
        main_later.first_parent_sequence(),
        Some(second_feature.sequence())
    );
}

#[test]
fn branch_snapshot_reads_and_deletion_reject_cross_branch_or_active_state() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"hello").expect("file is written on main");
    mounted.unmount().expect("filesystem unmounts");

    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let feature_name = BranchName::new("active-delete").expect("branch name is valid");
    let feature = filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");

    assert_eq!(
        filesystem.file_snapshot_on_branch_at_or_before(
            feature.branch_identifier(),
            file_identifier,
            main.head_position(),
        ),
        Err(FilesystemError::Integrity)
    );

    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature branch");
    assert_eq!(
        filesystem.delete_branch(&feature_name),
        Err(FilesystemError::Integrity)
    );
}

#[test]
fn branch_listing_and_deletion_keep_history_but_remove_inactive_refs() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let main = filesystem
        .current_branch()
        .expect("current branch is returned");
    let scratch_name = BranchName::new("scratch").expect("branch name is valid");
    let scratch = filesystem
        .create_branch(&scratch_name, main.head_position())
        .expect("scratch branch is created");

    let first_page = filesystem
        .list_branches(None, branch_page_limit(1))
        .expect("branches are listed");
    assert_eq!(first_page.records().len(), 1);
    let cursor = first_page
        .next_after()
        .expect("branch pagination cursor exists");
    let second_page = filesystem
        .list_branches(Some(cursor), branch_page_limit(10))
        .expect("remaining branches are listed");
    assert!(
        second_page
            .records()
            .iter()
            .any(|branch| branch.branch_identifier() == scratch.branch_identifier())
    );

    assert_eq!(
        filesystem.delete_branch(main.name()),
        Err(FilesystemError::Integrity)
    );
    filesystem
        .delete_branch(&scratch_name)
        .expect("inactive branch is deleted");
    assert_eq!(
        filesystem.create_branch(&scratch_name, main.head_position()),
        Err(FilesystemError::Integrity)
    );
    assert_eq!(
        filesystem.switch_branch(&scratch_name),
        Err(FilesystemError::Integrity)
    );

    let branches = filesystem
        .list_branches(None, branch_page_limit(10))
        .expect("branches are listed after deletion");
    let deleted = branches
        .records()
        .iter()
        .find(|branch| branch.branch_identifier() == scratch.branch_identifier())
        .expect("deleted branch record remains");
    assert_eq!(deleted.status(), BranchStatus::Deleted);
}

#[test]
fn branch_creation_rejects_deleted_branch_positions() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let deleted_name = BranchName::new("deleted-source").expect("branch name is valid");
    let deleted = filesystem
        .create_branch(&deleted_name, main.head_position())
        .expect("deleted source branch is created");

    filesystem
        .delete_branch(&deleted_name)
        .expect("inactive branch is deleted");

    assert_eq!(
        filesystem.create_branch(
            &BranchName::new("from-deleted-source").expect("branch name is valid"),
            deleted.head_position(),
        ),
        Err(FilesystemError::Integrity)
    );
}

#[test]
fn deleted_branch_refs_preserve_history_and_snapshot_reads() {
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
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let feature_name = BranchName::new("deleted-history").expect("branch name is valid");
    let feature = filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");

    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature branch");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"feature text").expect("feature branch file is written");
    mounted.unmount().expect("filesystem unmounts");

    let feature_head = filesystem
        .current_branch()
        .expect("feature branch head is returned");
    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    filesystem
        .delete_branch(&feature_name)
        .expect("feature branch is deleted");

    let branches = filesystem
        .list_branches(None, branch_page_limit(10))
        .expect("branches are listed after deletion");
    let deleted = branches
        .records()
        .iter()
        .find(|branch| branch.branch_identifier() == feature.branch_identifier())
        .expect("deleted branch remains listed");
    assert_eq!(deleted.name().as_str(), feature_name.as_str());
    assert_eq!(deleted.status(), BranchStatus::Deleted);
    assert_eq!(deleted.head_position(), feature_head.head_position());
    assert_eq!(deleted.head_sequence(), feature_head.head_sequence());

    let deleted_history = filesystem
        .list_branch_events(
            feature.branch_identifier(),
            Some(feature.head_position()),
            event_page_limit(10),
        )
        .expect("deleted branch history is listed");
    assert!(deleted_history.records().iter().any(|event| {
        event.branch_identifier() == Some(feature.branch_identifier())
            && event.kind() == EventKind::FileWritten
            && event.path() == Some("/message")
    }));

    let deleted_file_history = filesystem
        .list_branch_file_events(
            feature.branch_identifier(),
            file_identifier,
            Some(feature.head_position()),
            event_page_limit(10),
        )
        .expect("deleted branch file history is listed");
    assert!(deleted_file_history.records().iter().any(|event| {
        event.branch_identifier() == Some(feature.branch_identifier())
            && event.file_identifier() == Some(file_identifier)
            && event.path() == Some("/message")
    }));

    let deleted_snapshot = filesystem
        .file_snapshot_on_branch_at_or_before(
            feature.branch_identifier(),
            file_identifier,
            feature_head.head_position(),
        )
        .expect("deleted branch snapshot lookup succeeds")
        .expect("deleted branch snapshot exists");
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&deleted_snapshot, 0, deleted_snapshot.file_size())
            .expect("deleted branch snapshot bytes are read"),
        b"feature text"
    );
}

#[test]
fn branch_switching_and_deletion_do_not_append_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let feature_name = BranchName::new("switch-no-event").expect("branch name is valid");
    let scratch_name = BranchName::new("delete-no-event").expect("branch name is valid");
    filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");
    filesystem
        .create_branch(&scratch_name, main.head_position())
        .expect("scratch branch is created");

    let before = list_all_events(&filesystem);
    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature branch");
    assert_eq!(list_all_events(&filesystem), before);

    filesystem
        .switch_branch(main.name())
        .expect("switches back to main");
    assert_eq!(list_all_events(&filesystem), before);

    filesystem
        .delete_branch(&scratch_name)
        .expect("inactive branch is deleted");
    assert_eq!(list_all_events(&filesystem), before);
}

#[test]
fn concurrent_branch_deletion_switching_and_listing_preserve_branch_integrity() {
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
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let names = (0..8)
        .map(|index| BranchName::new(format!("load-{index}")).expect("branch name is valid"))
        .collect::<Vec<_>>();
    for name in &names {
        filesystem
            .create_branch(name, main.head_position())
            .expect("load branch is created");
    }

    let delete_filesystem = filesystem.clone();
    let delete_names = names[..4].to_vec();
    let deleter = thread::spawn(move || {
        for name in delete_names {
            delete_filesystem
                .delete_branch(&name)
                .expect("inactive branch is deleted");
        }
    });

    let switch_filesystem = filesystem.clone();
    let switch_names = names[4..].to_vec();
    let switcher = thread::spawn(move || {
        for _ in 0..3 {
            for name in &switch_names {
                switch_filesystem
                    .switch_branch(name)
                    .expect("open branch is switched to");
            }
        }
    });

    let list_filesystem = filesystem.clone();
    let main_branch_identifier = main.branch_identifier();
    let lister = thread::spawn(move || {
        for _ in 0..40 {
            list_filesystem
                .list_branches(None, branch_page_limit(100))
                .expect("branches are listed under load");
            list_filesystem
                .list_events(None, event_page_limit(100))
                .expect("events are listed under load");
            list_filesystem
                .list_branch_events(main_branch_identifier, None, event_page_limit(100))
                .expect("branch events are listed under load");
            list_filesystem
                .list_branch_file_events(
                    main_branch_identifier,
                    file_identifier,
                    None,
                    event_page_limit(100),
                )
                .expect("branch file events are listed under load");
            thread::yield_now();
        }
    });

    deleter.join().expect("deleter thread joins");
    switcher.join().expect("switcher thread joins");
    lister.join().expect("lister thread joins");

    filesystem
        .switch_branch(main.name())
        .expect("main branch remains switchable");
    for name in &names[..4] {
        assert_eq!(
            filesystem.switch_branch(name),
            Err(FilesystemError::Integrity)
        );
    }
    for name in &names[4..] {
        filesystem
            .switch_branch(name)
            .expect("remaining branch remains switchable");
    }
}

#[test]
fn mounted_branch_switch_conflicts_with_concurrent_writes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let feature_name = BranchName::new("mounted-conflict").expect("branch name is valid");
    filesystem
        .create_branch(&feature_name, main.head_position())
        .expect("feature branch is created");
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");

    let root = directories.mount_point_path().to_path_buf();
    let writer = thread::spawn(move || {
        for index in 0..12 {
            fs::write(
                root.join(format!("mounted-{index}")),
                format!("value-{index}"),
            )
            .expect("mounted conflict file is written");
        }
    });

    for _ in 0..12 {
        assert_eq!(
            filesystem.switch_branch(&feature_name),
            Err(FilesystemError::FilesystemOperation)
        );
        assert_eq!(
            filesystem.delete_branch(main.name()),
            Err(FilesystemError::Integrity)
        );
        thread::yield_now();
    }

    writer.join().expect("mounted writer joins");
    mounted.unmount().expect("filesystem unmounts");
    filesystem
        .switch_branch(&feature_name)
        .expect("branch switch succeeds after unmount");
}
