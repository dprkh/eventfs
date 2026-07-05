mod support;

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use eventfs::{
    BackupDirectory, BackupIdentifier, BranchName, EventKind, FileEventPayloadPart, Filesystem,
    FilesystemConfiguration, FilesystemError,
};

use support::{
    TestDirectories, configuration_for, file_identifier_for_path, list_all_events,
    open_test_filesystem,
};

#[test]
fn local_backups_create_increasing_nonzero_identifiers_in_a_persistent_directory() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();

    let first = filesystem
        .create_backup(backup_directory.clone())
        .expect("first backup succeeds");
    let second = filesystem
        .create_backup(backup_directory)
        .expect("second backup succeeds");

    assert!(first.backup_identifier().get() > 0);
    assert!(second.backup_identifier().get() > first.backup_identifier().get());
    assert_eq!(first.source_event_sequence().get(), 0);
    assert_eq!(second.source_event_sequence().get(), 0);
}

#[test]
fn local_import_verifies_requested_backup_replaces_target_and_imported_database_opens() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");

    let imported_database_path = directories.root_path().join("imported-database");
    fs::create_dir(&imported_database_path).expect("target directory is created");
    fs::write(imported_database_path.join("obsolete"), b"old").expect("obsolete file is written");

    let import = Filesystem::import_backup(
        imported_database_path.clone(),
        backup_directory,
        backup.backup_identifier(),
    )
    .expect("import succeeds");

    assert_eq!(import.backup_identifier(), backup.backup_identifier());
    assert_eq!(
        import.imported_event_sequence(),
        backup.source_event_sequence()
    );
    assert!(!imported_database_path.join("obsolete").exists());

    let imported_mount_point = directories.root_path().join("imported-mount");
    fs::create_dir(&imported_mount_point).expect("imported mount point is created");
    Filesystem::open(
        FilesystemConfiguration::builder()
            .database_directory(imported_database_path)
            .mount_point(imported_mount_point)
            .build()
            .expect("imported configuration is valid"),
    )
    .expect("imported filesystem opens");
}

#[test]
fn local_import_replaces_an_existing_target_database() {
    let directories = TestDirectories::new();
    let source = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    let source_file_path = directories.mount_point_path().join("source-message");

    let mounted = source
        .spawn_mount()
        .expect("source filesystem mounts in the background");
    fs::write(&source_file_path, b"source state").expect("source file is written");
    mounted.unmount().expect("source filesystem unmounts");

    let source_events = list_all_events(&source);
    let backup = source
        .create_backup(backup_directory.clone())
        .expect("source backup succeeds");

    let target_database_path = directories.root_path().join("existing-target-database");
    let target_mount_point = directories.root_path().join("existing-target-mount");
    let target = open_filesystem_at(target_database_path.clone(), target_mount_point.clone());

    let mounted = target
        .spawn_mount()
        .expect("target filesystem mounts in the background");
    fs::write(target_mount_point.join("target-only"), b"target state")
        .expect("target file is written");
    mounted.unmount().expect("target filesystem unmounts");

    assert_ne!(list_all_events(&target), source_events);
    drop(target);

    let import = Filesystem::import_backup(
        target_database_path.clone(),
        backup_directory,
        backup.backup_identifier(),
    )
    .expect("import over an existing target database succeeds");
    assert_eq!(import.backup_identifier(), backup.backup_identifier());
    assert_eq!(
        import.imported_event_sequence(),
        backup.source_event_sequence()
    );

    let reopened_mount_point = directories.root_path().join("replaced-target-mount");
    let imported = open_filesystem_at(target_database_path, reopened_mount_point.clone());
    assert_eq!(list_all_events(&imported), source_events);

    let mounted = imported
        .spawn_mount()
        .expect("imported filesystem mounts in the background");
    assert_eq!(
        fs::read(reopened_mount_point.join("source-message"))
            .expect("imported source file is read"),
        b"source state"
    );
    assert!(
        !reopened_mount_point.join("target-only").exists(),
        "existing target database contents are discarded during replacement"
    );
    mounted.unmount().expect("imported filesystem unmounts");
}

#[test]
fn imported_backups_restore_active_branch_contents_history_snapshots_and_payloads() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"main base").expect("main branch file is written");
    mounted.unmount().expect("filesystem unmounts");

    let main = filesystem
        .current_branch()
        .expect("main branch is returned");
    let file_identifier = file_identifier_for_path(&filesystem, "/message");
    let feature_name = BranchName::new("restored-feature").expect("branch name is valid");
    filesystem
        .create_branch(feature_name.clone(), main.head_position())
        .expect("feature branch is created");
    filesystem
        .switch_branch(&feature_name)
        .expect("switches to feature branch");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"feature payload").expect("feature branch file is written");
    mounted.unmount().expect("filesystem unmounts");

    let feature = filesystem
        .current_branch()
        .expect("feature branch head is returned");
    let source_events = list_all_events(&filesystem);
    let feature_write = source_events
        .iter()
        .find(|event| {
            event.kind() == EventKind::FileWritten
                && event.branch_identifier() == Some(feature.branch_identifier())
                && event.file_identifier() == Some(file_identifier)
                && event.path() == Some("/message")
        })
        .expect("feature write event exists");
    let source_snapshot = filesystem
        .file_snapshot_on_branch_at_or_before(
            feature.branch_identifier(),
            file_identifier,
            feature.head_position(),
        )
        .expect("feature snapshot lookup succeeds")
        .expect("feature snapshot exists");
    assert_eq!(
        filesystem
            .read_file_snapshot_range(&source_snapshot, 0, source_snapshot.file_size())
            .expect("feature snapshot bytes are read"),
        b"feature payload"
    );
    assert_eq!(
        filesystem
            .read_file_event_payload_range(
                feature_write.sequence(),
                FileEventPayloadPart::Written,
                0,
                100,
            )
            .expect("feature payload bytes are read"),
        b"feature payload"
    );

    let backup_directory = directories.backup_directory();
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");
    let imported_database_path = directories.root_path().join("restored-database");
    let imported_mount_point = directories.root_path().join("restored-mount");
    let import = Filesystem::import_backup(
        imported_database_path.clone(),
        backup_directory,
        backup.backup_identifier(),
    )
    .expect("import succeeds");
    assert_eq!(import.backup_identifier(), backup.backup_identifier());
    assert_eq!(
        import.imported_event_sequence(),
        backup.source_event_sequence()
    );

    let imported = open_filesystem_at(imported_database_path, imported_mount_point.clone());
    let imported_feature = imported
        .current_branch()
        .expect("imported current branch is returned");
    assert_eq!(imported_feature.name().as_str(), feature_name.as_str());
    assert_eq!(imported_feature.head_position(), feature.head_position());
    assert_eq!(imported_feature.head_sequence(), feature.head_sequence());
    assert_eq!(list_all_events(&imported), source_events);

    let imported_file_identifier = file_identifier_for_path(&imported, "/message");
    assert_eq!(imported_file_identifier, file_identifier);
    let mounted = imported
        .spawn_mount()
        .expect("imported filesystem mounts in the background");
    assert_eq!(
        fs::read(imported_mount_point.join("message")).expect("imported feature file is read"),
        b"feature payload"
    );
    mounted.unmount().expect("imported filesystem unmounts");

    let imported_snapshot = imported
        .file_snapshot_on_branch_at_or_before(
            imported_feature.branch_identifier(),
            imported_file_identifier,
            imported_feature.head_position(),
        )
        .expect("imported feature snapshot lookup succeeds")
        .expect("imported feature snapshot exists");
    assert_eq!(imported_snapshot, source_snapshot);
    assert_eq!(
        imported
            .read_file_snapshot_range(&imported_snapshot, 0, imported_snapshot.file_size())
            .expect("imported snapshot bytes are read"),
        b"feature payload"
    );
    assert_eq!(
        imported
            .read_file_event_payload_range(
                feature_write.sequence(),
                FileEventPayloadPart::Written,
                0,
                100,
            )
            .expect("imported payload bytes are read"),
        b"feature payload"
    );
}

#[test]
fn import_can_restore_an_older_requested_backup() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");
    let backup_directory = directories.backup_directory();

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"older state").expect("initial file contents are written");
    mounted.unmount().expect("filesystem unmounts");
    let older_backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("older backup succeeds");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"newer state").expect("later file contents are written");
    mounted.unmount().expect("filesystem unmounts");
    let newer_backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("newer backup succeeds");
    assert!(newer_backup.backup_identifier() > older_backup.backup_identifier());

    let imported_database_path = directories.root_path().join("older-imported-database");
    let imported_mount_point = directories.root_path().join("older-imported-mount");
    let import = Filesystem::import_backup(
        imported_database_path.clone(),
        backup_directory,
        older_backup.backup_identifier(),
    )
    .expect("older backup import succeeds");
    assert_eq!(import.backup_identifier(), older_backup.backup_identifier());
    assert_eq!(
        import.imported_event_sequence(),
        older_backup.source_event_sequence()
    );

    let imported = open_filesystem_at(imported_database_path, imported_mount_point.clone());
    assert_eq!(
        list_all_events(&imported)
            .last()
            .expect("imported older backup contains events")
            .sequence(),
        older_backup.source_event_sequence()
    );
    let mounted = imported
        .spawn_mount()
        .expect("imported older filesystem mounts in the background");
    assert_eq!(
        fs::read(imported_mount_point.join("message")).expect("older imported file is read"),
        b"older state"
    );
    mounted
        .unmount()
        .expect("imported older filesystem unmounts");
}

#[test]
fn mounted_backups_capture_the_state_before_later_live_writes() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let file_path = directories.mount_point_path().join("message");
    let backup_directory = directories.backup_directory();

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    fs::write(&file_path, b"before backup").expect("initial file contents are written");
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("mounted backup succeeds");
    fs::write(&file_path, b"after backup").expect("later file contents are written");
    mounted.unmount().expect("filesystem unmounts");

    assert!(
        list_all_events(&filesystem)
            .last()
            .expect("source filesystem contains events")
            .sequence()
            > backup.source_event_sequence()
    );

    let imported_database_path = directories.root_path().join("mounted-import-database");
    let imported_mount_point = directories.root_path().join("mounted-import-mount");
    let import = Filesystem::import_backup(
        imported_database_path.clone(),
        backup_directory,
        backup.backup_identifier(),
    )
    .expect("mounted backup import succeeds");

    assert_eq!(import.backup_identifier(), backup.backup_identifier());
    assert_eq!(
        import.imported_event_sequence(),
        backup.source_event_sequence()
    );

    let imported = open_filesystem_at(imported_database_path, imported_mount_point.clone());
    assert_eq!(
        list_all_events(&imported)
            .last()
            .expect("imported filesystem contains backup events")
            .sequence(),
        backup.source_event_sequence()
    );

    let mounted = imported
        .spawn_mount()
        .expect("imported filesystem mounts in the background");
    assert_eq!(
        fs::read(imported_mount_point.join("message")).expect("imported file is read"),
        b"before backup"
    );
    mounted
        .unmount()
        .expect("imported mounted filesystem unmounts");
}

#[test]
fn backup_creation_failures_map_to_backup_errors() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_path = directories.root_path().join("backup-path-file");
    fs::write(&backup_path, b"file").expect("backup path file is written");

    let error = filesystem
        .create_backup(BackupDirectory::new(backup_path).expect("backup path is valid"))
        .expect_err("backup path file is rejected");

    assert_eq!(error, FilesystemError::Backup);
}

#[test]
fn import_target_replacement_failures_map_to_import_errors() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");

    let parent_file = directories.root_path().join("not-a-directory-parent");
    fs::write(&parent_file, b"file").expect("parent path file is written");
    let blocked_target = parent_file.join("database");

    let error =
        Filesystem::import_backup(blocked_target, backup_directory, backup.backup_identifier())
            .expect_err("blocked import target is rejected");

    assert_eq!(error, FilesystemError::Import);
}

#[test]
fn import_rejects_empty_target_paths() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");

    let error =
        Filesystem::import_backup(PathBuf::new(), backup_directory, backup.backup_identifier())
            .expect_err("empty import target path is rejected");

    assert_eq!(error, FilesystemError::Import);
}

#[test]
fn local_backup_rejects_source_directory_overlap_after_path_normalization() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);

    let same_as_source = eventfs::BackupDirectory::new(
        directories
            .database_directory_path()
            .join("..")
            .join("database"),
    )
    .expect("backup directory path is valid");
    let same_error = filesystem
        .create_backup(same_as_source)
        .expect_err("backup at normalized source path is rejected");
    assert_eq!(same_error, FilesystemError::Backup);

    let nested_path = directories.database_directory_path().join("nested-backup");
    let nested =
        eventfs::BackupDirectory::new(nested_path.clone()).expect("backup directory path is valid");
    let nested_error = filesystem
        .create_backup(nested)
        .expect_err("backup inside source path is rejected");
    assert_eq!(nested_error, FilesystemError::Backup);
    assert!(
        !nested_path.exists(),
        "rejected backup directory is not created inside the source database"
    );
}

#[test]
fn backup_and_import_reject_overlap_when_one_path_contains_the_other() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);

    let containing_backup = BackupDirectory::new(directories.root_path().to_path_buf())
        .expect("backup directory path is valid");
    let backup_error = filesystem
        .create_backup(containing_backup)
        .expect_err("backup directory containing the source database is rejected");
    assert_eq!(backup_error, FilesystemError::Backup);

    let backup_directory = directories.backup_directory();
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");
    let import_error = Filesystem::import_backup(
        directories.root_path().to_path_buf(),
        backup_directory,
        backup.backup_identifier(),
    )
    .expect_err("import target containing the backup repository is rejected");
    assert_eq!(import_error, FilesystemError::Import);
}

#[test]
fn local_backup_overlap_uses_opened_database_path_after_cwd_changes() {
    let _cwd_lock = CWD_LOCK.lock().expect("cwd lock is available");
    let _cwd_guard = CurrentDirectoryGuard::new();
    let root = tempfile::tempdir().expect("temporary directory is created");
    let mount_point = root.path().join("mount");
    let other_directory = root.path().join("other");
    fs::create_dir(&mount_point).expect("mount point is created");
    fs::create_dir(&other_directory).expect("other directory is created");
    env::set_current_dir(root.path()).expect("cwd changes to test root");
    let filesystem = Filesystem::open(configuration_for(
        PathBuf::from("database"),
        PathBuf::from("mount"),
    ))
    .expect("filesystem opens from relative database path");
    env::set_current_dir(&other_directory).expect("cwd changes after filesystem opens");

    let nested_path = root.path().join("database").join("nested-backup");
    let error = filesystem
        .create_backup(
            eventfs::BackupDirectory::new(nested_path.clone())
                .expect("backup directory path is valid"),
        )
        .expect_err("backup inside opened source path is rejected after cwd change");

    assert_eq!(error, FilesystemError::Backup);
    assert!(
        !nested_path.exists(),
        "rejected backup directory is not created inside the source database"
    );
}

#[test]
fn local_import_rejects_target_overlap_after_path_normalization() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");

    let normalized_inside_backup = backup_directory
        .as_path()
        .join("..")
        .join("backups")
        .join("normalized-target");
    let error = Filesystem::import_backup(
        normalized_inside_backup,
        backup_directory,
        backup.backup_identifier(),
    )
    .expect_err("normalized target inside backup repository is rejected");

    assert_eq!(error, FilesystemError::Import);
}

#[test]
fn import_unknown_backup_identifiers_map_to_integrity() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");

    let imported_database_path = directories.root_path().join("unknown-backup-import");
    let error = Filesystem::import_backup(
        imported_database_path,
        backup_directory,
        BackupIdentifier::new(u32::MAX).expect("backup identifier is valid"),
    )
    .expect_err("unknown backup identifier is rejected");

    assert_eq!(error, FilesystemError::Integrity);
}

#[test]
fn import_non_directory_backup_paths_map_to_integrity() {
    let directories = TestDirectories::new();
    let backup_path = directories.root_path().join("backup-path-file");
    fs::write(&backup_path, b"file").expect("backup path file is written");

    let error = Filesystem::import_backup(
        directories.root_path().join("non-directory-backup-import"),
        BackupDirectory::new(backup_path).expect("backup path is valid"),
        BackupIdentifier::new(1).expect("backup identifier is valid"),
    )
    .expect_err("non-directory backup path is rejected");

    assert_eq!(error, FilesystemError::Integrity);
}

#[test]
fn unknown_backup_import_failures_preserve_existing_target_data() {
    let directories = TestDirectories::new();
    let source = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    source
        .create_backup(backup_directory.clone())
        .expect("source backup succeeds");

    let target_database_path = directories.root_path().join("target-database");
    let target_mount_point = directories.root_path().join("target-mount");
    let target = open_filesystem_at(target_database_path.clone(), target_mount_point.clone());
    let target_branch = BranchName::new("target-only").expect("branch name is valid");
    let main = target
        .current_branch()
        .expect("target main branch is returned");
    target
        .create_branch(target_branch.clone(), main.head_position())
        .expect("target branch is created");
    target
        .switch_branch(&target_branch)
        .expect("switches to target branch");
    let mounted = target
        .spawn_mount()
        .expect("target filesystem mounts in the background");
    fs::write(target_mount_point.join("keep"), b"keep").expect("target file is written");
    mounted.unmount().expect("target filesystem unmounts");
    let target_events = list_all_events(&target);
    drop(target);

    let error = Filesystem::import_backup(
        target_database_path.clone(),
        backup_directory,
        BackupIdentifier::new(u32::MAX).expect("backup identifier is valid"),
    )
    .expect_err("unknown backup identifier import is rejected");
    assert_eq!(error, FilesystemError::Integrity);

    let reopened_mount_point = directories.root_path().join("target-remount");
    let reopened = open_filesystem_at(target_database_path, reopened_mount_point.clone());
    assert_eq!(
        reopened
            .current_branch()
            .expect("reopened target branch is returned")
            .name()
            .as_str(),
        target_branch.as_str()
    );
    assert_eq!(list_all_events(&reopened), target_events);
    let mounted = reopened
        .spawn_mount()
        .expect("reopened target filesystem mounts in the background");
    assert_eq!(
        fs::read(reopened_mount_point.join("keep")).expect("preserved target file is read"),
        b"keep"
    );
    mounted
        .unmount()
        .expect("reopened target filesystem unmounts");
}

#[test]
fn import_rejects_targets_inside_the_backup_repository() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let backup_directory = directories.backup_directory();
    let backup = filesystem
        .create_backup(backup_directory.clone())
        .expect("backup succeeds");

    let error = Filesystem::import_backup(
        backup_directory.as_path().join("nested-database"),
        backup_directory,
        backup.backup_identifier(),
    )
    .expect_err("import target inside backup repository is rejected");

    assert_eq!(error, FilesystemError::Import);
}

#[test]
fn missing_backup_repositories_map_to_integrity() {
    let directories = TestDirectories::new();
    let backup_directory = eventfs::BackupDirectory::new(directories.root_path().join("missing"))
        .expect("backup directory path is valid");

    let error = Filesystem::import_backup(
        directories.root_path().join("missing-backup-import"),
        backup_directory,
        BackupIdentifier::new(1).expect("backup identifier is valid"),
    )
    .expect_err("missing backup repository is rejected");

    assert_eq!(error, FilesystemError::Integrity);
}

static CWD_LOCK: Mutex<()> = Mutex::new(());

struct CurrentDirectoryGuard {
    path: PathBuf,
}

impl CurrentDirectoryGuard {
    fn new() -> Self {
        Self {
            path: env::current_dir().expect("current directory is readable"),
        }
    }
}

impl Drop for CurrentDirectoryGuard {
    fn drop(&mut self) {
        env::set_current_dir(&self.path).expect("current directory is restored");
    }
}

fn open_filesystem_at(database_directory: PathBuf, mount_point: PathBuf) -> Filesystem {
    fs::create_dir_all(&mount_point).expect("mount point is created");
    Filesystem::open(configuration_for(database_directory, mount_point)).expect("filesystem opens")
}
