mod support;

use std::path::PathBuf;

use eventfs::{
    BackupDirectory, BackupIdentifier, BranchName, BranchPageLimit, ConfigurationError, EventKind,
    EventPageLimit, EventSequence, Filesystem, FilesystemConfiguration, FilesystemError,
};
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options};

use support::{TestDirectories, list_all_events, open_test_filesystem};

#[test]
fn path_valued_public_constructors_reject_empty_paths() {
    assert_eq!(
        FilesystemConfiguration::builder()
            .database_directory(PathBuf::new())
            .mount_point(PathBuf::from("mount"))
            .build(),
        Err(ConfigurationError::EmptyValue)
    );
    assert_eq!(
        FilesystemConfiguration::builder()
            .database_directory(PathBuf::from("database"))
            .mount_point(PathBuf::new())
            .build(),
        Err(ConfigurationError::EmptyValue)
    );
    assert_eq!(
        BackupDirectory::new(PathBuf::new()),
        Err(ConfigurationError::EmptyValue)
    );
}

#[test]
fn nonzero_public_constructors_reject_zero() {
    assert_eq!(BackupIdentifier::new(0), Err(ConfigurationError::ZeroValue));
    assert_eq!(
        EventPageLimit::try_from(0),
        Err(ConfigurationError::ZeroValue)
    );
    assert_eq!(
        BranchPageLimit::try_from(0),
        Err(ConfigurationError::ZeroValue)
    );
    assert_eq!(BranchName::new(""), Err(ConfigurationError::EmptyValue));
}

#[test]
fn valid_filesystem_configuration_opens_a_new_database_and_exposes_the_initial_event() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let events = list_all_events(&filesystem);

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence(), EventSequence::new(0));
    assert_eq!(events[0].kind(), EventKind::FilesystemInitialized);
}

#[test]
fn opening_an_existing_database_missing_required_metadata_values_returns_integrity() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);

    drop(filesystem);
    clear_configuration_metadata(directories.database_directory_path());
    let error = Filesystem::open(directories.configuration())
        .expect_err("existing database missing required metadata is rejected");

    assert_eq!(error, FilesystemError::Integrity);
}

#[test]
fn opening_a_filesystem_does_not_require_the_mount_point_to_exist_yet() {
    let directories = TestDirectories::new();
    let missing_mount_point = directories.root_path().join("future-mount-point");
    assert!(
        !missing_mount_point.exists(),
        "the mount point starts absent for this open-only configuration path"
    );

    let filesystem = Filesystem::open(
        FilesystemConfiguration::builder()
            .database_directory(directories.database_directory_path().to_path_buf())
            .mount_point(missing_mount_point)
            .build()
            .expect("configuration is valid"),
    )
    .expect("filesystem opens before the mount point exists");
    let events = list_all_events(&filesystem);

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence(), EventSequence::new(0));
    assert_eq!(events[0].kind(), EventKind::FilesystemInitialized);
}

#[test]
fn reopening_an_existing_database_preserves_the_initial_event_history() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let events_before_reopen = list_all_events(&filesystem);

    drop(filesystem);

    let reopened = Filesystem::open(directories.configuration()).expect("filesystem reopens");

    assert_eq!(list_all_events(&reopened), events_before_reopen);
}

const CONFIGURATION_COLUMN_FAMILY_FILESYSTEM_METADATA: &str = "filesystem_metadata";

fn clear_configuration_metadata(database_directory: &std::path::Path) {
    let options = Options::default();
    let descriptors = DB::list_cf(&options, database_directory)
        .expect("existing column families are listed")
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect::<Vec<_>>();
    let database = DB::open_cf_descriptors(&options, database_directory, descriptors)
        .expect("database opens for metadata removal");
    let metadata = database
        .cf_handle(CONFIGURATION_COLUMN_FAMILY_FILESYSTEM_METADATA)
        .expect("metadata column family exists");
    let keys = database
        .iterator_cf(metadata, IteratorMode::Start)
        .map(|entry| entry.expect("metadata entry is readable").0.to_vec())
        .collect::<Vec<_>>();

    for key in keys {
        database
            .delete_cf(metadata, key)
            .expect("metadata entry is removed");
    }

    database
        .flush_cf(metadata)
        .expect("metadata removals are flushed");
}
