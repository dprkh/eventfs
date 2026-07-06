mod support;

use std::path::PathBuf;

use eventfs::{
    BackupDirectory, BackupIdentifier, BranchName, BranchPageLimit, ConfigurationError, EventKind,
    EventPageLimit, EventSequence, Filesystem, FilesystemConfiguration, FilesystemError,
    MountOption, SessionAccessControlList,
};
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options};

use support::{TestDirectories, list_all_events, open_test_filesystem};

#[test]
fn path_valued_public_constructors_reject_empty_paths() {
    assert_eq!(
        FilesystemConfiguration::new(PathBuf::new(), PathBuf::from("mount")),
        Err(ConfigurationError::EmptyValue)
    );
    assert_eq!(
        FilesystemConfiguration::new(PathBuf::from("database"), PathBuf::new()),
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
    assert_eq!(EventPageLimit::new(0), Err(ConfigurationError::ZeroValue));
    assert_eq!(BranchPageLimit::new(0), Err(ConfigurationError::ZeroValue));
    assert_eq!(BranchName::new(""), Err(ConfigurationError::EmptyValue));
}

#[test]
fn public_configuration_and_error_values_expose_debug_display_and_equality() {
    let directories = TestDirectories::new();
    let configuration = directories.configuration();
    let same_configuration = FilesystemConfiguration::new(
        directories.database_directory_path().to_path_buf(),
        directories.mount_point_path().to_path_buf(),
    )
    .expect("matching configuration is valid");

    assert_eq!(configuration, same_configuration);
    let debug_configuration = format!("{configuration:?}");
    assert!(debug_configuration.contains("session_access_control_list: Owner"));
    assert!(debug_configuration.contains("mount_options: []"));
    assert!(debug_configuration.contains("fuse_error_callback: false"));

    let mount_configuration = configuration
        .clone()
        .with_session_access_control_list(SessionAccessControlList::RootAndOwner)
        .with_mount_options([
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::FilesystemName("eventfs-test".to_owned()),
        ]);

    assert_ne!(mount_configuration, configuration);
    assert!(format!("{mount_configuration:?}").contains("RootAndOwner"));
    assert!(format!("{mount_configuration:?}").contains("DefaultPermissions"));
    assert!(format!("{mount_configuration:?}").contains("FilesystemName"));

    let callback_configuration = configuration.clone().with_fuse_error_callback(|_error| {});
    let cloned_callback_configuration = callback_configuration.clone();
    let other_callback_configuration = same_configuration.with_fuse_error_callback(|_error| {});

    assert_eq!(callback_configuration, cloned_callback_configuration);
    assert_ne!(callback_configuration, configuration);
    assert_ne!(callback_configuration, other_callback_configuration);
    assert!(format!("{callback_configuration:?}").contains("fuse_error_callback: true"));

    let filesystem = Filesystem::open(configuration).expect("filesystem opens");
    assert!(format!("{filesystem:?}").contains("Filesystem"));

    assert_eq!(
        ConfigurationError::EmptyValue.to_string(),
        "configuration value must not be empty"
    );
    assert_eq!(
        ConfigurationError::ZeroValue.to_string(),
        "configuration value must be non-zero"
    );
    assert_eq!(
        FilesystemError::FilesystemOperation.to_string(),
        "filesystem operation failed"
    );
    assert_eq!(
        FilesystemError::Database.to_string(),
        "database operation failed"
    );
    assert_eq!(
        FilesystemError::Integrity.to_string(),
        "integrity check failed"
    );
    assert_eq!(
        FilesystemError::Backup.to_string(),
        "backup operation failed"
    );
    assert_eq!(
        FilesystemError::Import.to_string(),
        "import operation failed"
    );
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
        FilesystemConfiguration::new(
            directories.database_directory_path().to_path_buf(),
            missing_mount_point,
        )
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

#[test]
fn opening_storage_schema_baseline_preserves_existing_database() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let events_before_reopen = list_all_events(&filesystem);

    drop(filesystem);
    write_configuration_schema_version(directories.database_directory_path(), 8);

    let reopened =
        Filesystem::open(directories.configuration()).expect("baseline schema database opens");

    assert_eq!(list_all_events(&reopened), events_before_reopen);
    drop(reopened);
    assert_eq!(
        read_configuration_schema_version(directories.database_directory_path()),
        8
    );
}

#[test]
fn opening_storage_schema_outside_compatibility_window_fails_without_mutation() {
    for schema_version in [7, u64::MAX] {
        let directories = TestDirectories::new();
        let filesystem = open_test_filesystem(&directories);

        drop(filesystem);
        write_configuration_schema_version(directories.database_directory_path(), schema_version);
        let error = Filesystem::open(directories.configuration())
            .expect_err("incompatible storage schema is rejected");

        assert_eq!(error, FilesystemError::Integrity);
        assert_eq!(
            read_configuration_schema_version(directories.database_directory_path()),
            schema_version
        );
    }
}

#[test]
fn opening_database_missing_column_family_required_by_stored_schema_returns_integrity() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);

    drop(filesystem);
    drop_configuration_column_family(
        directories.database_directory_path(),
        CONFIGURATION_COLUMN_FAMILY_EVENT_PAYLOAD_MANIFESTS,
    );
    let error = Filesystem::open(directories.configuration())
        .expect_err("missing required stored-schema column family is rejected");

    assert_eq!(error, FilesystemError::Integrity);
}

const CONFIGURATION_COLUMN_FAMILY_FILESYSTEM_METADATA: &str = "filesystem_metadata";
const CONFIGURATION_COLUMN_FAMILY_EVENT_PAYLOAD_MANIFESTS: &str = "event_payload_manifests";
const CONFIGURATION_METADATA_KEY_STORAGE_SCHEMA_VERSION: &[u8] = b"schema_version";

fn write_configuration_schema_version(database_directory: &std::path::Path, version: u64) {
    let database = open_configuration_database(database_directory);
    let metadata = database
        .cf_handle(CONFIGURATION_COLUMN_FAMILY_FILESYSTEM_METADATA)
        .expect("metadata column family exists");

    database
        .put_cf(
            metadata,
            CONFIGURATION_METADATA_KEY_STORAGE_SCHEMA_VERSION,
            version.to_be_bytes(),
        )
        .expect("schema version is written");
    database
        .flush_cf(metadata)
        .expect("schema version write is flushed");
}

fn read_configuration_schema_version(database_directory: &std::path::Path) -> u64 {
    let database = open_configuration_database(database_directory);
    let metadata = database
        .cf_handle(CONFIGURATION_COLUMN_FAMILY_FILESYSTEM_METADATA)
        .expect("metadata column family exists");
    let value = database
        .get_cf(metadata, CONFIGURATION_METADATA_KEY_STORAGE_SCHEMA_VERSION)
        .expect("schema version is read")
        .expect("schema version exists");
    let bytes: [u8; 8] = value
        .as_slice()
        .try_into()
        .expect("schema version is encoded as u64");

    u64::from_be_bytes(bytes)
}

fn drop_configuration_column_family(database_directory: &std::path::Path, name: &str) {
    let mut database = open_configuration_database(database_directory);

    database
        .drop_cf(name)
        .expect("column family is dropped from test database");
}

fn open_configuration_database(database_directory: &std::path::Path) -> DB {
    let options = Options::default();
    let descriptors = DB::list_cf(&options, database_directory)
        .expect("existing column families are listed")
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect::<Vec<_>>();

    DB::open_cf_descriptors(&options, database_directory, descriptors)
        .expect("configuration test database opens")
}

fn clear_configuration_metadata(database_directory: &std::path::Path) {
    let database = open_configuration_database(database_directory);
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
