use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use time::UtcDateTime;

use crate::mount::{self, MountedFilesystem};
use crate::storage::{self, Storage};

/// Event-sourced filesystem handle backed by a RocksDB database.
#[derive(Clone)]
pub struct Filesystem {
    configuration: FilesystemConfiguration,
    database_directory: PathBuf,
    storage: Arc<Storage>,
    mount_state: Arc<Mutex<MountState>>,
}

impl Filesystem {
    /// Opens or creates the configured filesystem database.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when the database
    /// cannot be opened or required storage metadata is invalid.
    pub fn open(configuration: FilesystemConfiguration) -> Result<Self, FilesystemError> {
        open_filesystem(configuration)
    }

    /// Mounts the filesystem and blocks until it is unmounted.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::FilesystemOperation`] when the mount point is invalid or FUSE
    /// mounting fails.
    pub fn mount(&self) -> Result<(), FilesystemError> {
        mount::mount(self)
    }

    /// Mounts the filesystem in the background.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::FilesystemOperation`] when the mount point is invalid or FUSE
    /// mounting fails.
    pub fn spawn_mount(&self) -> Result<MountedFilesystem, FilesystemError> {
        mount::spawn_mount(self)
    }

    /// Lists committed filesystem events after an optional cursor.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when event indexes
    /// cannot be read or decoded.
    pub fn list_events(
        &self,
        after: Option<EventSequence>,
        limit: EventPageLimit,
    ) -> Result<EventPage, FilesystemError> {
        self.storage.list_events(after, limit)
    }

    /// Returns one committed filesystem event by sequence.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when the event
    /// record cannot be read or decoded.
    pub fn get_event(
        &self,
        sequence: EventSequence,
    ) -> Result<Option<EventRecord>, FilesystemError> {
        self.storage.get_event(sequence)
    }

    /// Returns the currently active branch.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when active branch
    /// metadata cannot be read or decoded.
    pub fn current_branch(&self) -> Result<BranchRecord, FilesystemError> {
        self.storage.current_branch()
    }

    /// Lists branches after an optional branch identifier cursor.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when branch records
    /// cannot be read or decoded.
    pub fn list_branches(
        &self,
        after: Option<BranchIdentifier>,
        limit: BranchPageLimit,
    ) -> Result<BranchPage, FilesystemError> {
        self.storage.list_branches(after, limit)
    }

    /// Creates a branch from an existing branch position without switching to it.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Integrity`] when the name already exists or the source position
    /// is invalid, and [`FilesystemError::Database`] when the branch cannot be persisted.
    pub fn create_branch(
        &self,
        name: BranchName,
        from: BranchPosition,
    ) -> Result<BranchRecord, FilesystemError> {
        self.storage.create_branch(name, from)
    }

    /// Switches the active branch when the filesystem is not mounted.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::FilesystemOperation`] when mounted, [`FilesystemError::Integrity`]
    /// when the branch cannot be switched to, and [`FilesystemError::Database`] when metadata
    /// cannot be updated.
    pub fn switch_branch(&self, name: &BranchName) -> Result<BranchRecord, FilesystemError> {
        let _mount_state = self.lock_unmounted()?;
        self.storage.switch_branch(name)
    }

    /// Deletes an inactive branch ref without deleting committed events.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Integrity`] when the branch is missing, active, or initial, and
    /// [`FilesystemError::Database`] when the ref cannot be updated.
    pub fn delete_branch(&self, name: &BranchName) -> Result<(), FilesystemError> {
        self.storage.delete_branch(name)
    }

    /// Lists committed filesystem events for one branch after an optional cursor.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Integrity`] when the cursor belongs to another branch, and
    /// [`FilesystemError::Database`] when event indexes cannot be read.
    pub fn list_branch_events(
        &self,
        branch: BranchIdentifier,
        after: Option<BranchPosition>,
        limit: EventPageLimit,
    ) -> Result<BranchEventPage, FilesystemError> {
        self.storage.list_branch_events(branch, after, limit)
    }

    /// Lists committed filesystem events for one regular file on one branch.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Integrity`] when the cursor belongs to another branch, and
    /// [`FilesystemError::Database`] when file event indexes cannot be read.
    pub fn list_branch_file_events(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        after: Option<BranchPosition>,
        limit: EventPageLimit,
    ) -> Result<BranchEventPage, FilesystemError> {
        self.storage
            .list_branch_file_events(branch, file_identifier, after, limit)
    }

    /// Lists committed filesystem events for one regular file on the active branch.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when file event
    /// indexes or records cannot be read.
    pub fn list_file_events(
        &self,
        file_identifier: FileIdentifier,
        after: Option<EventSequence>,
        limit: EventPageLimit,
    ) -> Result<EventPage, FilesystemError> {
        self.storage.list_file_events(file_identifier, after, limit)
    }

    /// Returns the newest persisted file snapshot at or before the requested event sequence.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when snapshot
    /// metadata cannot be read or decoded.
    pub fn file_snapshot_at_or_before(
        &self,
        file_identifier: FileIdentifier,
        sequence: EventSequence,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        self.storage
            .file_snapshot_at_or_before(file_identifier, sequence)
    }

    /// Returns the newest persisted file snapshot on a branch at or before a branch position.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Integrity`] when the position belongs to another branch, and
    /// [`FilesystemError::Database`] when snapshot metadata cannot be read.
    pub fn file_snapshot_on_branch_at_or_before(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        position: BranchPosition,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        self.storage
            .file_snapshot_on_branch_at_or_before(branch, file_identifier, position)
    }

    /// Reads a byte range from a persisted file snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when the referenced
    /// snapshot extents or chunks cannot be read.
    pub fn read_file_snapshot_range(
        &self,
        snapshot: &FileSnapshot,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        self.storage
            .read_file_snapshot_range(snapshot, offset, length)
    }

    /// Reads a byte range from one committed file event payload part.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when the referenced
    /// payload extents or chunks cannot be read.
    pub fn read_file_event_payload_range(
        &self,
        sequence: EventSequence,
        part: FileEventPayloadPart,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        self.storage
            .read_file_event_payload_range(sequence, part, offset, length)
    }
}

/// Configuration required to open and mount a filesystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilesystemConfiguration {
    database_directory: PathBuf,
    mount_point: PathBuf,
}

/// Type-state builder for filesystem configuration.
pub struct FilesystemConfigurationBuilder<State> {
    state: State,
}

/// Builder state requiring a database directory.
pub struct WantsDatabaseDirectory;

/// Builder state requiring a mount point.
pub struct WantsMountPoint {
    database_directory: PathBuf,
}

/// Builder state containing all required filesystem configuration values.
pub struct Ready {
    database_directory: PathBuf,
    mount_point: PathBuf,
}

impl FilesystemConfiguration {
    /// Starts a filesystem configuration builder.
    pub fn builder() -> FilesystemConfigurationBuilder<WantsDatabaseDirectory> {
        FilesystemConfigurationBuilder {
            state: WantsDatabaseDirectory,
        }
    }
}

impl FilesystemConfigurationBuilder<WantsDatabaseDirectory> {
    /// Sets the local RocksDB database directory.
    pub fn database_directory(
        self,
        database_directory: PathBuf,
    ) -> FilesystemConfigurationBuilder<WantsMountPoint> {
        FilesystemConfigurationBuilder {
            state: WantsMountPoint { database_directory },
        }
    }
}

impl FilesystemConfigurationBuilder<WantsMountPoint> {
    /// Sets the FUSE mount point directory.
    pub fn mount_point(self, mount_point: PathBuf) -> FilesystemConfigurationBuilder<Ready> {
        FilesystemConfigurationBuilder {
            state: Ready {
                database_directory: self.state.database_directory,
                mount_point,
            },
        }
    }
}

impl FilesystemConfigurationBuilder<Ready> {
    /// Builds a filesystem configuration and rejects empty paths.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError::EmptyValue`] when the database directory or mount point path is
    /// empty.
    pub fn build(self) -> Result<FilesystemConfiguration, ConfigurationError> {
        new_filesystem_configuration(self.state.database_directory, self.state.mount_point)
    }
}

/// Strictly ordered event sequence number.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct EventSequence(u64);

impl EventSequence {
    /// Creates an event sequence from its numeric value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric sequence value.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Maximum number of event records returned by one listing call.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventPageLimit(NonZeroU64);

impl EventPageLimit {
    /// Returns the page limit value.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for EventPageLimit {
    type Error = ConfigurationError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        new_event_page_limit(value)
    }
}

/// Maximum number of branch records returned by one listing call.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BranchPageLimit(NonZeroU64);

impl BranchPageLimit {
    /// Returns the page limit value.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for BranchPageLimit {
    type Error = ConfigurationError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        new_branch_page_limit(value)
    }
}

/// Stable identifier for a regular file.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct FileIdentifier(u64);

impl FileIdentifier {
    /// Creates a file identifier from its numeric value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric file identifier value.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Stable identifier for a branch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct BranchIdentifier(u64);

impl BranchIdentifier {
    /// Creates a branch identifier from its numeric value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric branch identifier value.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Name of a branch.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct BranchName(String);

impl BranchName {
    /// Creates a branch name and rejects empty names.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError::EmptyValue`] when the supplied name is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigurationError> {
        new_branch_name(value.into())
    }

    /// Returns the branch name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Branch lifecycle state.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum BranchStatus {
    /// Branch can be switched to and extended.
    Open,
    /// Branch ref was deleted and is retained only for historical integrity.
    Deleted,
}

/// Position of an event on one branch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct BranchPosition {
    branch_identifier: BranchIdentifier,
    ordinal: u64,
}

impl BranchPosition {
    /// Returns the branch identifier for this position.
    pub fn branch_identifier(&self) -> BranchIdentifier {
        self.branch_identifier
    }

    /// Returns the branch-local ordinal.
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
}

/// Branch metadata exposed by branch listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchRecord {
    branch_identifier: BranchIdentifier,
    name: BranchName,
    status: BranchStatus,
    head_position: BranchPosition,
    head_sequence: EventSequence,
}

impl BranchRecord {
    /// Returns the branch identifier.
    pub fn branch_identifier(&self) -> BranchIdentifier {
        self.branch_identifier
    }

    /// Returns the branch name.
    pub fn name(&self) -> &BranchName {
        &self.name
    }

    /// Returns the branch lifecycle state.
    pub fn status(&self) -> BranchStatus {
        self.status
    }

    /// Returns the current branch head position.
    pub fn head_position(&self) -> BranchPosition {
        self.head_position
    }

    /// Returns the global event sequence at the branch head.
    pub fn head_sequence(&self) -> EventSequence {
        self.head_sequence
    }
}

/// Page of branch records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchPage {
    records: Vec<BranchRecord>,
    next_after: Option<BranchIdentifier>,
}

impl BranchPage {
    /// Returns the branch records in this page.
    pub fn records(&self) -> &[BranchRecord] {
        &self.records
    }

    /// Returns the next branch cursor when another page exists.
    pub fn next_after(&self) -> Option<BranchIdentifier> {
        self.next_after
    }
}

/// Persisted metadata for one regular file at a committed branch position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileSnapshot {
    file_identifier: FileIdentifier,
    sequence: EventSequence,
    branch_position: BranchPosition,
    file_size: u64,
}

impl FileSnapshot {
    /// Returns the regular file identifier captured by this snapshot.
    pub fn file_identifier(&self) -> FileIdentifier {
        self.file_identifier
    }

    /// Returns the source event sequence captured by this snapshot.
    pub fn sequence(&self) -> EventSequence {
        self.sequence
    }

    /// Returns the branch position captured by this snapshot.
    pub fn branch_position(&self) -> BranchPosition {
        self.branch_position
    }

    /// Returns the file size captured by this snapshot.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

/// Payload part stored for one committed file event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FileEventPayloadPart {
    /// Bytes overwritten by a write.
    Overwritten,
    /// Bytes written by a write.
    Written,
    /// Bytes removed by a truncate.
    Removed,
}

/// Kind of committed filesystem event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum EventKind {
    /// Filesystem initialization event.
    FilesystemInitialized,
    /// Special node creation event.
    NodeCreated,
    /// Directory creation event.
    DirectoryCreated,
    /// Regular file creation event.
    FileCreated,
    /// Regular file write event.
    FileWritten,
    /// Regular file truncate event.
    FileTruncated,
    /// Metadata change event.
    MetadataChanged,
    /// Node unlink event.
    NodeUnlinked,
    /// Directory removal event.
    DirectoryRemoved,
    /// Node rename event.
    NodeRenamed,
    /// Hard link creation event.
    HardLinkCreated,
    /// Symbolic link creation event.
    SymbolicLinkCreated,
}

/// Committed filesystem event exposed by event listing.
#[derive(Clone, Deserialize, Serialize)]
pub struct EventRecord {
    schema_version: u64,
    sequence: EventSequence,
    kind: EventKind,
    created_at: UtcDateTime,
    file_identifier: Option<FileIdentifier>,
    branch_identifier: Option<BranchIdentifier>,
    branch_position: Option<BranchPosition>,
    first_parent_sequence: Option<EventSequence>,
    path: Option<String>,
    offset: Option<u64>,
    byte_length: Option<u64>,
    payload: EventPayload,
}

impl EventRecord {
    /// Returns the event sequence.
    pub fn sequence(&self) -> EventSequence {
        self.sequence
    }

    /// Returns the event kind.
    pub fn kind(&self) -> EventKind {
        self.kind
    }

    /// Returns the UTC time when the event was created.
    pub fn created_at(&self) -> UtcDateTime {
        self.created_at
    }

    /// Returns the affected regular file identifier, when applicable.
    pub fn file_identifier(&self) -> Option<FileIdentifier> {
        self.file_identifier
    }

    /// Returns the branch identifier, when the event belongs to a branch.
    pub fn branch_identifier(&self) -> Option<BranchIdentifier> {
        self.branch_identifier
    }

    /// Returns the branch-local position, when the event belongs to a branch.
    pub fn branch_position(&self) -> Option<BranchPosition> {
        self.branch_position
    }

    /// Returns the first parent sequence, when the event belongs to a branch.
    pub fn first_parent_sequence(&self) -> Option<EventSequence> {
        self.first_parent_sequence
    }

    /// Returns the affected path, when applicable.
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// Returns the affected byte offset, when applicable.
    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    /// Returns the affected byte length, when applicable.
    pub fn byte_length(&self) -> Option<u64> {
        self.byte_length
    }

    /// Returns the old file size for file write and truncate events.
    pub fn old_file_size(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileWrite { old_file_size, .. }
            | EventPayload::FileTruncate { old_file_size, .. } => Some(*old_file_size),
            EventPayload::None => None,
        }
    }

    /// Returns the new file size for file write and truncate events.
    pub fn new_file_size(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileWrite { new_file_size, .. }
            | EventPayload::FileTruncate { new_file_size, .. } => Some(*new_file_size),
            EventPayload::None => None,
        }
    }

    /// Returns the number of bytes overwritten by a file write event.
    pub fn overwritten_byte_length(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileWrite {
                overwritten_byte_length,
                ..
            } => Some(*overwritten_byte_length),
            EventPayload::None | EventPayload::FileTruncate { .. } => None,
        }
    }

    /// Returns the number of bytes written by a file write event.
    pub fn written_byte_length(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileWrite {
                written_byte_length,
                ..
            } => Some(*written_byte_length),
            EventPayload::None | EventPayload::FileTruncate { .. } => None,
        }
    }

    /// Returns the number of bytes removed by a file truncate event.
    pub fn removed_byte_length(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileTruncate {
                removed_byte_length,
                ..
            } => Some(*removed_byte_length),
            EventPayload::None | EventPayload::FileWrite { .. } => None,
        }
    }
}

impl fmt::Debug for EventRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventRecord")
            .field("schema_version", &self.schema_version)
            .field("sequence", &self.sequence)
            .field("kind", &self.kind)
            .field("created_at", &self.created_at)
            .field("file_identifier", &self.file_identifier)
            .field("branch_identifier", &self.branch_identifier)
            .field("branch_position", &self.branch_position)
            .field("first_parent_sequence", &self.first_parent_sequence)
            .field("path", &self.path)
            .field("offset", &self.offset)
            .field("byte_length", &self.byte_length)
            .finish()
    }
}

impl PartialEq for EventRecord {
    fn eq(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.sequence == other.sequence
            && self.kind == other.kind
            && self.created_at == other.created_at
            && self.file_identifier == other.file_identifier
            && self.branch_identifier == other.branch_identifier
            && self.branch_position == other.branch_position
            && self.first_parent_sequence == other.first_parent_sequence
            && self.path == other.path
            && self.offset == other.offset
            && self.byte_length == other.byte_length
            && self.payload == other.payload
    }
}

impl Eq for EventRecord {}

/// Page of committed filesystem events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPage {
    records: Vec<EventRecord>,
    next_after: Option<EventSequence>,
}

impl EventPage {
    /// Returns the event records in this page.
    pub fn records(&self) -> &[EventRecord] {
        &self.records
    }

    /// Returns the next event cursor when another page exists.
    pub fn next_after(&self) -> Option<EventSequence> {
        self.next_after
    }
}

/// Page of committed filesystem events ordered by branch position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchEventPage {
    records: Vec<EventRecord>,
    next_after: Option<BranchPosition>,
}

impl BranchEventPage {
    /// Returns the event records in this page.
    pub fn records(&self) -> &[EventRecord] {
        &self.records
    }

    /// Returns the next branch cursor when another page exists.
    pub fn next_after(&self) -> Option<BranchPosition> {
        self.next_after
    }
}

/// Error returned by public configuration value constructors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationError {
    /// A required path value was empty.
    EmptyValue,
    /// A required non-zero numeric value was zero.
    ZeroValue,
}

/// Error returned by filesystem operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemError {
    /// A mounted filesystem operation failed.
    FilesystemOperation,
    /// A RocksDB operation failed.
    Database,
    /// Stored data or backup data failed an integrity check.
    Integrity,
    /// A backup operation failed.
    Backup,
    /// An import operation failed.
    Import,
}

impl fmt::Debug for Filesystem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Filesystem")
            .field("configuration", &self.configuration)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyValue => formatter.write_str("configuration value must not be empty"),
            Self::ZeroValue => formatter.write_str("configuration value must be non-zero"),
        }
    }
}

impl std::error::Error for ConfigurationError {}

impl fmt::Display for FilesystemError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FilesystemOperation => formatter.write_str("filesystem operation failed"),
            Self::Database => formatter.write_str("database operation failed"),
            Self::Integrity => formatter.write_str("integrity check failed"),
            Self::Backup => formatter.write_str("backup operation failed"),
            Self::Import => formatter.write_str("import operation failed"),
        }
    }
}

impl std::error::Error for FilesystemError {}

impl EventRecord {
    pub(crate) fn new(
        sequence: EventSequence,
        kind: EventKind,
        created_at: UtcDateTime,
        file_identifier: Option<FileIdentifier>,
        path: Option<String>,
        offset: Option<u64>,
        byte_length: Option<u64>,
    ) -> Self {
        Self {
            schema_version: 6,
            sequence,
            kind,
            created_at,
            file_identifier,
            branch_identifier: None,
            branch_position: None,
            first_parent_sequence: None,
            path,
            offset,
            byte_length,
            payload: EventPayload::None,
        }
    }

    pub(crate) fn with_payload(mut self, payload: EventPayload) -> Self {
        self.payload = payload;
        self
    }

    pub(crate) fn with_branch(
        mut self,
        branch_identifier: BranchIdentifier,
        branch_position: BranchPosition,
        first_parent_sequence: EventSequence,
    ) -> Self {
        self.branch_identifier = Some(branch_identifier);
        self.branch_position = Some(branch_position);
        self.first_parent_sequence = Some(first_parent_sequence);
        self
    }
}

impl BranchPosition {
    pub(crate) fn new(branch_identifier: BranchIdentifier, ordinal: u64) -> Self {
        Self {
            branch_identifier,
            ordinal,
        }
    }
}

impl BranchRecord {
    pub(crate) fn new(
        branch_identifier: BranchIdentifier,
        name: BranchName,
        status: BranchStatus,
        head_position: BranchPosition,
        head_sequence: EventSequence,
    ) -> Self {
        Self {
            branch_identifier,
            name,
            status,
            head_position,
            head_sequence,
        }
    }
}

impl BranchPage {
    pub(crate) fn new(records: Vec<BranchRecord>, next_after: Option<BranchIdentifier>) -> Self {
        Self {
            records,
            next_after,
        }
    }
}

impl FileSnapshot {
    pub(crate) fn new(
        file_identifier: FileIdentifier,
        sequence: EventSequence,
        branch_position: BranchPosition,
        file_size: u64,
    ) -> Self {
        Self {
            file_identifier,
            sequence,
            branch_position,
            file_size,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) enum EventPayload {
    None,
    FileWrite {
        old_file_size: u64,
        new_file_size: u64,
        overwritten_byte_length: u64,
        written_byte_length: u64,
    },
    FileTruncate {
        old_file_size: u64,
        new_file_size: u64,
        removed_byte_length: u64,
    },
}

impl EventPage {
    pub(crate) fn new(records: Vec<EventRecord>, next_after: Option<EventSequence>) -> Self {
        Self {
            records,
            next_after,
        }
    }
}

impl BranchEventPage {
    pub(crate) fn new(records: Vec<EventRecord>, next_after: Option<BranchPosition>) -> Self {
        Self {
            records,
            next_after,
        }
    }
}

impl FilesystemConfiguration {
    pub(crate) fn database_directory(&self) -> &Path {
        &self.database_directory
    }

    pub(crate) fn mount_point(&self) -> &Path {
        &self.mount_point
    }
}

impl fuser::Filesystem for Filesystem {
    fn lookup(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        match self.storage.lookup(parent.into(), name) {
            Ok(entry) => reply.entry(&entry.ttl, &entry.attr, fuser::Generation(0)),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn getattr(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: Option<fuser::FileHandle>,
        reply: fuser::ReplyAttr,
    ) {
        match self.storage.getattr(ino.into()) {
            Ok(entry) => reply.attr(&entry.ttl, &entry.attr),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn setattr(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<fuser::FileHandle>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<fuser::BsdFileFlags>,
        reply: fuser::ReplyAttr,
    ) {
        let attributes = storage::SetAttributes {
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            ctime,
            crtime,
            flags: flags.map(|flags| flags.bits()),
        };
        match self
            .storage
            .setattr(ino.into(), attributes, _req.uid(), _req.gid())
        {
            Ok(entry) => reply.attr(&entry.ttl, &entry.attr),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn readlink(&self, _req: &fuser::Request, ino: fuser::INodeNo, reply: fuser::ReplyData) {
        match self.storage.readlink(ino.into()) {
            Ok(target) => reply.data(&target),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn mknod(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        match self.storage.create_node(
            parent.into(),
            name,
            mode,
            umask,
            storage::FuseCredentials {
                uid: _req.uid(),
                gid: _req.gid(),
            },
            storage::CreateNodeKind::Special { mode, rdev },
        ) {
            Ok(entry) => reply.entry(&entry.ttl, &entry.attr, fuser::Generation(0)),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn mkdir(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        match self.storage.create_node(
            parent.into(),
            name,
            mode,
            umask,
            storage::FuseCredentials {
                uid: _req.uid(),
                gid: _req.gid(),
            },
            storage::CreateNodeKind::Directory,
        ) {
            Ok(entry) => reply.entry(&entry.ttl, &entry.attr, fuser::Generation(0)),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn unlink(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        match self
            .storage
            .unlink(parent.into(), name, _req.uid(), _req.gid())
        {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn rmdir(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        match self
            .storage
            .rmdir(parent.into(), name, _req.uid(), _req.gid())
        {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn symlink(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: fuser::ReplyEntry,
    ) {
        match self
            .storage
            .create_symlink(parent.into(), link_name, target, _req.uid(), _req.gid())
        {
            Ok(entry) => reply.entry(&entry.ttl, &entry.attr, fuser::Generation(0)),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn rename(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        newparent: fuser::INodeNo,
        newname: &OsStr,
        flags: fuser::RenameFlags,
        reply: fuser::ReplyEmpty,
    ) {
        match rename_mode(flags).and_then(|no_replace| {
            self.storage.rename(
                parent.into(),
                name,
                newparent.into(),
                newname,
                no_replace,
                storage::FuseCredentials {
                    uid: _req.uid(),
                    gid: _req.gid(),
                },
            )
        }) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn link(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        newparent: fuser::INodeNo,
        newname: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        match self.storage.hard_link(
            ino.into(),
            newparent.into(),
            newname,
            _req.uid(),
            _req.gid(),
        ) {
            Ok(entry) => reply.entry(&entry.ttl, &entry.attr, fuser::Generation(0)),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn open(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _flags: fuser::OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        match self
            .storage
            .open_file(ino.into(), _req.uid(), _req.gid(), _flags)
        {
            Ok(()) => reply.opened(fuser::FileHandle(0), fuser::FopenFlags::empty()),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn read(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyData,
    ) {
        match self
            .storage
            .read_file(ino.into(), offset, size, _req.uid(), _req.gid())
        {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn write(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        match self
            .storage
            .write_file(ino.into(), offset, data, _req.uid(), _req.gid())
        {
            Ok(write) => reply.written(write.written),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn flush(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn opendir(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _flags: fuser::OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        match self.storage.opendir(ino.into()) {
            Ok(()) => reply.opened(fuser::FileHandle(0), fuser::FopenFlags::empty()),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn readdir(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let result = self.storage.readdir(ino.into(), offset, |entry| {
            !reply.add(
                fuser::INodeNo(entry.inode),
                entry.offset,
                entry.kind,
                entry.name,
            )
        });
        match result {
            Ok(()) => {
                reply.ok();
            }
            Err(error) => reply.error(error.errno()),
        }
    }

    fn releasedir(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _flags: fuser::OpenFlags,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&self, _req: &fuser::Request, _ino: fuser::INodeNo, reply: fuser::ReplyStatfs) {
        match self.storage.statfs(self.configuration.database_directory()) {
            Ok(statistics) => reply.statfs(
                statistics.blocks,
                statistics.free_blocks,
                statistics.available_blocks,
                statistics.files,
                statistics.free_files,
                statistics.block_size,
                statistics.maximum_name_length,
                statistics.fragment_size,
            ),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn setxattr(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn getxattr(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: fuser::ReplyXattr,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn listxattr(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _size: u32,
        reply: fuser::ReplyXattr,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn removexattr(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn access(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        mask: fuser::AccessFlags,
        reply: fuser::ReplyEmpty,
    ) {
        match self
            .storage
            .access(ino.into(), _req.uid(), _req.gid(), mask)
        {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
        }
    }

    fn create(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        match self.storage.create_node(
            parent.into(),
            name,
            mode,
            umask,
            storage::FuseCredentials {
                uid: _req.uid(),
                gid: _req.gid(),
            },
            storage::CreateNodeKind::RegularFile,
        ) {
            Ok(entry) => {
                reply.created(
                    &entry.ttl,
                    &entry.attr,
                    fuser::Generation(0),
                    fuser::FileHandle(0),
                    fuser::FopenFlags::empty(),
                );
            }
            Err(error) => reply.error(error.errno()),
        }
    }

    fn readdirplus(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _offset: u64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn getlk(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _lock_owner: fuser::LockOwner,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn setlk(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _lock_owner: fuser::LockOwner,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        _sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn bmap(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _blocksize: u32,
        _idx: u64,
        reply: fuser::ReplyBmap,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn ioctl(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _flags: fuser::IoctlFlags,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn poll(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _ph: fuser::PollNotifier,
        _events: fuser::PollEvents,
        _flags: fuser::PollFlags,
        reply: fuser::ReplyPoll,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn fallocate(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _offset: u64,
        _length: u64,
        _mode: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn lseek(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _offset: i64,
        _whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        reply.error(unsupported_operation_error());
    }

    fn copy_file_range(
        &self,
        _req: &fuser::Request,
        _ino_in: fuser::INodeNo,
        _fh_in: fuser::FileHandle,
        _offset_in: u64,
        _ino_out: fuser::INodeNo,
        _fh_out: fuser::FileHandle,
        _offset_out: u64,
        _len: u64,
        _flags: fuser::CopyFileRangeFlags,
        reply: fuser::ReplyWrite,
    ) {
        reply.error(unsupported_operation_error());
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&self, _req: &fuser::Request, _name: &OsStr, reply: fuser::ReplyEmpty) {
        reply.error(unsupported_operation_error());
    }

    #[cfg(target_os = "macos")]
    fn exchange(
        &self,
        _req: &fuser::Request,
        _parent: fuser::INodeNo,
        _name: &OsStr,
        _newparent: fuser::INodeNo,
        _newname: &OsStr,
        _options: u64,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(unsupported_operation_error());
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&self, _req: &fuser::Request, _ino: fuser::INodeNo, reply: fuser::ReplyXTimes) {
        reply.error(unsupported_operation_error());
    }
}

#[cfg(target_os = "macos")]
fn unsupported_operation_error() -> fuser::Errno {
    fuser::Errno::ENOTSUP
}

#[cfg(not(target_os = "macos"))]
fn unsupported_operation_error() -> fuser::Errno {
    fuser::Errno::EOPNOTSUPP
}

fn rename_mode(flags: fuser::RenameFlags) -> Result<bool, storage::FuseError> {
    #[cfg(target_os = "linux")]
    {
        let supported = fuser::RenameFlags::RENAME_NOREPLACE;
        if !(flags - supported).is_empty() {
            return Err(storage::FuseError::Errno(fuser::Errno::EINVAL));
        }
        Ok(flags.contains(fuser::RenameFlags::RENAME_NOREPLACE))
    }
    #[cfg(not(target_os = "linux"))]
    {
        if !flags.is_empty() {
            return Err(storage::FuseError::Errno(fuser::Errno::EINVAL));
        }
        Ok(false)
    }
}

impl Filesystem {
    pub(crate) fn configuration(&self) -> &FilesystemConfiguration {
        &self.configuration
    }

    pub(crate) fn database_directory(&self) -> &Path {
        &self.database_directory
    }

    pub(crate) fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }

    pub(crate) fn mark_mounted(&self) -> Result<(), FilesystemError> {
        let mut state = self
            .mount_state
            .lock()
            .map_err(|_| FilesystemError::FilesystemOperation)?;
        state.mounted_sessions = state
            .mounted_sessions
            .checked_add(1)
            .ok_or(FilesystemError::FilesystemOperation)?;
        Ok(())
    }

    pub(crate) fn mark_unmounted(&self) -> Result<(), FilesystemError> {
        let mut state = self
            .mount_state
            .lock()
            .map_err(|_| FilesystemError::FilesystemOperation)?;
        if state.mounted_sessions == 0 {
            return Err(FilesystemError::FilesystemOperation);
        }
        state.mounted_sessions -= 1;
        Ok(())
    }

    fn lock_unmounted(&self) -> Result<MutexGuard<'_, MountState>, FilesystemError> {
        let state = self
            .mount_state
            .lock()
            .map_err(|_| FilesystemError::FilesystemOperation)?;
        if state.mounted_sessions != 0 {
            return Err(FilesystemError::FilesystemOperation);
        }
        Ok(state)
    }
}

#[derive(Debug, Default)]
struct MountState {
    mounted_sessions: usize,
}

fn open_filesystem(configuration: FilesystemConfiguration) -> Result<Filesystem, FilesystemError> {
    let storage = Arc::new(storage::open_database(&configuration)?);
    let database_directory = fs::canonicalize(configuration.database_directory())
        .map_err(|_| FilesystemError::Database)?;
    Ok(Filesystem {
        configuration,
        database_directory,
        storage,
        mount_state: Arc::new(Mutex::new(MountState::default())),
    })
}

fn new_filesystem_configuration(
    database_directory: PathBuf,
    mount_point: PathBuf,
) -> Result<FilesystemConfiguration, ConfigurationError> {
    validate_configuration_path(&database_directory)?;
    validate_configuration_path(&mount_point)?;
    Ok(FilesystemConfiguration {
        database_directory,
        mount_point,
    })
}

fn validate_configuration_path(path: &Path) -> Result<(), ConfigurationError> {
    if path.as_os_str().is_empty() {
        return Err(ConfigurationError::EmptyValue);
    }
    Ok(())
}

fn new_event_page_limit(value: u64) -> Result<EventPageLimit, ConfigurationError> {
    NonZeroU64::new(value)
        .map(EventPageLimit)
        .ok_or(ConfigurationError::ZeroValue)
}

fn new_branch_page_limit(value: u64) -> Result<BranchPageLimit, ConfigurationError> {
    NonZeroU64::new(value)
        .map(BranchPageLimit)
        .ok_or(ConfigurationError::ZeroValue)
}

fn new_branch_name(value: String) -> Result<BranchName, ConfigurationError> {
    if value.is_empty() {
        return Err(ConfigurationError::EmptyValue);
    }
    Ok(BranchName(value))
}
