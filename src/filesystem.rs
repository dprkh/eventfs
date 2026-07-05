use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::num::NonZeroU64;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
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
    lock_table: Arc<(Mutex<LockTable>, Condvar)>,
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

    /// Iterates over committed filesystem events.
    ///
    /// # Errors
    ///
    /// Yields [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when event indexes
    /// cannot be read or decoded.
    pub fn events(
        &self,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
        events(self, limit)
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

    /// Iterates over branches.
    ///
    /// # Errors
    ///
    /// Yields [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when branch records
    /// cannot be read or decoded.
    pub fn branches(
        &self,
        limit: BranchPageLimit,
    ) -> impl Iterator<Item = Result<BranchRecord, FilesystemError>> + '_ {
        branches(self, limit)
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
        name: &BranchName,
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

    /// Iterates over committed filesystem events for one branch.
    ///
    /// # Errors
    ///
    /// Yields [`FilesystemError::Integrity`] when the cursor belongs to another branch, and
    /// [`FilesystemError::Database`] when event indexes cannot be read.
    pub fn branch_events(
        &self,
        branch: BranchIdentifier,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
        branch_events(self, branch, limit)
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

    /// Iterates over committed filesystem events for one regular file on one branch.
    ///
    /// # Errors
    ///
    /// Yields [`FilesystemError::Integrity`] when the cursor belongs to another branch, and
    /// [`FilesystemError::Database`] when file event indexes cannot be read.
    pub fn branch_file_events(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
        branch_file_events(self, branch, file_identifier, limit)
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

    /// Iterates over committed filesystem events for one regular file on the active branch.
    ///
    /// # Errors
    ///
    /// Yields [`FilesystemError::Database`] or [`FilesystemError::Integrity`] when file event
    /// indexes or records cannot be read.
    pub fn file_events(
        &self,
        file_identifier: FileIdentifier,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
        file_events(self, file_identifier, limit)
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
#[derive(Clone)]
pub struct FilesystemConfiguration {
    database_directory: PathBuf,
    mount_point: PathBuf,
    fuse_error_callback: Option<Arc<FuseErrorCallback>>,
}

impl FilesystemConfiguration {
    /// Creates a filesystem configuration and rejects empty paths.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError::EmptyValue`] when the database directory or mount point path is
    /// empty.
    pub fn new(
        database_directory: impl Into<PathBuf>,
        mount_point: impl Into<PathBuf>,
    ) -> Result<Self, ConfigurationError> {
        new_filesystem_configuration(database_directory.into(), mount_point.into())
    }

    /// Configures a callback invoked after failed or unsupported FUSE operations.
    pub fn with_fuse_error_callback(
        self,
        callback: impl Fn(FuseOperationError) + Send + Sync + 'static,
    ) -> Self {
        with_fuse_error_callback(self, callback)
    }
}

/// Error context passed to configured FUSE operation failure callbacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FuseOperationError {
    operation: &'static str,
    errno: i32,
    filesystem_error: FilesystemError,
    unsupported: bool,
}

impl FuseOperationError {
    /// Returns the FUSE operation name.
    pub fn operation(&self) -> &'static str {
        fuse_operation_error_operation(self)
    }

    /// Returns the positive platform errno returned to FUSE.
    pub fn errno(&self) -> i32 {
        fuse_operation_error_errno(self)
    }

    /// Returns the mapped public filesystem error.
    pub fn filesystem_error(&self) -> FilesystemError {
        fuse_operation_error_filesystem_error(self)
    }

    /// Returns whether the operation is explicitly unsupported by eventfs.
    pub fn is_unsupported(&self) -> bool {
        fuse_operation_error_is_unsupported(self)
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
    /// Creates an event page limit and rejects zero.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError::ZeroValue`] when the supplied value is zero.
    pub fn new(value: u64) -> Result<Self, ConfigurationError> {
        new_event_page_limit(value)
    }

    /// Returns the page limit value.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// Maximum number of branch records returned by one listing call.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BranchPageLimit(NonZeroU64);

impl BranchPageLimit {
    /// Creates a branch page limit and rejects zero.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError::ZeroValue`] when the supplied value is zero.
    pub fn new(value: u64) -> Result<Self, ConfigurationError> {
        new_branch_page_limit(value)
    }

    /// Returns the page limit value.
    pub fn get(self) -> u64 {
        self.0.get()
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

    /// Returns the branch records owned by this page.
    pub fn into_records(self) -> Vec<BranchRecord> {
        self.records
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
    /// Extended attribute set event.
    ExtendedAttributeSet,
    /// Extended attribute removal event.
    ExtendedAttributeRemoved,
    /// Regular file range zeroing event.
    FileRangeZeroed,
    /// Regular file contents exchange event.
    FileContentsExchanged,
    /// Volume rename event.
    VolumeRenamed,
}

/// Committed filesystem event exposed by event listing.
#[derive(Clone, Deserialize, Serialize)]
pub struct EventRecord {
    schema_version: u64,
    sequence: EventSequence,
    kind: EventKind,
    created_at: UtcDateTime,
    file_identifier: Option<FileIdentifier>,
    secondary_file_identifier: Option<FileIdentifier>,
    branch_identifier: Option<BranchIdentifier>,
    branch_position: Option<BranchPosition>,
    first_parent_sequence: Option<EventSequence>,
    path: Option<String>,
    secondary_path: Option<String>,
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

    /// Returns the secondary affected regular file identifier, when applicable.
    pub fn secondary_file_identifier(&self) -> Option<FileIdentifier> {
        self.secondary_file_identifier
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

    /// Returns the secondary affected path, when applicable.
    pub fn secondary_path(&self) -> Option<&str> {
        self.secondary_path.as_deref()
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
            EventPayload::None
            | EventPayload::FileSizeChange { .. }
            | EventPayload::FileExchange { .. } => None,
        }
    }

    /// Returns the new file size for file write and truncate events.
    pub fn new_file_size(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileWrite { new_file_size, .. }
            | EventPayload::FileTruncate { new_file_size, .. } => Some(*new_file_size),
            EventPayload::None
            | EventPayload::FileSizeChange { .. }
            | EventPayload::FileExchange { .. } => None,
        }
    }

    /// Returns the number of bytes overwritten by a file write event.
    pub fn overwritten_byte_length(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileWrite {
                overwritten_byte_length,
                ..
            } => Some(*overwritten_byte_length),
            EventPayload::None
            | EventPayload::FileTruncate { .. }
            | EventPayload::FileSizeChange { .. }
            | EventPayload::FileExchange { .. } => None,
        }
    }

    /// Returns the number of bytes written by a file write event.
    pub fn written_byte_length(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileWrite {
                written_byte_length,
                ..
            } => Some(*written_byte_length),
            EventPayload::None
            | EventPayload::FileTruncate { .. }
            | EventPayload::FileSizeChange { .. }
            | EventPayload::FileExchange { .. } => None,
        }
    }

    /// Returns the number of bytes removed by a file truncate event.
    pub fn removed_byte_length(&self) -> Option<u64> {
        match &self.payload {
            EventPayload::FileTruncate {
                removed_byte_length,
                ..
            } => Some(*removed_byte_length),
            EventPayload::None
            | EventPayload::FileWrite { .. }
            | EventPayload::FileSizeChange { .. }
            | EventPayload::FileExchange { .. } => None,
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
            .field("secondary_file_identifier", &self.secondary_file_identifier)
            .field("branch_identifier", &self.branch_identifier)
            .field("branch_position", &self.branch_position)
            .field("first_parent_sequence", &self.first_parent_sequence)
            .field("path", &self.path)
            .field("secondary_path", &self.secondary_path)
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
            && self.secondary_file_identifier == other.secondary_file_identifier
            && self.branch_identifier == other.branch_identifier
            && self.branch_position == other.branch_position
            && self.first_parent_sequence == other.first_parent_sequence
            && self.path == other.path
            && self.secondary_path == other.secondary_path
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

    /// Returns the event records owned by this page.
    pub fn into_records(self) -> Vec<EventRecord> {
        self.records
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

    /// Returns the event records owned by this page.
    pub fn into_records(self) -> Vec<EventRecord> {
        self.records
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

impl fmt::Debug for FilesystemConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FilesystemConfiguration")
            .field("database_directory", &self.database_directory)
            .field("mount_point", &self.mount_point)
            .field("fuse_error_callback", &self.fuse_error_callback.is_some())
            .finish()
    }
}

impl PartialEq for FilesystemConfiguration {
    fn eq(&self, other: &Self) -> bool {
        self.database_directory == other.database_directory
            && self.mount_point == other.mount_point
            && fuse_error_callbacks_equal(
                self.fuse_error_callback.as_ref(),
                other.fuse_error_callback.as_ref(),
            )
    }
}

impl Eq for FilesystemConfiguration {}

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

type FuseErrorCallback = dyn Fn(FuseOperationError) + Send + Sync + 'static;

fn fuse_error_callbacks_equal(
    left: Option<&Arc<FuseErrorCallback>>,
    right: Option<&Arc<FuseErrorCallback>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn fuse_operation_error_operation(error: &FuseOperationError) -> &'static str {
    error.operation
}

fn fuse_operation_error_errno(error: &FuseOperationError) -> i32 {
    error.errno
}

fn fuse_operation_error_filesystem_error(error: &FuseOperationError) -> FilesystemError {
    error.filesystem_error
}

fn fuse_operation_error_is_unsupported(error: &FuseOperationError) -> bool {
    error.unsupported
}

fn new_fuse_operation_error(
    operation: &'static str,
    errno: fuser::Errno,
    unsupported: bool,
) -> FuseOperationError {
    FuseOperationError {
        operation,
        errno: errno.code(),
        filesystem_error: FilesystemError::FilesystemOperation,
        unsupported,
    }
}

fn events(
    filesystem: &Filesystem,
    limit: EventPageLimit,
) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
    paged_records(move |after| filesystem.list_events(after, limit))
}

fn branches(
    filesystem: &Filesystem,
    limit: BranchPageLimit,
) -> impl Iterator<Item = Result<BranchRecord, FilesystemError>> + '_ {
    paged_records(move |after| filesystem.list_branches(after, limit))
}

fn branch_events(
    filesystem: &Filesystem,
    branch: BranchIdentifier,
    limit: EventPageLimit,
) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
    paged_records(move |after| filesystem.list_branch_events(branch, after, limit))
}

fn branch_file_events(
    filesystem: &Filesystem,
    branch: BranchIdentifier,
    file_identifier: FileIdentifier,
    limit: EventPageLimit,
) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
    paged_records(move |after| {
        filesystem.list_branch_file_events(branch, file_identifier, after, limit)
    })
}

fn file_events(
    filesystem: &Filesystem,
    file_identifier: FileIdentifier,
    limit: EventPageLimit,
) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_ {
    paged_records(move |after| filesystem.list_file_events(file_identifier, after, limit))
}

fn paged_records<Page, Cursor, Record>(
    mut load_page: impl FnMut(Option<Cursor>) -> Result<Page, FilesystemError>,
) -> impl Iterator<Item = Result<Record, FilesystemError>>
where
    Page: PagedRecords<Cursor = Cursor, Record = Record>,
    Cursor: Copy,
{
    let mut after = None;
    let mut records = Vec::<Record>::new().into_iter();
    let mut exhausted = false;

    std::iter::from_fn(move || {
        loop {
            if let Some(record) = records.next() {
                return Some(Ok(record));
            }
            if exhausted {
                return None;
            }

            match load_page(after) {
                Ok(page) => {
                    after = page.next_after();
                    exhausted = after.is_none();
                    let next_records = page.into_records();
                    if next_records.is_empty() {
                        exhausted = true;
                        return None;
                    }
                    records = next_records.into_iter();
                }
                Err(error) => {
                    exhausted = true;
                    return Some(Err(error));
                }
            }
        }
    })
}

trait PagedRecords {
    type Cursor: Copy;
    type Record;

    fn next_after(&self) -> Option<Self::Cursor>;

    fn into_records(self) -> Vec<Self::Record>;
}

impl PagedRecords for EventPage {
    type Cursor = EventSequence;
    type Record = EventRecord;

    fn next_after(&self) -> Option<Self::Cursor> {
        self.next_after
    }

    fn into_records(self) -> Vec<Self::Record> {
        self.records
    }
}

impl PagedRecords for BranchPage {
    type Cursor = BranchIdentifier;
    type Record = BranchRecord;

    fn next_after(&self) -> Option<Self::Cursor> {
        self.next_after
    }

    fn into_records(self) -> Vec<Self::Record> {
        self.records
    }
}

impl PagedRecords for BranchEventPage {
    type Cursor = BranchPosition;
    type Record = EventRecord;

    fn next_after(&self) -> Option<Self::Cursor> {
        self.next_after
    }

    fn into_records(self) -> Vec<Self::Record> {
        self.records
    }
}

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
            schema_version: 7,
            sequence,
            kind,
            created_at,
            file_identifier,
            secondary_file_identifier: None,
            branch_identifier: None,
            branch_position: None,
            first_parent_sequence: None,
            path,
            secondary_path: None,
            offset,
            byte_length,
            payload: EventPayload::None,
        }
    }

    pub(crate) fn with_secondary_file(
        mut self,
        file_identifier: FileIdentifier,
        path: Option<String>,
    ) -> Self {
        self.secondary_file_identifier = Some(file_identifier);
        self.secondary_path = path;
        self
    }

    pub(crate) fn with_payload(mut self, payload: EventPayload) -> Self {
        self.payload = payload;
        self
    }

    pub(crate) fn payload(&self) -> &EventPayload {
        &self.payload
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
    FileSizeChange {
        old_file_size: u64,
        new_file_size: u64,
    },
    FileExchange {
        primary_file_size: u64,
        secondary_file_size: u64,
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

    fn fuse_error_callback(&self) -> Option<&Arc<FuseErrorCallback>> {
        self.fuse_error_callback.as_ref()
    }
}

impl fuser::Filesystem for Filesystem {
    fn init(
        &mut self,
        _req: &fuser::Request,
        config: &mut fuser::KernelConfig,
    ) -> std::io::Result<()> {
        let mut capabilities =
            fuser::InitFlags::FUSE_POSIX_LOCKS | fuser::InitFlags::FUSE_DO_READDIRPLUS;
        #[cfg(target_os = "macos")]
        {
            capabilities |= fuser::InitFlags::FUSE_ALLOCATE
                | fuser::InitFlags::FUSE_EXCHANGE_DATA
                | fuser::InitFlags::FUSE_VOL_RENAME
                | fuser::InitFlags::FUSE_XTIMES;
        }
        let _ = config.add_capabilities(capabilities);
        Ok(())
    }

    fn lookup(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        match self.storage.lookup(parent.into(), name) {
            Ok(entry) => reply.entry(&entry.ttl, &entry.attr, fuser::Generation(0)),
            Err(error) => reply.error(self.fuse_error("lookup", error)),
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
            Err(error) => reply.error(self.fuse_error("getattr", error)),
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
        bkuptime: Option<SystemTime>,
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
            bkuptime,
            flags: flags.map(|flags| flags.bits()),
        };
        match self
            .storage
            .setattr(ino.into(), attributes, _req.uid(), _req.gid())
        {
            Ok(entry) => reply.attr(&entry.ttl, &entry.attr),
            Err(error) => reply.error(self.fuse_error("setattr", error)),
        }
    }

    fn readlink(&self, _req: &fuser::Request, ino: fuser::INodeNo, reply: fuser::ReplyData) {
        match self.storage.readlink(ino.into()) {
            Ok(target) => reply.data(&target),
            Err(error) => reply.error(self.fuse_error("readlink", error)),
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
            Err(error) => reply.error(self.fuse_error("mknod", error)),
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
            Err(error) => reply.error(self.fuse_error("mkdir", error)),
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
            Err(error) => reply.error(self.fuse_error("unlink", error)),
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
            Err(error) => reply.error(self.fuse_error("rmdir", error)),
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
            Err(error) => reply.error(self.fuse_error("symlink", error)),
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
            Err(error) => reply.error(self.fuse_error("rename", error)),
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
            Err(error) => reply.error(self.fuse_error("link", error)),
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
            Err(error) => reply.error(self.fuse_error("open", error)),
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
            Err(error) => reply.error(self.fuse_error("read", error)),
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
            Err(error) => reply.error(self.fuse_error("write", error)),
        }
    }

    fn flush(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        lock_owner: fuser::LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        self.clear_file_locks_for_owner(lock_owner);
        reply.ok();
    }

    fn release(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _flags: fuser::OpenFlags,
        lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        if let Some(lock_owner) = lock_owner {
            self.clear_file_locks_for_owner(lock_owner);
        }
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
            Err(error) => reply.error(self.fuse_error("opendir", error)),
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
            Err(error) => reply.error(self.fuse_error("readdir", error)),
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
            Err(error) => reply.error(self.fuse_error("statfs", error)),
        }
    }

    fn setxattr(
        &self,
        req: &fuser::Request,
        ino: fuser::INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let result = if position == 0 {
            self.storage
                .setxattr(ino.into(), name, value, flags, req.uid(), req.gid())
        } else {
            Err(storage::FuseError::Errno(fuser::Errno::EINVAL))
        };
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.fuse_error("setxattr", error)),
        }
    }

    fn getxattr(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        match self.storage.getxattr(ino.into(), name) {
            Ok(value) => reply_xattr_bytes(reply, "getxattr", self, &value, size),
            Err(error) => reply.error(self.fuse_error("getxattr", error)),
        }
    }

    fn listxattr(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        match self.storage.listxattr(ino.into()) {
            Ok(list) => reply_xattr_bytes(reply, "listxattr", self, &list.bytes, size),
            Err(error) => reply.error(self.fuse_error("listxattr", error)),
        }
    }

    fn removexattr(
        &self,
        req: &fuser::Request,
        ino: fuser::INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        match self.storage.removexattr(ino.into(), name, req.uid()) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.fuse_error("removexattr", error)),
        }
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
            Err(error) => reply.error(self.fuse_error("access", error)),
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
            Err(error) => reply.error(self.fuse_error("create", error)),
        }
    }

    fn readdirplus(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        let result = self.storage.readdirplus(ino.into(), offset, |entry| {
            !reply.add(
                fuser::INodeNo(entry.inode),
                entry.offset,
                entry.name,
                &entry.entry.ttl,
                &entry.entry.attr,
                fuser::Generation(0),
            )
        });
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.fuse_error("readdirplus", error)),
        }
    }

    fn getlk(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: fuser::ReplyLock,
    ) {
        match lock_type(typ).and_then(|typ| {
            self.storage.bmap(ino.into())?;
            let requested = FileLock {
                inode: ino.into(),
                owner: lock_owner.0,
                start,
                end,
                typ,
                pid,
            };
            let (lock_table, _condition) = &*self.lock_table;
            let table = lock_table
                .lock()
                .map_err(|_| storage::FuseError::Errno(fuser::Errno::EIO))?;
            Ok(table.conflicting_lock(requested))
        }) {
            Ok(Some(lock)) => reply.locked(lock.start, lock.end, lock.typ, lock.pid),
            Ok(None) => reply.locked(start, end, libc::F_UNLCK as i32, pid),
            Err(error) => reply.error(self.fuse_error("getlk", error)),
        }
    }

    fn setlk(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        match self.set_file_lock(ino.into(), lock_owner, start, end, typ, pid, sleep) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.fuse_error("setlk", error)),
        }
    }

    fn bmap(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _blocksize: u32,
        idx: u64,
        reply: fuser::ReplyBmap,
    ) {
        match self.storage.bmap(ino.into()) {
            Ok(()) => reply.bmap(idx),
            Err(error) => reply.error(self.fuse_error("bmap", error)),
        }
    }

    fn ioctl(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _flags: fuser::IoctlFlags,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        match self.storage.bmap(ino.into()) {
            Ok(()) => reply
                .error(self.fuse_error("ioctl", storage::FuseError::Errno(fuser::Errno::ENOTTY))),
            Err(error) => reply.error(self.fuse_error("ioctl", error)),
        }
    }

    fn poll(
        &self,
        req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        _ph: fuser::PollNotifier,
        events: fuser::PollEvents,
        _flags: fuser::PollFlags,
        reply: fuser::ReplyPoll,
    ) {
        match self.storage.poll(ino.into(), req.uid(), req.gid(), events) {
            Ok(ready) => reply.poll(ready),
            Err(error) => reply.error(self.fuse_error("poll", error)),
        }
    }

    fn fallocate(
        &self,
        req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: fuser::ReplyEmpty,
    ) {
        match self
            .storage
            .fallocate(ino.into(), offset, length, mode, req.uid(), req.gid())
        {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.fuse_error("fallocate", error)),
        }
    }

    fn lseek(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        offset: i64,
        whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        match self.storage.lseek(ino.into(), offset, whence) {
            Ok(offset) => reply.offset(offset),
            Err(error) => reply.error(self.fuse_error("lseek", error)),
        }
    }

    fn copy_file_range(
        &self,
        req: &fuser::Request,
        ino_in: fuser::INodeNo,
        _fh_in: fuser::FileHandle,
        offset_in: u64,
        ino_out: fuser::INodeNo,
        _fh_out: fuser::FileHandle,
        offset_out: u64,
        len: u64,
        flags: fuser::CopyFileRangeFlags,
        reply: fuser::ReplyWrite,
    ) {
        let result = if flags.is_empty() {
            self.storage.copy_file_range(
                ino_in.into(),
                offset_in,
                ino_out.into(),
                offset_out,
                len,
                req.uid(),
                req.gid(),
            )
        } else {
            Err(storage::FuseError::Errno(fuser::Errno::EINVAL))
        };
        match result {
            Ok(write) => reply.written(write.written),
            Err(error) => reply.error(self.fuse_error("copy_file_range", error)),
        }
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&self, _req: &fuser::Request, name: &OsStr, reply: fuser::ReplyEmpty) {
        match self.storage.set_volume_name(name) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.fuse_error("setvolname", error)),
        }
    }

    #[cfg(target_os = "macos")]
    fn exchange(
        &self,
        req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &OsStr,
        newparent: fuser::INodeNo,
        newname: &OsStr,
        options: u64,
        reply: fuser::ReplyEmpty,
    ) {
        let result = if options == 0 {
            self.storage.exchange(
                parent.into(),
                name,
                newparent.into(),
                newname,
                req.uid(),
                req.gid(),
            )
        } else {
            Err(storage::FuseError::Errno(fuser::Errno::EINVAL))
        };
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.fuse_error("exchange", error)),
        }
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&self, _req: &fuser::Request, ino: fuser::INodeNo, reply: fuser::ReplyXTimes) {
        match self.storage.getxtimes(ino.into()) {
            Ok(times) => reply.xtimes(times.bkuptime, times.crtime),
            Err(error) => reply.error(self.fuse_error("getxtimes", error)),
        }
    }
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

fn reply_xattr_bytes(
    reply: fuser::ReplyXattr,
    operation: &'static str,
    filesystem: &Filesystem,
    bytes: &[u8],
    size: u32,
) {
    let Ok(byte_len) = u32::try_from(bytes.len()) else {
        reply.error(filesystem.fuse_error(
            operation,
            storage::FuseError::Errno(fuser::Errno::EOVERFLOW),
        ));
        return;
    };
    if size == 0 {
        reply.size(byte_len);
    } else if size < byte_len {
        reply.error(
            filesystem.fuse_error(operation, storage::FuseError::Errno(fuser::Errno::ERANGE)),
        );
    } else {
        reply.data(bytes);
    }
}

fn lock_type(typ: i32) -> Result<i32, storage::FuseError> {
    if typ == libc::F_RDLCK as i32 || typ == libc::F_WRLCK as i32 || typ == libc::F_UNLCK as i32 {
        Ok(typ)
    } else {
        Err(storage::FuseError::Errno(fuser::Errno::EINVAL))
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

    fn fuse_error(&self, operation: &'static str, error: storage::FuseError) -> fuser::Errno {
        let errno = error.errno();
        self.notify_fuse_error(operation, errno, false);
        errno
    }

    fn set_file_lock(
        &self,
        inode: u64,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
    ) -> Result<(), storage::FuseError> {
        let typ = lock_type(typ)?;
        self.storage.bmap(inode)?;
        let requested = FileLock {
            inode,
            owner: lock_owner.0,
            start,
            end,
            typ,
            pid,
        };
        let (lock_table, condition) = &*self.lock_table;
        let mut table = lock_table
            .lock()
            .map_err(|_| storage::FuseError::Errno(fuser::Errno::EIO))?;
        loop {
            if typ == libc::F_UNLCK as i32 || table.conflicting_lock(requested).is_none() {
                table.set_lock(requested);
                condition.notify_all();
                return Ok(());
            }
            if !sleep {
                return Err(storage::FuseError::Errno(fuser::Errno::EAGAIN));
            }
            table = condition
                .wait(table)
                .map_err(|_| storage::FuseError::Errno(fuser::Errno::EIO))?;
        }
    }

    fn clear_file_locks_for_owner(&self, owner: fuser::LockOwner) {
        let (lock_table, condition) = &*self.lock_table;
        if let Ok(mut table) = lock_table.lock() {
            table.clear_owner(owner.0);
            condition.notify_all();
        }
    }

    fn notify_fuse_error(&self, operation: &'static str, errno: fuser::Errno, unsupported: bool) {
        if let Some(callback) = self.configuration.fuse_error_callback() {
            let error = new_fuse_operation_error(operation, errno, unsupported);
            let _ = catch_unwind(AssertUnwindSafe(|| callback(error)));
        }
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

#[derive(Debug, Default)]
struct LockTable {
    locks: Vec<FileLock>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileLock {
    inode: u64,
    owner: u64,
    start: u64,
    end: u64,
    typ: i32,
    pid: u32,
}

impl LockTable {
    fn conflicting_lock(&self, requested: FileLock) -> Option<FileLock> {
        self.locks.iter().copied().find(|existing| {
            existing.inode == requested.inode
                && existing.owner != requested.owner
                && lock_ranges_overlap(*existing, requested)
                && (existing.typ == libc::F_WRLCK as i32 || requested.typ == libc::F_WRLCK as i32)
        })
    }

    fn set_lock(&mut self, requested: FileLock) {
        self.remove_owner_range(
            requested.inode,
            requested.owner,
            requested.start,
            requested.end,
        );
        if requested.typ != libc::F_UNLCK as i32 {
            self.locks.push(requested);
        }
    }

    fn clear_owner(&mut self, owner: u64) {
        self.locks.retain(|lock| lock.owner != owner);
    }

    fn remove_owner_range(&mut self, inode: u64, owner: u64, start: u64, end: u64) {
        let mut retained = Vec::with_capacity(self.locks.len());
        for lock in self.locks.drain(..) {
            if lock.inode != inode
                || lock.owner != owner
                || !lock_ranges_overlap(
                    lock,
                    FileLock {
                        inode,
                        owner,
                        start,
                        end,
                        typ: lock.typ,
                        pid: lock.pid,
                    },
                )
            {
                retained.push(lock);
                continue;
            }
            if lock.start < start {
                retained.push(FileLock {
                    end: start.saturating_sub(1),
                    ..lock
                });
            }
            if lock.end > end {
                retained.push(FileLock {
                    start: end.saturating_add(1),
                    ..lock
                });
            }
        }
        self.locks = retained;
    }
}

fn lock_ranges_overlap(left: FileLock, right: FileLock) -> bool {
    left.start <= right.end && right.start <= left.end
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
        lock_table: Arc::new((Mutex::new(LockTable::default()), Condvar::new())),
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
        fuse_error_callback: None,
    })
}

fn with_fuse_error_callback(
    mut configuration: FilesystemConfiguration,
    callback: impl Fn(FuseOperationError) + Send + Sync + 'static,
) -> FilesystemConfiguration {
    configuration.fuse_error_callback = Some(Arc::new(callback));
    configuration
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use tempfile::TempDir;

    #[test]
    fn lock_table_conflicts_follow_advisory_range_rules() {
        let mut write_table = LockTable::default();
        let write_lock = file_lock(1, 10, 0, 10, libc::F_WRLCK as i32);
        write_table.set_lock(write_lock);

        assert_eq!(
            write_table.conflicting_lock(file_lock(1, 11, 5, 5, libc::F_RDLCK as i32)),
            Some(write_lock)
        );
        assert_eq!(
            write_table.conflicting_lock(file_lock(1, 11, 5, 5, libc::F_WRLCK as i32)),
            Some(write_lock)
        );
        assert_eq!(
            write_table.conflicting_lock(file_lock(1, 10, 5, 5, libc::F_WRLCK as i32)),
            None
        );
        assert_eq!(
            write_table.conflicting_lock(file_lock(2, 11, 5, 5, libc::F_WRLCK as i32)),
            None
        );
        assert_eq!(
            write_table.conflicting_lock(file_lock(1, 11, 11, 20, libc::F_WRLCK as i32)),
            None
        );

        let mut read_table = LockTable::default();
        let read_lock = file_lock(1, 10, 0, 10, libc::F_RDLCK as i32);
        read_table.set_lock(read_lock);
        assert_eq!(
            read_table.conflicting_lock(file_lock(1, 11, 5, 5, libc::F_RDLCK as i32)),
            None
        );
        assert_eq!(
            read_table.conflicting_lock(file_lock(1, 11, 5, 5, libc::F_WRLCK as i32)),
            Some(read_lock)
        );

        assert!(lock_ranges_overlap(
            file_lock(1, 1, 0, 10, libc::F_RDLCK as i32),
            file_lock(1, 2, 10, 20, libc::F_RDLCK as i32)
        ));
        assert!(!lock_ranges_overlap(
            file_lock(1, 1, 0, 10, libc::F_RDLCK as i32),
            file_lock(1, 2, 11, 20, libc::F_RDLCK as i32)
        ));
    }

    #[test]
    fn lock_table_replaces_splits_and_clears_owner_ranges() {
        let mut table = LockTable::default();
        table.set_lock(file_lock(1, 7, 0, 99, libc::F_WRLCK as i32));

        table.set_lock(file_lock(1, 7, 20, 29, libc::F_UNLCK as i32));

        assert_eq!(table.locks.len(), 2);
        assert!(
            table
                .locks
                .contains(&file_lock(1, 7, 0, 19, libc::F_WRLCK as i32))
        );
        assert!(
            table
                .locks
                .contains(&file_lock(1, 7, 30, 99, libc::F_WRLCK as i32))
        );

        table.set_lock(file_lock(1, 7, 0, 19, libc::F_RDLCK as i32));

        assert_eq!(table.locks.len(), 2);
        assert!(
            table
                .locks
                .contains(&file_lock(1, 7, 0, 19, libc::F_RDLCK as i32))
        );
        assert!(
            table
                .locks
                .contains(&file_lock(1, 7, 30, 99, libc::F_WRLCK as i32))
        );

        table.set_lock(file_lock(2, 8, 0, 10, libc::F_WRLCK as i32));
        table.clear_owner(7);

        assert_eq!(
            table.locks,
            vec![file_lock(2, 8, 0, 10, libc::F_WRLCK as i32)]
        );
    }

    #[test]
    fn file_lock_adapter_validates_inodes_types_and_conflicts() {
        let fixture = TestFilesystem::new("file-lock-adapter");
        let filesystem = &fixture.filesystem;

        filesystem
            .set_file_lock(
                1,
                fuser::LockOwner(1),
                0,
                10,
                libc::F_WRLCK as i32,
                100,
                false,
            )
            .expect("first lock succeeds");

        let conflict = filesystem
            .set_file_lock(
                1,
                fuser::LockOwner(2),
                5,
                5,
                libc::F_WRLCK as i32,
                200,
                false,
            )
            .expect_err("non-blocking conflicting lock is rejected");
        assert_eq!(conflict.errno().code(), fuser::Errno::EAGAIN.code());

        filesystem.clear_file_locks_for_owner(fuser::LockOwner(1));
        filesystem
            .set_file_lock(
                1,
                fuser::LockOwner(2),
                5,
                5,
                libc::F_WRLCK as i32,
                200,
                false,
            )
            .expect("lock succeeds after conflicting owner is cleared");

        let invalid_type = filesystem
            .set_file_lock(1, fuser::LockOwner(3), 0, 1, 999, 300, false)
            .expect_err("unknown lock type is rejected");
        assert_eq!(invalid_type.errno().code(), fuser::Errno::EINVAL.code());

        let missing_inode = filesystem
            .set_file_lock(
                u64::MAX,
                fuser::LockOwner(3),
                0,
                1,
                libc::F_RDLCK as i32,
                300,
                false,
            )
            .expect_err("missing inode is rejected");
        assert_eq!(missing_inode.errno().code(), fuser::Errno::ENOENT.code());
    }

    #[test]
    fn blocking_file_lock_waits_until_conflicting_owner_is_cleared() {
        let fixture = TestFilesystem::new("blocking-file-lock");
        let filesystem = fixture.filesystem.clone();

        filesystem
            .set_file_lock(
                1,
                fuser::LockOwner(1),
                0,
                10,
                libc::F_WRLCK as i32,
                100,
                false,
            )
            .expect("initial write lock succeeds");

        let waiting_filesystem = filesystem.clone();
        let (started_sender, started_receiver) = mpsc::channel();
        let waiter = thread::spawn(move || {
            started_sender.send(()).expect("waiter start is sent");
            waiting_filesystem.set_file_lock(
                1,
                fuser::LockOwner(2),
                0,
                10,
                libc::F_WRLCK as i32,
                200,
                true,
            )
        });

        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter starts");
        thread::sleep(Duration::from_millis(50));
        filesystem.clear_file_locks_for_owner(fuser::LockOwner(1));

        waiter
            .join()
            .expect("waiter thread joins")
            .expect("blocking lock succeeds after wake");
    }

    #[test]
    fn fuse_database_and_integrity_errors_map_to_io_errors() {
        assert_eq!(
            storage::FuseError::Database.errno().code(),
            fuser::Errno::EIO.code()
        );
        assert_eq!(
            storage::FuseError::Integrity.errno().code(),
            fuser::Errno::EIO.code()
        );
    }

    #[test]
    fn paged_record_iterators_stop_on_empty_pages_and_surface_loader_errors() {
        let empty_records = paged_records::<TestPage, u64, u64>(|_| {
            Ok(TestPage {
                records: Vec::new(),
                next_after: None,
            })
        })
        .collect::<Vec<_>>();
        assert!(empty_records.is_empty());

        let mut failed = false;
        let mut errors = paged_records::<TestPage, u64, u64>(move |_| {
            if failed {
                Ok(TestPage {
                    records: vec![1],
                    next_after: None,
                })
            } else {
                failed = true;
                Err(FilesystemError::Integrity)
            }
        });

        assert_eq!(errors.next(), Some(Err(FilesystemError::Integrity)));
        assert_eq!(errors.next(), None);
    }

    #[test]
    fn unmount_without_mounted_session_is_rejected() {
        let fixture = TestFilesystem::new("unmount-without-mounted-session");

        assert_eq!(
            fixture.filesystem.mark_unmounted(),
            Err(FilesystemError::FilesystemOperation)
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn rename_mode_rejects_non_empty_flags_on_non_linux_targets() {
        let error = rename_mode(fuser::RenameFlags::from_bits_retain(1))
            .expect_err("non-linux rename flags are rejected");

        assert_eq!(error.errno().code(), fuser::Errno::EINVAL.code());
    }

    struct TestFilesystem {
        _root: TempDir,
        filesystem: Filesystem,
    }

    struct TestPage {
        records: Vec<u64>,
        next_after: Option<u64>,
    }

    impl PagedRecords for TestPage {
        type Cursor = u64;
        type Record = u64;

        fn next_after(&self) -> Option<Self::Cursor> {
            self.next_after
        }

        fn into_records(self) -> Vec<Self::Record> {
            self.records
        }
    }

    impl TestFilesystem {
        fn new(name: &str) -> Self {
            let root = TempDir::new().expect("temporary directory is created");
            let configuration = FilesystemConfiguration::new(
                root.path().join(format!("{name}-database")),
                root.path().join(format!("{name}-mount")),
            )
            .expect("configuration is valid");
            let filesystem = Filesystem::open(configuration).expect("filesystem opens");
            Self {
                _root: root,
                filesystem,
            }
        }
    }

    fn file_lock(inode: u64, owner: u64, start: u64, end: u64, typ: i32) -> FileLock {
        FileLock {
            inode,
            owner,
            start,
            end,
            typ,
            pid: owner as u32,
        }
    }
}
