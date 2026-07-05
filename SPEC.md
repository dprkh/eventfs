# eventfs Specification

## Summary

- eventfs MUST provide a Rust library crate named `eventfs`.
- eventfs MUST implement an event-sourced filesystem mounted through FUSE and persisted in RocksDB.
- eventfs MUST support local incremental backup creation, local backup import, paginated event listing, branch-aware per-file event listing, persisted file snapshots, branch switching, and the mounted FUSE operations defined in this specification.
- eventfs MUST NOT expose built-in file diff rendering APIs.
- eventfs MAY include `examples/hello_world.rs` as a non-contract example that mounts the filesystem, writes and reads one file, and lists events.
- eventfs MAY include `examples/file_diff.rs` as a non-contract example that computes a diff outside the library from public snapshot and payload range APIs.
- eventfs MAY include `examples/branching.rs` as a non-contract example that creates, switches, and reads independent branches.
- eventfs MUST NOT expose public API, dependencies, storage schema, tests, or implementation code for functionality not defined in this specification.
- New eventfs functionality MUST be specified here before it is added to code.

## Public API

The public API MUST expose only these library-owned public types:

```rust
pub struct Filesystem;
pub struct FilesystemConfiguration;
pub struct MountedFilesystem;
pub struct FuseOperationError;

pub struct BackupDirectory;
pub struct BackupIdentifier;
pub struct BackupReceipt;
pub struct ImportReceipt;

pub struct EventSequence;
pub struct EventPageLimit;
pub struct EventPage;
pub struct EventRecord;
pub struct FileIdentifier;
pub struct FileSnapshot;
pub enum FileEventPayloadPart;
pub struct BranchName;
pub struct BranchIdentifier;
pub enum BranchStatus;
pub struct BranchRecord;
pub struct BranchPosition;
pub struct BranchPageLimit;
pub struct BranchPage;
pub struct BranchEventPage;
pub enum EventKind;

pub enum ConfigurationError;
pub enum FilesystemError;
```

The public API MUST expose these methods:

```rust
impl MountedFilesystem {
    pub fn unmount(self) -> Result<(), FilesystemError>;
}

impl FilesystemConfiguration {
    pub fn new(
        database_directory: impl Into<std::path::PathBuf>,
        mount_point: impl Into<std::path::PathBuf>,
    ) -> Result<Self, ConfigurationError>;

    pub fn with_fuse_error_callback(
        self,
        callback: impl Fn(FuseOperationError) + Send + Sync + 'static,
    ) -> Self;
}

impl FuseOperationError {
    pub fn operation(&self) -> &'static str;
    pub fn errno(&self) -> i32;
    pub fn filesystem_error(&self) -> FilesystemError;
    pub fn is_unsupported(&self) -> bool;
}

impl BackupDirectory {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Result<Self, ConfigurationError>;
    pub fn as_path(&self) -> &std::path::Path;
}

impl BackupIdentifier {
    pub fn new(value: u32) -> Result<Self, ConfigurationError>;
    pub fn get(self) -> u32;
}

impl BackupReceipt {
    pub fn backup_identifier(&self) -> BackupIdentifier;
    pub fn source_event_sequence(&self) -> EventSequence;
}

impl ImportReceipt {
    pub fn backup_identifier(&self) -> BackupIdentifier;
    pub fn imported_event_sequence(&self) -> EventSequence;
}

impl EventSequence {
    pub const fn new(value: u64) -> Self;
    pub fn get(self) -> u64;
}

impl EventPageLimit {
    pub fn new(value: u64) -> Result<Self, ConfigurationError>;
    pub fn get(self) -> u64;
}

impl EventPage {
    pub fn records(&self) -> &[EventRecord];
    pub fn into_records(self) -> Vec<EventRecord>;
    pub fn next_after(&self) -> Option<EventSequence>;
}

impl EventRecord {
    pub fn sequence(&self) -> EventSequence;
    pub fn kind(&self) -> EventKind;
    pub fn created_at(&self) -> time::UtcDateTime;
    pub fn file_identifier(&self) -> Option<FileIdentifier>;
    pub fn secondary_file_identifier(&self) -> Option<FileIdentifier>;
    pub fn branch_identifier(&self) -> Option<BranchIdentifier>;
    pub fn branch_position(&self) -> Option<BranchPosition>;
    pub fn first_parent_sequence(&self) -> Option<EventSequence>;
    pub fn path(&self) -> Option<&str>;
    pub fn secondary_path(&self) -> Option<&str>;
    pub fn offset(&self) -> Option<u64>;
    pub fn byte_length(&self) -> Option<u64>;
    pub fn old_file_size(&self) -> Option<u64>;
    pub fn new_file_size(&self) -> Option<u64>;
    pub fn overwritten_byte_length(&self) -> Option<u64>;
    pub fn written_byte_length(&self) -> Option<u64>;
    pub fn removed_byte_length(&self) -> Option<u64>;
}

impl FileIdentifier {
    pub fn new(value: u64) -> Self;
    pub fn get(self) -> u64;
}

impl FileSnapshot {
    pub fn file_identifier(&self) -> FileIdentifier;
    pub fn sequence(&self) -> EventSequence;
    pub fn branch_position(&self) -> BranchPosition;
    pub fn file_size(&self) -> u64;
}

impl BranchName {
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigurationError>;
    pub fn as_str(&self) -> &str;
}

impl BranchIdentifier {
    pub fn new(value: u64) -> Self;
    pub fn get(self) -> u64;
}

impl BranchRecord {
    pub fn branch_identifier(&self) -> BranchIdentifier;
    pub fn name(&self) -> &BranchName;
    pub fn status(&self) -> BranchStatus;
    pub fn head_position(&self) -> BranchPosition;
    pub fn head_sequence(&self) -> EventSequence;
}

impl BranchPosition {
    pub fn branch_identifier(&self) -> BranchIdentifier;
    pub fn ordinal(&self) -> u64;
}

impl BranchPageLimit {
    pub fn new(value: u64) -> Result<Self, ConfigurationError>;
    pub fn get(self) -> u64;
}

impl BranchPage {
    pub fn records(&self) -> &[BranchRecord];
    pub fn into_records(self) -> Vec<BranchRecord>;
    pub fn next_after(&self) -> Option<BranchIdentifier>;
}

impl BranchEventPage {
    pub fn records(&self) -> &[EventRecord];
    pub fn into_records(self) -> Vec<EventRecord>;
    pub fn next_after(&self) -> Option<BranchPosition>;
}

impl Filesystem {
    pub fn open(configuration: FilesystemConfiguration) -> Result<Self, FilesystemError>;
    pub fn mount(&self) -> Result<(), FilesystemError>;
    pub fn spawn_mount(&self) -> Result<MountedFilesystem, FilesystemError>;

    pub fn create_backup(
        &self,
        backup_directory: BackupDirectory,
    ) -> Result<BackupReceipt, FilesystemError>;

    pub fn import_backup(
        database_directory: impl Into<std::path::PathBuf>,
        backup_directory: BackupDirectory,
        backup_identifier: BackupIdentifier,
    ) -> Result<ImportReceipt, FilesystemError>;

    pub fn events(
        &self,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_;

    pub fn list_events(
        &self,
        after: Option<EventSequence>,
        limit: EventPageLimit,
    ) -> Result<EventPage, FilesystemError>;

    pub fn get_event(
        &self,
        sequence: EventSequence,
    ) -> Result<Option<EventRecord>, FilesystemError>;

    pub fn current_branch(&self) -> Result<BranchRecord, FilesystemError>;

    pub fn branches(
        &self,
        limit: BranchPageLimit,
    ) -> impl Iterator<Item = Result<BranchRecord, FilesystemError>> + '_;

    pub fn list_branches(
        &self,
        after: Option<BranchIdentifier>,
        limit: BranchPageLimit,
    ) -> Result<BranchPage, FilesystemError>;

    pub fn create_branch(
        &self,
        name: &BranchName,
        from: BranchPosition,
    ) -> Result<BranchRecord, FilesystemError>;

    pub fn switch_branch(&self, name: &BranchName) -> Result<BranchRecord, FilesystemError>;

    pub fn delete_branch(&self, name: &BranchName) -> Result<(), FilesystemError>;

    pub fn branch_events(
        &self,
        branch: BranchIdentifier,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_;

    pub fn list_branch_events(
        &self,
        branch: BranchIdentifier,
        after: Option<BranchPosition>,
        limit: EventPageLimit,
    ) -> Result<BranchEventPage, FilesystemError>;

    pub fn branch_file_events(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_;

    pub fn list_branch_file_events(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        after: Option<BranchPosition>,
        limit: EventPageLimit,
    ) -> Result<BranchEventPage, FilesystemError>;

    pub fn file_snapshot_on_branch_at_or_before(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        position: BranchPosition,
    ) -> Result<Option<FileSnapshot>, FilesystemError>;

    pub fn read_file_snapshot_range(
        &self,
        snapshot: &FileSnapshot,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError>;

    pub fn read_file_event_payload_range(
        &self,
        sequence: EventSequence,
        part: FileEventPayloadPart,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError>;

    pub fn file_events(
        &self,
        file_identifier: FileIdentifier,
        limit: EventPageLimit,
    ) -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_;

    pub fn list_file_events(
        &self,
        file_identifier: FileIdentifier,
        after: Option<EventSequence>,
        limit: EventPageLimit,
    ) -> Result<EventPage, FilesystemError>;

    pub fn file_snapshot_at_or_before(
        &self,
        file_identifier: FileIdentifier,
        sequence: EventSequence,
    ) -> Result<Option<FileSnapshot>, FilesystemError>;
}
```

- New public API functionality MUST be specified in this section before it is added to code.
- Public APIs MUST provide one obvious common-path call shape.
- Caller-authored path and text values MUST accept standard convertible inputs when validation remains explicit.
- Public methods MUST borrow caller-owned values when ownership is not required.
- Validated scalar wrapper types MUST expose `new` and `get` and MUST NOT expose duplicate public construction paths.
- Paginated APIs MUST remain available for explicit cursor control when pagination is part of the contract.
- Public iterator APIs MUST provide the common sequential read path for paginated records.
- README and examples MUST demonstrate the intended common-path API for changed public functionality.
- `FilesystemConfiguration::new` and path-valued public constructors MUST reject empty paths.
- `FilesystemConfiguration::with_fuse_error_callback` MUST configure one optional callback shared by filesystem clones and mounted sessions opened from the configuration.
- `FuseOperationError` MUST expose the failed FUSE operation name, the positive platform errno returned to FUSE, the mapped `FilesystemError`, and whether the operation was unsupported.
- `BackupIdentifier` MUST reject zero.
- `EventPageLimit` MUST reject zero.
- `BranchPageLimit` MUST reject zero.
- `BranchName` MUST reject empty names.
- `EventRecord` MUST expose event sequence, event kind, UTC creation time, optional affected file identifier, and optional event path.
- `EventRecord` MUST expose an optional secondary affected file identifier and secondary path for single-event operations that mutate two regular files.
- `EventRecord` MUST expose branch identifier, branch position, and first-parent event sequence when the event belongs to a branch.
- `EventRecord` MUST expose file write and truncate payload byte lengths, not payload bytes.
- `FileSnapshot` MUST expose the file identifier, source event sequence, branch position, and file size.
- Public API MUST NOT expose RocksDB-owned public types.
- Branch APIs MUST use `BranchPosition`, not `EventSequence`, for branch-local cursors.
- File snapshot and event payload bytes MUST be read through explicit range APIs.
- Event creation times MUST be represented as `time::UtcDateTime`, not text.
- Every library-owned public API item, including public methods and enum variants, MUST have concise rustdoc documenting observable behavior and errors without exposing private storage details.

## FUSE Behavior

- `Filesystem` MUST implement `fuser::Filesystem`.
- `Filesystem` MUST implement `Clone`, `Send`, and `Sync`.
- `Filesystem::mount` and `Filesystem::spawn_mount` MUST mount the filesystem at `FilesystemConfiguration.mount_point`.
- The mounted filesystem MUST support lookup, attribute read, attribute update, node creation, directory creation, file creation, file open, file read, file write, file truncate, flush, file synchronization, directory open, directory read, directory read with attributes, directory synchronization, file release, directory release, unlink, directory removal, rename, hard link, symbolic link, symbolic link read, access check, filesystem statistics, extended attributes, POSIX byte-range locks, block mapping, ioctl rejection, poll readiness, space allocation, sparse seek, file-range copy, macOS volume rename, macOS file-content exchange, and macOS extended times.
- Unsupported FUSE operations MUST return the platform-appropriate unsupported-operation error and MUST NOT append events.
- Failed supported FUSE operations MUST invoke the configured FUSE error callback once with `FuseOperationError::is_unsupported` returning `false`.
- Unsupported FUSE operations MUST invoke the configured FUSE error callback once with `FuseOperationError::is_unsupported` returning `true`.
- Successful FUSE operations MUST NOT invoke the configured FUSE error callback.
- FUSE error callback failures MUST NOT change the FUSE error returned to the caller.
- Inode numbers MUST be stable across process restarts.
- File handles MAY be process-local and MUST NOT be required to reconstruct persistent filesystem state.
- Filesystem statistics MUST NOT append events or mutate storage.
- Filesystem statistics MUST report `blocks`, `bfree`, `bavail`, and `frsize` from `statvfs` on `FilesystemConfiguration.database_directory`.
- Filesystem statistics MUST report `files` and `ffree` from eventfs logical inode-number capacity.
- Filesystem statistics MUST report `bsize` as the eventfs preferred file I/O block size.
- Filesystem statistics MUST report `namelen` as 255 bytes.
- FUSE name components longer than 255 bytes MUST be rejected.
- Extended attributes MUST be inode-scoped, branch-local, opaque byte values keyed by non-empty names no longer than 255 bytes.
- `setxattr` MUST honor create and replace flags, reject unsupported position values, and append an extended-attribute-set event on success.
- `getxattr` and `listxattr` MUST honor size probes and MUST return `ERANGE` when a non-zero output size is too small.
- Missing extended attributes MUST return the platform missing-attribute errno.
- `removexattr` MUST append an extended-attribute-removed event on success.
- `readdirplus` MUST return the same entries as `readdir` with stable attributes from one storage snapshot.
- `getlk` and `setlk` MUST implement process-local advisory byte-range locks, including blocking waits when requested, and MUST NOT append events.
- `flush` and `release` MUST clear byte-range locks held by the released lock owner.
- `bmap` MUST return the requested logical block index after inode validation and MUST NOT append events.
- `ioctl` MUST expose no eventfs-specific commands, MUST return `ENOTTY` for every command after inode validation, and MUST NOT append events.
- `poll` MUST report regular files and directories as immediately readable and writable according to requested events and access checks, and MUST NOT append events.
- `fallocate` MUST support preallocation by size extension, keep-size no-op allocation, punch-hole, and zero-range.
- `fallocate` collapse-range, insert-range, unshare-range, and unknown mode bits MUST return `EINVAL`.
- `fallocate` requests with no logical file state change MUST NOT append events.
- `lseek` MUST support `SEEK_DATA` and `SEEK_HOLE` using eventfs sparse extents and MUST NOT append events.
- `copy_file_range` MUST reject non-empty flags and MUST append one destination file write event for copied bytes.
- macOS `setvolname` MUST persist the volume name and append one volume-renamed event.
- macOS `exchange` MUST atomically swap regular-file contents and append one file-contents-exchanged event with primary and secondary file identifiers and paths.
- macOS `getxtimes` MUST return stored backup and creation times and MUST NOT append events.

## Event Semantics

- Every mutating supported FUSE operation that changes logical filesystem state MUST create exactly one durable event.
- Supported FUSE operations that do not change logical filesystem state MUST NOT append events.
- Events MUST be assigned strictly increasing `u64` event sequences.
- Event sequence assignment, event append, branch head update, branch event index updates, per-file event index updates, current extent updates, event payload extent updates, file snapshot extent updates, extended attribute updates, and filesystem metadata updates created for the event MUST commit in one RocksDB write batch.
- Committed event keys MUST NOT be overwritten or deleted.
- Every event MUST store schema version, sequence, kind, UTC creation time, affected file identifier when applicable, secondary affected file identifier when applicable, path when applicable, secondary path when applicable, and byte range when applicable.
- Branch events MUST store branch identifier, branch position, and first-parent event sequence.
- File write events MUST store old file size, new file size, overwritten byte length, and written byte length in the durable event record.
- File write events MUST store overwritten bytes and written bytes as durable event payload extents outside event metadata.
- File truncate events MUST store old file size, new file size, and removed byte length in the durable event record.
- File truncate events MUST store removed bytes for shrink operations as durable event payload extents outside event metadata.
- Zero-filled file extension MUST be represented by old and new file sizes, not repeated zero bytes.
- `EventKind` MUST include filesystem initialization, node creation, directory creation, file creation, file write, file truncate, metadata change, node unlink, directory removal, node rename, hard link creation, symbolic link creation, extended attribute set, extended attribute removal, file range zeroing, file contents exchange, and volume rename.
- Extended attribute set and removal, file range zeroing, file contents exchange, volume rename, file-range copy destination writes, and size-growing allocation MUST append events when they change logical filesystem state.
- Read-only operations, locks, poll, bmap, ioctl failures, and allocation requests with no logical state change MUST NOT append events.
- `get_event` MUST return the event at the requested sequence when it exists.
- `current_branch` MUST return the currently active branch.
- `list_branches` MUST return branches ordered by branch identifier with pagination.
- `create_branch` MUST create a uniquely named branch from an existing branch position.
- `create_branch` MUST materialize the new branch's file contents from snapshots at the requested branch position.
- `switch_branch` MUST change the active branch only when the filesystem is not mounted and MUST NOT append an event.
- `delete_branch` MUST delete only the branch name/ref and MUST NOT delete committed events or content chunks.
- `delete_branch` MUST reject the active branch and the initial branch.
- `read_file_event_payload_range` MUST return the requested payload byte range and MUST return an empty vector for bytes beyond the payload length.
- `list_events` MUST return events with sequences greater than `after` when `after` is present.
- `list_events` MUST return at most `limit` records and MUST expose the next cursor only when another page exists.
- `list_branch_events` MUST return events in branch-position order for the requested branch.
- `list_branch_file_events` MUST return only events for the requested branch and file identifier in branch-position order.
- `list_file_events` MUST return events for the active branch and requested file identifier in branch-position order.
- `file_snapshot_on_branch_at_or_before` MUST return the newest persisted snapshot for the requested branch and file whose branch position is less than or equal to the requested position.
- `file_snapshot_at_or_before` MUST return the newest persisted snapshot for the active branch and requested file whose event sequence is less than or equal to the requested sequence.
- `read_file_snapshot_range` MUST return the requested snapshot byte range and MUST return an empty vector for bytes beyond the snapshot file size.
- Event listing MUST scan event indexes and MUST NOT replay stored file chunks.
- Event record debug formatting MUST NOT expose stored file payload bytes.
- File snapshots MUST be maintained as derived extent manifests from committed file events and stored outside event metadata.
- File snapshots MUST include branch fork snapshots for regular files present at the requested branch position.
- File snapshots MUST bound the number of file content events a caller must replay after the returned snapshot to reconstruct a file at a later event sequence.
- The initial branch MUST be named `main`.

## Storage

The RocksDB database MUST contain these column families:

- `events`
- `inodes`
- `directory_entries`
- `extended_attributes`
- `filesystem_metadata`
- `file_events`
- `branches`
- `branch_names`
- `branch_events`
- `branch_file_events`
- `content_chunks`
- `current_file_extents`
- `file_snapshot_extents`
- `event_payload_extents`

Storage requirements:

- `events` keys MUST be big-endian event sequences.
- `inodes` keys MUST be ordered by branch identifier and inode number.
- `directory_entries` keys MUST be ordered by branch identifier, parent inode number, and name.
- `extended_attributes` keys MUST be ordered by branch identifier, inode number, and attribute name.
- `file_events` keys MUST be ordered by file identifier and event sequence.
- `branch_events` keys MUST be ordered by branch identifier and branch position.
- `branch_file_events` keys MUST be ordered by branch identifier, file identifier, and branch position.
- `content_chunks` keys MUST be cryptographic content identifiers.
- `current_file_extents` keys MUST be ordered by branch identifier, file identifier, and logical byte offset.
- `file_snapshot_extents` keys MUST be ordered by branch identifier, file identifier, branch position, and logical byte offset.
- `event_payload_extents` keys MUST be ordered by event sequence, payload part, and logical byte offset.
- `content_chunks` MUST use RocksDB integrated BlobDB.
- Content chunks MUST be immutable whole BlobDB values.
- File state, file snapshots, and event payloads MUST be represented by extent manifests that reference `content_chunks`.
- Content chunks MUST be created with FastCDC v2020 using minimum 16 KiB, target 64 KiB, maximum 256 KiB, and seed 0.
- Content chunk identifiers MUST be cryptographic digests of chunk bytes and MUST NOT use FastCDC rolling fingerprints.
- eventfs MUST use RocksDB pinned reads for durable byte content whenever the API or internal operation can borrow the value.
- eventfs MUST NOT materialize owned byte buffers for durable byte content unless mutation, FUSE output, serialization, or caller-owned copies require it.
- `filesystem_metadata` MUST store storage schema version, next inode number, last committed event sequence, next branch identifier, active branch identifier, and volume name.
- Stored inodes MUST include backup time and creation time.
- `Filesystem::open` MUST create a missing database and required column families for a new database directory.
- `Filesystem::open` MUST reject existing databases missing required current metadata values.
- After the first release, `Filesystem::open` MUST create required current column families for compatible released databases.
- The first released eventfs version MUST establish the first released storage schema compatibility baseline.
- Pre-release storage schemas MAY be rejected or replaced until the first release.
- Released storage schema changes MUST preserve compatibility with all earlier released storage schemas.
- `Filesystem::open` MUST reject storage schema versions newer than the compiled current storage schema version.
- Reads required by FUSE MUST use materialized current filesystem state, not event-log replay.
- Mutating filesystem operations MUST use synchronous RocksDB write batches.

## Local Backup And Import

- Local backup and import MUST always be compiled.
- Local backup and import MUST use RocksDB `BackupEngine`.
- `BackupDirectory` MUST identify a local RocksDB BackupEngine repository directory.
- Backup, import target, and source database directories MUST NOT overlap after path normalization.
- `create_backup` MUST open or create the specified backup directory as a persistent BackupEngine repository.
- `create_backup` MUST create a new incremental BackupEngine backup with memtable flush enabled.
- Repeated `create_backup` calls with the same `BackupDirectory` MUST preserve existing BackupEngine repository contents.
- `create_backup` MUST verify the newly created BackupEngine backup before returning `BackupReceipt`.
- `BackupReceipt` MUST expose the created BackupEngine backup identifier and the source event sequence.
- `import_backup` MUST verify the requested BackupEngine backup identifier before restoring.
- `import_backup` MUST reject an empty target database directory as `FilesystemError::Import`.
- `import_backup` MUST restore the requested backup into a temporary directory.
- `import_backup` MUST open the restored RocksDB database successfully before replacing the target database directory.
- `import_backup` MUST discard existing data in the target database directory before moving the verified restored database into place.
- eventfs MUST NOT perform remote object storage synchronization.

## Operations

- `FilesystemConfiguration.database_directory` MUST identify the durable RocksDB data directory.
- `FilesystemConfiguration.mount_point` MUST identify only the FUSE presentation mount point.
- `BackupDirectory` MUST identify the local BackupEngine repository used for backup retention.
- Backup rotation MUST be performed by retaining, archiving, or replacing whole `BackupDirectory` repositories outside active `create_backup` and `import_backup` calls.
- eventfs MUST NOT expose APIs for deleting individual BackupEngine backups.
- Recovery MUST use `Filesystem::import_backup` to restore a verified backup into a target database directory.
- Recovery MUST open the restored RocksDB database successfully before replacing the target database directory.
- Downgrade to an older eventfs version MUST use a backup whose storage schema is compatible with that older version.
- Opening a database with a newer storage schema MUST fail before mutation.
- Repository operational documentation MUST describe data location, backup rotation, recovery, and upgrade/downgrade behavior.

## Errors

- Public methods MUST return `FilesystemError`.
- Configuration constructor failures MUST return `ConfigurationError`.
- RocksDB failures MUST map to `FilesystemError::Database`.
- Corrupt storage, corrupt backup repositories, missing backup files, and unknown backup identifiers MUST map to `FilesystemError::Integrity`.
- FUSE operation failures MUST map to `FilesystemError::FilesystemOperation`.
- `FuseOperationError::filesystem_error` MUST return `FilesystemError::FilesystemOperation`.
- Non-FUSE public API failures MUST NOT invoke the configured FUSE error callback.
- Backup repository creation, backup repository opening, and backup creation failures MUST map to `FilesystemError::Backup`.
- Import target replacement and import-open failures MUST map to `FilesystemError::Import`.

## Acceptance Tests

Acceptance test requirements:

- Tests MUST NOT assert the complete public API inventory.
- End-to-end tests MUST cover only user-facing flows.
- End-to-end tests MUST cover every new and existing code path reachable through public library APIs, mounted FUSE operations, or documented operational workflows.
- End-to-end tests MUST NOT assert private dependencies, private storage encodings, or other internal implementation details.
- Internal behavior MUST be tested below end-to-end boundaries.
- Longer FUSE stress, crash/fault-injection, and concurrency/load tests MUST use bounded deterministic workloads in the default test suite.

Implementations MUST include automated tests for:

- Invalid configuration values are rejected by public constructors.
- Event listing returns paginated events with UTC creation times.
- Event, branch, branch event, branch file event, and active branch file event iterators return the same ordered records as explicit pagination.
- Local backup creates increasing non-zero BackupEngine backup identifiers in a persistent backup directory.
- Local import verifies a requested BackupEngine backup, replaces existing target data, and opens the imported database before success.
- Local backup and import reject overlapping source, backup, and target directories after path normalization.
- Event records expose file write and truncate payload sizes without payload bytes.
- File event payload range reads expose file write and truncate payload bytes.
- `get_event` returns the requested event or no event.
- Per-file event listing returns only events for the requested active branch file in branch-position order.
- Branch event listing returns only events for the requested branch in branch-position order.
- File snapshots return the nearest snapshot at or before a requested sequence.
- File snapshots and file event payloads can reconstruct file bytes before and after a write or truncate event.
- Snapshot and event payload content is read through range APIs backed by content chunk reads.
- Content chunks are deduplicated by content identifier.
- Cross-chunk reads, overwrites, truncates, and sparse zero-filled ranges return correct bytes.
- Branch creation from a branch position creates an independent branch.
- Branch switching changes the mounted filesystem's future active state only when unmounted.
- Branch deletion removes the branch ref without deleting committed events or content chunks.
- Branch switching, divergence, file event listing, and snapshot reads preserve independent file contents across branches.
- Event debug formatting does not expose stored file payload bytes.
- Every supported FUSE operation works through the mounted filesystem.
- Extended attributes, copy-range, allocation, sparse seek, locks, poll, bmap, ioctl failure, macOS volume rename, macOS file exchange, and macOS extended times work through the mounted filesystem where supported by the target operating system.
- Mutating supported FUSE operations append exactly the specified event kinds, and supported read-only or no-op operations append none.
- Configured FUSE error callbacks receive failed supported operation errors and unsupported operation errors with the returned errno.
- Successful FUSE operations do not invoke configured FUSE error callbacks.
- Mounted FUSE operation tests cover success cases, failure cases, edge cases, event append behavior for mutating operations, and no-event behavior for read-only and unsupported operations.
- Longer mounted FUSE stress tests cover repeated supported operations, combinations of supported operations, and edge cases.
- Crash and fault injection around write batches, backup creation, backup import replacement, and mount/unmount state transitions preserves durable consistency.
- Concurrency and load tests cover branch switching, branch deletion, mounted writes, and event, branch, and file listing.
- Filesystem statistics report backing block capacity, eventfs inode capacity, and do not append events.
