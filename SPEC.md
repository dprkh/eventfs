# eventfs Specification

## Summary

- eventfs MUST provide a Rust library crate named `eventfs`.
- eventfs MUST implement an event-sourced filesystem mounted through FUSE and persisted in RocksDB.
- eventfs MUST compile only for Linux targets, and non-Linux targets MUST fail with an explicit Linux-only compile error.
- eventfs MUST support local incremental backup creation, local backup import, paginated event listing, branch-aware per-file event listing, persisted file snapshots, branch switching, and the mounted FUSE operations defined in this specification.
- eventfs MUST include a Linux-only Criterion benchmark comparing supported mounted FUSE operation speed against the current host's normal filesystem.
- eventfs MUST NOT expose built-in file diff rendering APIs.
- The eventfs library target source MUST compile with Rust `unsafe_code` forbidden.
- eventfs MAY include these non-contract examples: `examples/hello_world.rs` mounts the filesystem, writes and reads one file, and lists events; `examples/file_diff.rs` computes a diff outside the library from public snapshot and payload range APIs; and `examples/branching.rs` creates, switches, and reads independent branches.
- eventfs MUST NOT expose public API, dependencies, storage schema, tests, or implementation code for functionality not defined in this specification.
- eventfs MUST NOT specify, expose, implement, test, or document non-Linux behavior unless that behavior is required for Linux support.
- New eventfs functionality MUST be specified here before it is added to code.

## Public API

The public API MUST expose only these library-owned public types:

```rust
pub struct Filesystem;
pub struct FilesystemConfiguration;
pub struct MountedFilesystem;
pub struct FuseOperationError;
pub enum MountOption;

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
    pub fn new(database_directory: impl Into<std::path::PathBuf>, mount_point: impl Into<std::path::PathBuf>)
        -> Result<Self, ConfigurationError>;
    pub fn with_mount_options(self, mount_options: impl IntoIterator<Item = MountOption>) -> Self;
    pub fn with_fuse_error_callback(self, callback: impl Fn(FuseOperationError) + Send + Sync + 'static)
        -> Self;
}

impl FuseOperationError {
    pub fn operation(&self) -> &'static str;
    pub fn errno(&self) -> i32;
    pub fn filesystem_error(&self) -> FilesystemError;
    pub fn is_unsupported(&self) -> bool;
}

pub enum MountOption {
    FilesystemName(String),
    Subtype(String),
    Custom(String),
    AutoUnmount,
    ReadOnly,
    ReadWrite,
    Atime,
    NoAtime,
    DirSync,
    Sync,
    Async,
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
    pub fn branch_identifier(&self) -> Option<BranchIdentifier>;
    pub fn branch_position(&self) -> Option<BranchPosition>;
    pub fn first_parent_sequence(&self) -> Option<EventSequence>;
    pub fn path(&self) -> Option<&str>;
    pub fn offset(&self) -> Option<u64>;
    pub fn byte_length(&self) -> Option<u64>;
    pub fn old_file_size(&self) -> Option<u64>;
    pub fn new_file_size(&self) -> Option<u64>;
    pub fn overwritten_byte_length(&self) -> Option<u64>;
    pub fn written_byte_length(&self) -> Option<u64>;
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
    pub fn create_backup(&self, backup_directory: BackupDirectory)
        -> Result<BackupReceipt, FilesystemError>;
    pub fn import_backup(
        database_directory: impl Into<std::path::PathBuf>,
        backup_directory: BackupDirectory,
        backup_identifier: BackupIdentifier,
    ) -> Result<ImportReceipt, FilesystemError>;
    pub fn events(&self, limit: EventPageLimit)
        -> impl Iterator<Item = Result<EventRecord, FilesystemError>> + '_;
    pub fn list_events(&self, after: Option<EventSequence>, limit: EventPageLimit)
        -> Result<EventPage, FilesystemError>;
    pub fn get_event(&self, sequence: EventSequence)
        -> Result<Option<EventRecord>, FilesystemError>;
    pub fn current_branch(&self) -> Result<BranchRecord, FilesystemError>;
    pub fn branches(&self, limit: BranchPageLimit)
        -> impl Iterator<Item = Result<BranchRecord, FilesystemError>> + '_;
    pub fn list_branches(&self, after: Option<BranchIdentifier>, limit: BranchPageLimit)
        -> Result<BranchPage, FilesystemError>;
    pub fn create_branch(&self, name: &BranchName, from: BranchPosition)
        -> Result<BranchRecord, FilesystemError>;
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
- README and examples MUST demonstrate the default common-path API unless the example's capability requires optional configuration.
- `FilesystemConfiguration::new` and path-valued public constructors MUST reject empty paths.
- `FilesystemConfiguration::new` MUST default to no caller-supplied mount options.
- `FilesystemConfiguration::with_mount_options` MUST replace the caller-supplied mount options shared by filesystem clones and mounted sessions opened from the configuration.
- `FilesystemConfiguration::with_fuse_error_callback` MUST configure one optional callback shared by filesystem clones and mounted sessions opened from the configuration.
- `FuseOperationError` MUST expose the failed FUSE operation name, the positive platform errno returned to FUSE, the mapped `FilesystemError`, and whether the operation was unsupported.
- `MountOption` MUST expose FUSE filesystem name, subtype, custom string option, automatic unmount, read-only/read-write mode, access-time handling, directory synchronization, and synchronous/asynchronous I/O mount options without exposing fuser-owned public types.
- `BackupIdentifier`, `EventPageLimit`, and `BranchPageLimit` MUST reject zero; `BranchName` MUST reject empty names.
- `EventRecord` MUST expose event sequence, kind, UTC creation time, optional affected file identifier and path, branch identifier, branch position, and first-parent event sequence when the event belongs to a branch, and file write payload byte lengths but not payload bytes.
- `FileSnapshot` MUST expose the file identifier, source event sequence, branch position, and file size.
- Public API MUST NOT expose fuser-owned or RocksDB-owned public types.
- Branch APIs MUST use `BranchPosition`, not `EventSequence`, for branch-local cursors.
- File snapshot and event payload bytes MUST be read through explicit range APIs.
- Event creation times MUST be represented as `time::UtcDateTime`, not text.
- Every library-owned public API item, including public methods and enum variants, MUST have concise rustdoc documenting observable behavior and errors without exposing private storage details.

## FUSE Behavior

- `Filesystem` MUST implement `fuser::Filesystem`, `Clone`, `Send`, and `Sync`.
- `Filesystem::mount` and `Filesystem::spawn_mount` MUST mount at `FilesystemConfiguration.mount_point`, apply caller-supplied mount options, and use unrestricted FUSE session access control.
- When the caller omits `MountOption::FilesystemName`, eventfs MUST supply it from the persisted volume name.
- The mounted filesystem MUST support lookup, attribute read, metadata changes, file size changes, node creation, directory creation, file creation, file open, file read, file write, flush, file synchronization, directory open, directory read, directory read with attributes, directory synchronization, file release, directory release, unlink, directory removal, rename, hard link, symbolic link, symbolic link read, access check, filesystem statistics, and extended attributes.
- The mounted filesystem MUST NOT support POSIX byte-range locks, block mapping, ioctl, poll, space allocation, sparse seek, or file-range copy; specifically, `getlk`, `setlk`, `bmap`, `ioctl`, `poll`, `fallocate`, `lseek`, and `copy_file_range` MUST be unsupported. Every unsupported FUSE operation MUST return the platform-appropriate unsupported-operation error, append no events, and invoke the configured FUSE error callback once with `FuseOperationError::is_unsupported` returning `true`.
- With the `tracing` Cargo feature enabled, every eventfs-handled FUSE operation MUST emit one `tracing::trace!` event containing its name; this logging MUST NOT append events, change FUSE replies or errno mapping, or affect the configured FUSE error callback.
- Each failed supported FUSE operation MUST invoke the configured FUSE error callback once with `FuseOperationError::is_unsupported` returning `false`; successful operations MUST NOT invoke it. FUSE error callback failures MUST NOT change the FUSE error returned to the caller.
- eventfs MUST always enable kernel default permission checking when mounting and MUST NOT expose default permission checking as a public mount option.
- `setattr` MUST support `mode`, `uid`, `gid`, `size`, `atime`, and `mtime`; reject every other fuser-provided field as unsupported without appending events; append a metadata-changed event for supported metadata-only changes; and append no event for no-op metadata changes.
- `setattr` size changes MUST apply only to regular files, discard trailing bytes when shrinking, and make extended ranges read as zero bytes when growing.
- File opens with `O_TRUNC` MUST truncate existing regular files to zero bytes before returning a successful open handle; file creates MUST return zero-size regular files; and writes through `O_APPEND` handles MUST append at the current file size.
- Inode numbers MUST be stable across process restarts.
- File handles MAY be process-local and MUST NOT be required to reconstruct persistent filesystem state.
- Filesystem statistics MUST NOT append events or mutate storage and MUST report: `blocks`, `bfree`, `bavail`, and `frsize` from `statvfs` on `FilesystemConfiguration.database_directory`; `files` and `ffree` from eventfs logical inode-number capacity; `bsize` as the eventfs preferred file I/O block size; and `namelen` as 255 bytes.
- FUSE name components longer than 255 bytes MUST be rejected.
- Extended attributes MUST be inode-scoped, branch-local, opaque byte values keyed by `user.*` names.
- Extended attribute names MUST be valid UTF-8, MUST NOT contain NUL bytes, MUST be no longer than 255 bytes, and MUST be supported only on regular files and directories.
- Extended attribute values MUST be no larger than 65536 bytes, and an extended attribute list response MUST be no larger than 65536 bytes.
- `setxattr` MUST honor create and replace flags, reject unsupported position values, and append an extended-attribute-set event on success.
- `getxattr` and `listxattr` MUST honor size probes and MUST return `ERANGE` when a non-zero output size is too small.
- Missing extended attributes MUST return the platform missing-attribute errno.
- `removexattr` MUST append an extended-attribute-removed event on success.
- `readdirplus` MUST return the same entries as `readdir` with stable attributes from one storage snapshot.
- `flush` and `release` MUST NOT force storage synchronization.
- `fsync` and `fsyncdir` MUST synchronize all previously committed mounted filesystem mutations to persistent storage before returning success and MAY synchronize committed mutations outside the requested inode or directory.
- File writes opened with synchronized I/O flags and mounted operations covered by `MountOption::Sync` or `MountOption::DirSync` MUST synchronize affected committed mutations before returning success.
- `access` MUST validate that the inode exists and MUST apply stored ownership and permission mode when the kernel routes an access request to eventfs.

## Event Semantics

- Every mutating supported FUSE operation that changes logical filesystem state MUST create exactly one committed event before returning success; supported operations that do not change logical filesystem state, read-only operations, and unsupported operations MUST NOT append events.
- Events MUST be assigned strictly increasing `u64` event sequences.
- Event sequence assignment, event append, branch head update, branch root update, branch event index updates, per-file event index updates, namespace root update, content manifest root updates, event payload manifest updates, file snapshot manifest updates, and filesystem metadata updates created for the event MUST commit in one RocksDB write batch.
- Committed events MUST be immediately visible through mounted reads, event listing, branch state, file snapshots, and event payload range reads in the same eventfs process.
- Committed mounted filesystem mutations MUST NOT be required to survive system crash or reboot until a successful filesystem synchronization operation covers them.
- Committed event keys MUST NOT be overwritten or deleted.
- Every event MUST store schema version, sequence, kind, UTC creation time, and applicable affected file identifier, path, and byte range; branch events MUST also store branch identifier, branch position, and first-parent event sequence.
- File write event records MUST store old and new file sizes plus overwritten and written byte lengths; their overwritten and written bytes MUST be event payload manifests outside event metadata.
- Zero-filled file extension MUST be represented by old and new file sizes, not repeated zero bytes.
- Successful file truncation and file extension through mounted operations MUST append one file write event when the file size changes.
- File truncation events MUST store discarded bytes as overwritten payload bytes, no written payload bytes, old and new file sizes, the new file size as the event offset, and zero byte length; file extension events MUST store no payload bytes, old and new file sizes, the old file size as the event offset, and zero byte length.
- A single supported `setattr` operation that changes file size and metadata MUST commit all changes atomically in one event and MUST use a file write event when the file size changes.
- `EventKind` MUST include filesystem initialization, node creation, directory creation, file creation, file write, metadata change, node unlink, directory removal, node rename, hard link creation, symbolic link creation, extended attribute set, and extended attribute removal.
- Metadata changes and extended attribute set and removal MUST append events when they change logical filesystem state.
- `get_event` MUST return the event at the requested sequence when it exists.
- `current_branch` MUST return the currently active branch.
- `list_branches` MUST return branches ordered by branch identifier with pagination.
- `create_branch` MUST create a uniquely named branch from an existing branch position, copy that position's namespace root, and not duplicate content chunks, content manifest nodes, namespace nodes, or per-file snapshot records.
- `switch_branch` MUST change the active branch only when the filesystem is not mounted and MUST NOT append an event.
- `delete_branch` MUST reject the active and initial branches and otherwise delete only the branch name/ref, not committed events or content chunks.
- `list_events` MUST return at most `limit` records with sequences greater than `after` when present, and expose the next cursor only when another page exists.
- `list_branch_events` MUST return events in branch-position order for the requested branch.
- `list_branch_file_events` MUST return only events for the requested branch and file identifier in branch-position order.
- `list_file_events` MUST return events for the active branch and requested file identifier in branch-position order.
- `file_snapshot_on_branch_at_or_before` MUST return the newest persisted snapshot for the requested branch and file whose branch position is less than or equal to the requested position.
- `file_snapshot_at_or_before` MUST return the newest persisted snapshot for the active branch and requested file whose event sequence is less than or equal to the requested sequence.
- `read_file_event_payload_range` MUST return the requested payload byte range and an empty vector for bytes beyond the payload length.
- `read_file_snapshot_range` MUST return the requested snapshot byte range and MUST return an empty vector for bytes beyond the snapshot file size.
- Event listing MUST scan event indexes and MUST NOT replay stored file chunks.
- Event record debug formatting MUST NOT expose stored file payload bytes.
- File snapshots MUST be derived content manifest roots maintained from committed file events, stored outside event metadata, and bound the number of file content events after the returned snapshot that a caller must replay to reconstruct a file at a later event sequence.
- The initial branch MUST be named `main`.

## Storage

RocksDB MUST contain these column families; keys MUST follow each stated contract:

| Column family | Key contract |
| --- | --- |
| `events` | Big-endian event sequence |
| `filesystem_metadata` | — |
| `file_events` | Ordered by file identifier and event sequence |
| `branches` | — |
| `branch_names` | — |
| `branch_events` | Ordered by branch identifier and branch position |
| `branch_file_events` | Ordered by branch identifier, file identifier, and branch position |
| `content_chunks` | Cryptographic content identifier |
| `content_manifest_nodes` | Cryptographic content manifest identifier |
| `namespace_nodes` | Cryptographic namespace identifier |
| `branch_roots` | Ordered by branch identifier and branch position |
| `file_snapshot_manifests` | Ordered by branch identifier, file identifier, and branch position |
| `event_payload_manifests` | Ordered by event sequence and payload part |

- `content_chunks` MUST use RocksDB integrated BlobDB, with each content chunk an immutable whole BlobDB value.
- Content manifest nodes MUST be immutable values that reference `content_chunks`.
- Namespace nodes MUST be immutable typed Merkle nodes that represent namespace roots, map branches, and map leaves for inode metadata, directory entries, extended attributes, and regular file content manifest roots.
- Current file state MUST be read from the active branch namespace root.
- File snapshots and event payloads MUST be represented by content manifest roots.
- Branch records MUST store the branch head namespace root.
- `branch_roots` MUST store each committed branch position's namespace root.
- Content chunks MUST use FastCDC v2020 with 16 KiB minimum, 64 KiB target, 256 KiB maximum, and seed 0; identifiers MUST be cryptographic digests of chunk bytes, not FastCDC rolling fingerprints.
- Content manifest and namespace identifiers MUST be cryptographic digests of their respective canonical content manifest and namespace node bytes.
- eventfs MUST use RocksDB pinned reads for durable byte content whenever the API or internal operation can borrow the value, and MUST materialize owned byte buffers only when mutation, FUSE output, serialization, or caller-owned copies require them.
- `filesystem_metadata` MUST store storage schema version, next inode number, last committed event sequence, next branch identifier, active branch identifier, and volume name.
- Stored inodes MUST include access, modification, and status-change times, ownership, and permission mode, but MUST NOT store file flags or Linux birth-time metadata.
- New root inodes MUST be owned by the effective user and group that created the database and MUST use permission mode `0755`.
- New non-root inodes MUST be owned by the FUSE request user, MUST use the FUSE request group unless parent-directory setgid inheritance applies, and MUST apply the request umask to the requested permission mode.
- New symbolic links MUST use permission mode `0777`.
- `Filesystem::open` MUST create a missing database and required column families for a new database directory.
- Storage schema version `0` MUST be the first supported storage schema compatibility baseline.
- `Filesystem::open` MUST reject existing databases missing metadata required by their stored schema version, and MUST reject storage schema versions newer than the compiled current storage schema version before mutating storage.
- `Filesystem::open` MUST automatically migrate compatible released storage schema versions to the compiled current storage schema version before returning.
- Storage schema migrations MUST run in schema-version order, be idempotent and crash-safe, and MUST NOT append filesystem events, rewrite committed event keys, change event sequences, or alter user-observable history.
- Migrations MAY create column families introduced by later compatible storage schemas and rebuild derived indexes from committed events and materialized current filesystem state.
- Missing column families required by the stored schema version MUST be rejected as corrupt storage; those introduced after the stored compatible schema version MUST be created during migration.
- Reads required by FUSE MUST use materialized current filesystem state, not event-log replay.
- Mounted mutating filesystem operations MUST keep RocksDB write-ahead logging enabled and use non-synchronous RocksDB write batches unless synchronized I/O semantics require synchronization before success.
- Filesystem synchronization operations MUST synchronize the RocksDB write-ahead log to persistent storage.

## Local Backup And Import

- Local backup and import MUST always be compiled and use RocksDB `BackupEngine`.
- `BackupDirectory` MUST identify a local RocksDB BackupEngine repository directory.
- Backup, import target, and source database directories MUST NOT overlap after path normalization.
- `create_backup` MUST open or create the specified directory as a persistent BackupEngine repository, preserve its contents across repeated calls with the same `BackupDirectory`, create a new incremental BackupEngine backup with memtable flush enabled, and verify the newly created backup before returning `BackupReceipt`.
- `BackupReceipt` MUST expose the created BackupEngine backup identifier and the source event sequence.
- `import_backup` MUST verify the requested BackupEngine backup identifier before restoring.
- `import_backup` MUST reject an empty target database directory as `FilesystemError::Import`.
- `import_backup` MUST restore the requested backup into a temporary directory and successfully open and migrate the restored RocksDB database before replacing the target database directory.
- `import_backup` MUST discard existing target data before moving the verified restored database into place.
- eventfs MUST NOT perform remote object storage synchronization.

## Operations

- `FilesystemConfiguration.database_directory` MUST identify the durable RocksDB data directory; `FilesystemConfiguration.mount_point` MUST identify only the FUSE presentation mount point; and `BackupDirectory` MUST identify the local BackupEngine repository retained for backups.
- Backup rotation MUST retain, archive, or replace whole `BackupDirectory` repositories outside active `create_backup` and `import_backup` calls; eventfs MUST NOT expose APIs for deleting individual BackupEngine backups.
- Recovery MUST use `Filesystem::import_backup` to restore a verified backup and MUST successfully open and migrate the restored RocksDB database before replacing the target database directory.
- Downgrade to an older eventfs version MUST use a backup whose storage schema has not been migrated beyond that older version's compiled current storage schema version; opening a database with a newer storage schema MUST fail before mutation.
- Repository operational documentation MUST describe data location, backup rotation, recovery, and upgrade/downgrade behavior.

## Errors

- Public methods MUST return `FilesystemError`, except configuration constructor failures MUST return `ConfigurationError`.
- Non-FUSE public API failures MUST NOT invoke the configured FUSE error callback.

Failures MUST map and accessors MUST return as follows:

| Source | Required result |
| --- | --- |
| RocksDB failure | `FilesystemError::Database` |
| Corrupt storage or backup repository; missing backup files; unknown backup identifier | `FilesystemError::Integrity` |
| FUSE operation failure | `FilesystemError::FilesystemOperation` |
| `FuseOperationError::filesystem_error` | `FilesystemError::FilesystemOperation` |
| Backup repository creation or opening; backup creation | `FilesystemError::Backup` |
| Import target replacement; import-open failure | `FilesystemError::Import` |

## Benchmarks

- eventfs MUST provide a Linux-only Criterion benchmark target named `fuse_operations`; it MUST fail to compile on non-Linux targets with an explicit Linux-only error.
- `fuse_operations` MUST mount eventfs through the public `FilesystemConfiguration`, `Filesystem::open`, and `Filesystem::spawn_mount` APIs.
- `fuse_operations` MUST benchmark every supported Linux mounted FUSE operation with an equivalent operation on a sibling directory in the current host's normal filesystem.
- `fuse_operations` MUST exclude non-Linux mounted operations and fuser lifecycle or cache callbacks without a normal-filesystem operation baseline.
- `fuse_operations` MUST exclude per-iteration setup and cleanup from timed measurements for mutating operations.
- `fuse_operations` MUST group eventfs and host filesystem measurements by FUSE operation name.

## Acceptance Tests

- Tests MUST NOT assert the complete public API inventory.
- End-to-end tests under `tests/` MUST contain only realistic user-facing use cases and edge cases, comprehensively cover every FUSE operation defined in this specification, and MUST NOT assert private dependencies, private storage encodings, or other internal implementation details.
- Unit tests inside Rust source files MUST test internal behavior below end-to-end boundaries and maintain at least 95% test coverage as measured by `llvm-conv`.
- Longer FUSE stress, crash/fault-injection, and concurrency/load tests MUST use bounded deterministic workloads in the default test suite.

Implementations MUST include automated tests for:

- **Public API and listing:** public constructors reject invalid configuration values; event listing returns paginated events with UTC creation times; event, branch, branch event, branch file event, and active branch file event iterators return the same ordered records as explicit pagination; and `get_event` returns the requested event or no event.
- **Backup and import:** local backup creates increasing non-zero BackupEngine backup identifiers in a persistent backup directory; local import verifies the requested BackupEngine backup, replaces existing target data, and opens and migrates the imported database before success; and both reject overlapping source, backup, and target directories after path normalization.
- **Storage schemas:** opening version `0` succeeds and preserves public filesystem behavior; every compatible released version migrates to the current storage schema version; versions newer than the compiled current storage schema version fail without mutation; interrupted migrations resume on reopen or leave a valid compatible released schema; and opening a compatible released database creates missing column families introduced after its stored schema version during migration but fails as corrupt storage when column families required by that version are missing.
- **Events and content:** event records expose file write payload sizes without payload bytes; file event payload range reads expose file write payload bytes; per-file event listing returns only events for the requested active branch file in branch-position order; branch event listing returns only events for the requested branch in branch-position order; file snapshots return the nearest snapshot at or before a requested sequence; snapshots and event payloads reconstruct file bytes before and after a write event; snapshot and event payload content is read through range APIs backed by content chunk reads; chunks deduplicate by content identifier; cross-chunk reads, overwrites, and sparse zero-filled ranges return correct bytes; and event debug formatting hides stored file payload bytes.
- **Branches:** creation copies namespace root identifiers without duplicating content chunks, content manifest nodes, namespace nodes, or file snapshot manifest records and creates an independent branch from a branch position; switching changes the mounted filesystem's future active state only while unmounted; deletion removes the branch ref without deleting committed events or content chunks; and switching, divergence, file event listing, and snapshot reads preserve independent file contents.
- **Mounted FUSE capabilities:** every supported operation works through the mounted filesystem; extended attributes, metadata changes, and file size changes work through it; and client-style upload, overwrite upload, metadata-preserving upload, download, directory, rename, remove, hard link, symbolic link, symbolic link read, filesystem-statistics, and fsync workflows work through it.
- **Mounted FUSE behavior:** POSIX byte-range locks, block mapping, ioctl, poll, space allocation, sparse seek, and file-range copy fail as unsupported; mutating supported operations append exactly the specified event kinds while supported read-only and no-op operations append none; configured error callbacks receive failed supported and unsupported operation errors with the returned errno while successful operations invoke none; and operation tests cover success, failure, edge cases, mutating event appends, and no-event behavior for read-only and unsupported operations.
- **Stress, durability, and load:** longer mounted stress tests cover repeated and combined supported operations plus edge cases; crash and fault injection around write batches, filesystem synchronization, backup creation, backup import replacement, and mount/unmount state transitions preserves committed state consistency; synced mounted mutations MUST recover after simulated system crash while unsynced mounted mutations MAY be absent after simulated system crash; concurrency and load tests cover branch switching and deletion, mounted writes, and event, branch, and file listing; and filesystem statistics report backing block capacity and eventfs inode capacity without appending events.
