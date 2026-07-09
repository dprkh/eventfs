#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB,
    DataBlockIndexType, Options, SliceTransform, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use time::UtcDateTime;

use crate::filesystem::{
    BranchEventPage, BranchIdentifier, BranchName, BranchPage, BranchPageLimit, BranchPosition,
    BranchRecord, BranchStatus, EventKind, EventPage, EventPageLimit, EventPayload, EventRecord,
    EventSequence, FileEventPayloadPart, FileIdentifier, FileSnapshot, FilesystemConfiguration,
    FilesystemError,
};

pub(crate) struct Storage {
    database: DB,
    write_state: Mutex<WriteState>,
    active_branch_state: Mutex<ActiveBranchState>,
    active_branch_identifier: AtomicU64,
}

#[derive(Clone, Debug)]
pub(crate) struct EntrySnapshot {
    pub(crate) ttl: Duration,
    pub(crate) attr: fuser::FileAttr,
}

#[derive(Clone, Debug)]
pub(crate) struct DirectoryEntrySnapshot {
    pub(crate) inode: u64,
    pub(crate) offset: u64,
    pub(crate) kind: fuser::FileType,
    pub(crate) name: OsString,
}

#[derive(Clone, Debug)]
pub(crate) struct DirectoryEntryPlusSnapshot {
    pub(crate) inode: u64,
    pub(crate) offset: u64,
    pub(crate) name: OsString,
    pub(crate) entry: EntrySnapshot,
}

#[derive(Clone, Debug)]
pub(crate) struct FileSystemStatistics {
    pub(crate) blocks: u64,
    pub(crate) free_blocks: u64,
    pub(crate) available_blocks: u64,
    pub(crate) files: u64,
    pub(crate) free_files: u64,
    pub(crate) block_size: u32,
    pub(crate) maximum_name_length: u32,
    pub(crate) fragment_size: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateNodeKind {
    Directory,
    RegularFile,
    Special { mode: u32, rdev: u32 },
}

#[derive(Clone, Debug)]
pub(crate) struct FuseWrite {
    pub(crate) written: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct ExtendedAttributeList {
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CreateNodeMetadata {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
    pub(crate) mode: u32,
    pub(crate) umask: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SetattrMetadata {
    pub(crate) request_uid: u32,
    pub(crate) request_gid: u32,
    pub(crate) mode: Option<u32>,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) size: Option<u64>,
    pub(crate) atime: Option<SystemTime>,
    pub(crate) mtime: Option<SystemTime>,
}

#[derive(Clone, Debug)]
pub(crate) enum FuseError {
    Errno(fuser::Errno),
    Database,
    Integrity,
}

impl FuseError {
    pub(crate) fn errno(&self) -> fuser::Errno {
        match self {
            Self::Errno(errno) => *errno,
            Self::Database | Self::Integrity => fuser::Errno::EIO,
        }
    }
}

#[cfg(test)]
impl CreateNodeMetadata {
    fn default_for_kind(kind: CreateNodeKind) -> Self {
        let mode = match kind {
            CreateNodeKind::Directory => u32::from(INODE_MODE_DEFAULT_DIRECTORY),
            CreateNodeKind::RegularFile | CreateNodeKind::Special { .. } => {
                u32::from(INODE_MODE_DEFAULT_FILE)
            }
        };
        Self {
            uid: 0,
            gid: 0,
            mode,
            umask: 0,
        }
    }
}

pub(crate) fn open_database(
    configuration: &FilesystemConfiguration,
) -> Result<Storage, FilesystemError> {
    open_database_path(configuration.database_directory())
}

pub(crate) fn open_database_path(path: &Path) -> Result<Storage, FilesystemError> {
    let is_new_database = is_new_database_directory(path)?;
    let block_cache = Cache::new_lru_cache(ROCKSDB_BLOCK_CACHE_CAPACITY);
    let options = database_options(is_new_database, &block_cache);

    let database = if is_new_database {
        let descriptors = column_family_descriptors(required_column_family_names(), &block_cache);
        let database = DB::open_cf_descriptors(&options, path, descriptors)
            .map_err(|_| FilesystemError::Database)?;
        initialize_new_database(&database)?;
        database
    } else {
        let existing_column_families = existing_column_family_names(&options, path)?;
        let descriptors = column_family_descriptors(existing_column_families, &block_cache);
        let mut database = DB::open_cf_descriptors(&options, path, descriptors)
            .map_err(|_| FilesystemError::Database)?;
        migrate_database_to_current_schema(&mut database, &block_cache)?;
        validate_existing_database(&database)?;
        database
    };
    let write_state = load_write_state(&database)?;
    let active_branch = stored_branch_in_database(
        &database,
        BranchIdentifier::new(write_state.active_branch_identifier),
    )?;
    let active_namespace_root =
        namespace_root_in_database(&database, &active_branch.namespace_identifier)?;
    let active_namespace =
        namespace_state_for_identifier_in_database(&database, &active_branch.namespace_identifier)?;
    Ok(storage(
        database,
        write_state,
        active_branch,
        active_namespace_root,
        active_namespace,
    ))
}

fn storage(
    database: DB,
    write_state: WriteState,
    active_branch: StoredBranch,
    active_namespace_root: StoredNamespaceRoot,
    active_namespace: MaterializedBranchState,
) -> Storage {
    let active_branch_identifier = write_state.active_branch_identifier;
    Storage {
        database,
        write_state: Mutex::new(write_state),
        active_branch_state: Mutex::new(ActiveBranchState::new(
            active_branch,
            active_namespace_root,
            active_namespace,
        )),
        active_branch_identifier: AtomicU64::new(active_branch_identifier),
    }
}

type FuseResult<T> = Result<T, FuseError>;

const STORAGE_SCHEMA_VERSION_BASELINE: u64 = 0;
const STORAGE_SCHEMA_VERSION_CURRENT: u64 = 0;

const EVENT_SEQUENCE_INITIAL: EventSequence = EventSequence::new(0);
const BRANCH_IDENTIFIER_INITIAL: BranchIdentifier = BranchIdentifier::new(1);
const BRANCH_NAME_INITIAL: &str = "main";

const INODE_ROOT: u64 = 1;
const INODE_FIRST_ALLOCATED: u64 = 2;
const INODE_LOGICAL_CAPACITY: u64 = u64::MAX - 1;

const INODE_MODE_PERMISSION_MASK: u32 = 0o7777;
#[cfg(test)]
const INODE_MODE_DEFAULT_FILE: u16 = 0o666;
#[cfg(test)]
const INODE_MODE_DEFAULT_DIRECTORY: u16 = 0o777;
#[cfg(test)]
const INODE_MODE_DEFAULT_SYMLINK: u16 = 0o777;
const INODE_MODE_ROOT: u16 = 0o755;

const XATTR_NAME_MAX_BYTES: usize = 255;
const XATTR_VALUE_MAX_BYTES: usize = 65_536;
const XATTR_LIST_MAX_BYTES: usize = 65_536;
const XATTR_USER_PREFIX: &[u8] = b"user.";

const CONTENT_CHUNK_MIN_SIZE: usize = 16 * 1024;
const CONTENT_CHUNK_TARGET_SIZE: usize = 64 * 1024;
const CONTENT_CHUNK_MAX_SIZE: usize = 256 * 1024;
const CONTENT_CHUNK_FASTCDC_SEED: u64 = 0;

const FUSE_ATTRIBUTE_TTL: Duration = Duration::from_secs(1);
const FUSE_STATFS_BLOCK_SIZE: u32 = 4096;
const FUSE_MAX_NAME_LENGTH: usize = 255;
const FUSE_DIRECTORY_ENTRY_FIRST_OFFSET: u64 = 3;

const ROCKSDB_BLOCK_CACHE_CAPACITY: usize = 128 * 1024 * 1024;
const ROCKSDB_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;
const ROCKSDB_BYTES_PER_SYNC: u64 = 1024 * 1024;
const ROCKSDB_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
const ROCKSDB_MAX_WRITE_BUFFER_NUMBER: i32 = 4;
const ROCKSDB_BLOB_MIN_SIZE: u64 = 1;

const COLUMN_FAMILY_EVENTS: &str = "events";
const COLUMN_FAMILY_FILESYSTEM_METADATA: &str = "filesystem_metadata";
const COLUMN_FAMILY_FILE_EVENTS: &str = "file_events";
const COLUMN_FAMILY_BRANCHES: &str = "branches";
const COLUMN_FAMILY_BRANCH_NAMES: &str = "branch_names";
const COLUMN_FAMILY_BRANCH_EVENTS: &str = "branch_events";
const COLUMN_FAMILY_BRANCH_FILE_EVENTS: &str = "branch_file_events";
const COLUMN_FAMILY_CONTENT_CHUNKS: &str = "content_chunks";
const COLUMN_FAMILY_CONTENT_MANIFEST_NODES: &str = "content_manifest_nodes";
const COLUMN_FAMILY_NAMESPACE_NODES: &str = "namespace_nodes";
const COLUMN_FAMILY_BRANCH_ROOTS: &str = "branch_roots";
const COLUMN_FAMILY_FILE_SNAPSHOT_MANIFESTS: &str = "file_snapshot_manifests";
const COLUMN_FAMILY_EVENT_PAYLOAD_MANIFESTS: &str = "event_payload_manifests";
const COLUMN_FAMILY_REQUIRED: &[&str] = &[
    COLUMN_FAMILY_EVENTS,
    COLUMN_FAMILY_FILESYSTEM_METADATA,
    COLUMN_FAMILY_FILE_EVENTS,
    COLUMN_FAMILY_BRANCHES,
    COLUMN_FAMILY_BRANCH_NAMES,
    COLUMN_FAMILY_BRANCH_EVENTS,
    COLUMN_FAMILY_BRANCH_FILE_EVENTS,
    COLUMN_FAMILY_CONTENT_CHUNKS,
    COLUMN_FAMILY_CONTENT_MANIFEST_NODES,
    COLUMN_FAMILY_NAMESPACE_NODES,
    COLUMN_FAMILY_BRANCH_ROOTS,
    COLUMN_FAMILY_FILE_SNAPSHOT_MANIFESTS,
    COLUMN_FAMILY_EVENT_PAYLOAD_MANIFESTS,
];

const METADATA_KEY_STORAGE_SCHEMA_VERSION: &[u8] = b"schema_version";
const METADATA_KEY_NEXT_INODE_NUMBER: &[u8] = b"next_inode_number";
const METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE: &[u8] = b"last_committed_event_sequence";
const METADATA_KEY_NEXT_BRANCH_IDENTIFIER: &[u8] = b"next_branch_identifier";
const METADATA_KEY_ACTIVE_BRANCH_IDENTIFIER: &[u8] = b"active_branch_identifier";
const METADATA_KEY_VOLUME_NAME: &[u8] = b"volume_name";
const METADATA_VOLUME_NAME_DEFAULT: &str = "eventfs";

const EVENT_PAYLOAD_PART_OVERWRITTEN: u8 = b'o';
const EVENT_PAYLOAD_PART_WRITTEN: u8 = b'w';
const CONTENT_CHUNK_HASH_BLAKE3: u8 = 1;
const CONTENT_MANIFEST_HASH_BLAKE3: u8 = 2;
const CONTENT_MANIFEST_NODE_MAGIC: &[u8] = b"eventfs.content-manifest.node.v1\0";
const CONTENT_MANIFEST_CONCAT_MAX_CHILDREN: usize = 256;
const CONTENT_MANIFEST_MAX_DEPTH: usize = 4096;
const NAMESPACE_HASH_BLAKE3: u8 = 3;
const NAMESPACE_MAP_LEAF_MAX_ENTRIES: usize = 32;
const NAMESPACE_MAP_TAG_INODES: u8 = b'i';
const NAMESPACE_MAP_TAG_DIRECTORY_ENTRIES: u8 = b'd';
const NAMESPACE_MAP_TAG_EXTENDED_ATTRIBUTES: u8 = b'x';

#[derive(Clone, Debug)]
struct BackingFileSystemStatistics {
    blocks: u64,
    free_blocks: u64,
    available_blocks: u64,
    fragment_size: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WriteState {
    next_inode_number: u64,
    last_event_sequence: u64,
    next_branch_identifier: u64,
    active_branch_identifier: u64,
    active_branch_head_sequence: u64,
    active_branch_head_ordinal: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteDurability {
    Buffered,
    Synchronous,
}

#[derive(Clone, Debug)]
struct ActiveBranchState {
    branch: StoredBranch,
    namespace_root: StoredNamespaceRoot,
    namespace: MaterializedBranchState,
}

#[derive(Clone, Debug)]
struct ActiveBranchCommit {
    branch: StoredBranch,
    namespace_root: StoredNamespaceRoot,
    mutation: StoredMutation,
}

impl WriteState {
    fn next_inode_number(self) -> FuseResult<u64> {
        if self.next_inode_number < INODE_FIRST_ALLOCATED {
            return Err(FuseError::Integrity);
        }
        if self.next_inode_number == u64::MAX {
            return Err(FuseError::Errno(fuser::Errno::ENOSPC));
        }
        Ok(self.next_inode_number)
    }

    fn next_event_sequence(self) -> FuseResult<EventSequence> {
        self.last_event_sequence
            .checked_add(1)
            .map(EventSequence::new)
            .ok_or(FuseError::Integrity)
    }

    fn record_committed_event(&mut self, event_sequence: EventSequence) {
        self.last_event_sequence = event_sequence.get();
        self.active_branch_head_sequence = event_sequence.get();
        self.active_branch_head_ordinal = self.active_branch_head_ordinal.saturating_add(1);
    }

    fn record_committed_inode_number(&mut self, next_inode_number: u64) {
        self.next_inode_number = next_inode_number;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
enum StoredNodeKind {
    NamedPipe,
    CharacterDevice,
    BlockDevice,
    Directory,
    RegularFile,
    Symlink,
    Socket,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredTime {
    seconds: i64,
    nanoseconds: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredInode {
    kind: StoredNodeKind,
    size: u64,
    nlink: u32,
    uid: u32,
    gid: u32,
    mode: u16,
    rdev: u32,
    atime: StoredTime,
    mtime: StoredTime,
    ctime: StoredTime,
    parent: u64,
    name: Vec<u8>,
    symlink_target: Option<Vec<u8>>,
    content_manifest_identifier: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredDirectoryEntry {
    inode: u64,
    kind: StoredNodeKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredBranch {
    identifier: u64,
    name: String,
    status: BranchStatus,
    head_sequence: u64,
    head_ordinal: u64,
    namespace_identifier: Vec<u8>,
    fork_branch_identifier: Option<u64>,
    fork_ordinal: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredBranchRoot {
    sequence: u64,
    namespace_identifier: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredEvent {
    record: EventRecord,
    mutation: StoredMutation,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
struct StoredMutation {
    inode_puts: Vec<StoredInodePut>,
    inode_deletes: Vec<u64>,
    directory_entry_puts: Vec<StoredDirectoryEntryPut>,
    directory_entry_deletes: Vec<StoredDirectoryEntryDelete>,
    extended_attribute_puts: Vec<StoredExtendedAttributePut>,
    extended_attribute_deletes: Vec<StoredExtendedAttributeDelete>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredInodePut {
    inode_number: u64,
    inode: StoredInode,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredDirectoryEntryPut {
    parent: u64,
    name: Vec<u8>,
    entry: StoredDirectoryEntry,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredDirectoryEntryDelete {
    parent: u64,
    name: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredExtendedAttributePut {
    inode_number: u64,
    name: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredExtendedAttributeDelete {
    inode_number: u64,
    name: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredExtent {
    logical_offset: u64,
    length: u64,
    chunk_identifier: Vec<u8>,
    chunk_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
enum StoredContentManifestNode {
    Concat {
        children: Vec<StoredContentManifestChild>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredContentManifestChild {
    logical_offset: u64,
    length: u64,
    identifier: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DecodedContentManifest {
    Extents(Vec<StoredExtent>),
    Node(StoredContentManifestNode),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredSnapshotMetadata {
    file_size: u64,
    sequence: u64,
    content_manifest_identifier: Vec<u8>,
}

struct FileSnapshotWriteContext<'a> {
    event_sequence: EventSequence,
    event: &'a EventRecord,
    file_identifier: FileIdentifier,
    branch_position: BranchPosition,
    final_inode: Option<&'a StoredInode>,
    snapshot_extents: Option<&'a [StoredExtent]>,
    snapshot_content_manifest_identifier: Option<&'a [u8]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredEventPayloadManifest {
    byte_length: u64,
    content_manifest_identifier: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContentManifestIdentifier<'a> {
    bytes: &'a [u8],
}

impl<'a> ContentManifestIdentifier<'a> {
    fn from_bytes(bytes: &'a [u8]) -> Result<Self, FilesystemError> {
        validate_prefixed_blake3_identifier_shape(bytes, CONTENT_MANIFEST_HASH_BLAKE3)?;
        Ok(Self { bytes })
    }

    fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NamespaceIdentifier<'a> {
    bytes: &'a [u8],
}

impl<'a> NamespaceIdentifier<'a> {
    fn from_bytes(bytes: &'a [u8]) -> Result<Self, FilesystemError> {
        validate_prefixed_blake3_identifier_shape(bytes, NAMESPACE_HASH_BLAKE3)?;
        Ok(Self { bytes })
    }

    fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Clone, Debug)]
struct PendingExtent {
    logical_offset: u64,
    length: u64,
    byte_start: usize,
    byte_end: usize,
    chunk_identifier: Vec<u8>,
}

impl PendingExtent {
    fn byte_range(&self) -> std::ops::Range<usize> {
        self.byte_start..self.byte_end
    }
}

#[derive(Clone, Debug)]
enum EventPayloadExtents {
    None,
    FileWrite {
        overwritten_extents: Vec<StoredExtent>,
        written_extents: Vec<StoredExtent>,
    },
}

#[derive(Clone, Debug)]
struct CommitEventPayloads<'a> {
    event_payload_extents: EventPayloadExtents,
    snapshot_content_manifest_identifier: Option<&'a [u8]>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
struct MaterializedBranchState {
    inodes: BTreeMap<u64, StoredInode>,
    directory_entries: BTreeMap<(u64, Vec<u8>), StoredDirectoryEntry>,
    extended_attributes: BTreeMap<(u64, Vec<u8>), Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
enum StoredNamespaceNode {
    Root(StoredNamespaceRoot),
    Branch(Vec<StoredNamespaceChild>),
    Leaf(Vec<StoredNamespaceEntry>),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredNamespaceRoot {
    inodes: Vec<u8>,
    directory_entries: Vec<u8>,
    extended_attributes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredNamespaceChild {
    nibble: u8,
    identifier: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct StoredNamespaceEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NamespaceMapKind {
    Inodes,
    DirectoryEntries,
    ExtendedAttributes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NamespaceMapMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Copy, Debug)]
enum ExtentSet {
    Current {
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
    },
    Snapshot {
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        ordinal: u64,
    },
    EventPayload {
        sequence: EventSequence,
        part: u8,
    },
}

impl StoredMutation {
    fn put_inode(&mut self, inode_number: u64, inode: &StoredInode) {
        self.inode_puts.push(StoredInodePut {
            inode_number,
            inode: inode.clone(),
        });
    }

    fn delete_inode(&mut self, inode_number: u64) {
        self.inode_deletes.push(inode_number);
    }

    fn put_directory_entry(&mut self, parent: u64, name: &[u8], entry: &StoredDirectoryEntry) {
        self.directory_entry_puts.push(StoredDirectoryEntryPut {
            parent,
            name: name.to_vec(),
            entry: entry.clone(),
        });
    }

    fn delete_directory_entry(&mut self, parent: u64, name: &[u8]) {
        self.directory_entry_deletes
            .push(StoredDirectoryEntryDelete {
                parent,
                name: name.to_vec(),
            });
    }

    fn put_extended_attribute(&mut self, inode_number: u64, name: &[u8], value: &[u8]) {
        self.extended_attribute_puts
            .push(StoredExtendedAttributePut {
                inode_number,
                name: name.to_vec(),
                value: value.to_vec(),
            });
    }

    fn delete_extended_attribute(&mut self, inode_number: u64, name: &[u8]) {
        self.extended_attribute_deletes
            .push(StoredExtendedAttributeDelete {
                inode_number,
                name: name.to_vec(),
            });
    }
}

impl<'a> CommitEventPayloads<'a> {
    fn none() -> Self {
        Self {
            event_payload_extents: EventPayloadExtents::None,
            snapshot_content_manifest_identifier: None,
        }
    }

    fn file_write_append(
        overwritten_extents: Vec<StoredExtent>,
        written_extents: Vec<StoredExtent>,
        snapshot_content_manifest_identifier: &'a [u8],
    ) -> Self {
        Self {
            event_payload_extents: EventPayloadExtents::FileWrite {
                overwritten_extents,
                written_extents,
            },
            snapshot_content_manifest_identifier: Some(snapshot_content_manifest_identifier),
        }
    }

    fn file_write_without_extent_update(
        overwritten_extents: Vec<StoredExtent>,
        written_extents: Vec<StoredExtent>,
        snapshot_content_manifest_identifier: &'a [u8],
    ) -> Self {
        Self {
            event_payload_extents: EventPayloadExtents::FileWrite {
                overwritten_extents,
                written_extents,
            },
            snapshot_content_manifest_identifier: Some(snapshot_content_manifest_identifier),
        }
    }
}

impl MaterializedBranchState {
    fn apply(&mut self, mutation: &StoredMutation) {
        for inode_number in &mutation.inode_deletes {
            self.inodes.remove(inode_number);
            self.extended_attributes
                .retain(|(attribute_inode, _name), _value| attribute_inode != inode_number);
        }
        for delete in &mutation.directory_entry_deletes {
            self.directory_entries
                .remove(&(delete.parent, delete.name.clone()));
        }
        for delete in &mutation.extended_attribute_deletes {
            self.extended_attributes
                .remove(&(delete.inode_number, delete.name.clone()));
        }
        for put in &mutation.inode_puts {
            self.inodes.insert(put.inode_number, put.inode.clone());
        }
        for put in &mutation.directory_entry_puts {
            self.directory_entries
                .insert((put.parent, put.name.clone()), put.entry.clone());
        }
        for put in &mutation.extended_attribute_puts {
            self.extended_attributes
                .insert((put.inode_number, put.name.clone()), put.value.clone());
        }
    }
}

fn namespace_inode_after_mutation<'a>(
    namespace: &'a MaterializedBranchState,
    mutation: &'a StoredMutation,
    inode_number: u64,
) -> Option<&'a StoredInode> {
    if mutation.inode_deletes.contains(&inode_number) {
        return None;
    }
    mutation
        .inode_puts
        .iter()
        .rev()
        .find(|put| put.inode_number == inode_number)
        .map(|put| &put.inode)
        .or_else(|| namespace.inodes.get(&inode_number))
}

impl ActiveBranchState {
    fn new(
        branch: StoredBranch,
        namespace_root: StoredNamespaceRoot,
        namespace: MaterializedBranchState,
    ) -> Self {
        Self {
            branch,
            namespace_root,
            namespace,
        }
    }

    fn apply_commit(&mut self, commit: ActiveBranchCommit) {
        self.branch = commit.branch;
        self.namespace_root = commit.namespace_root;
        self.namespace.apply(&commit.mutation);
    }
}

fn update_parent_after_entry_removal(
    removed_inode: &StoredInode,
    parent_inode: &mut StoredInode,
    timestamp: StoredTime,
) {
    if removed_inode.kind == StoredNodeKind::Directory {
        parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
    }
    touch_loaded_directory(parent_inode, timestamp);
}

fn touch_loaded_directory(inode: &mut StoredInode, timestamp: StoredTime) {
    inode.mtime = timestamp;
    inode.ctime = timestamp;
}

impl Storage {
    pub(crate) fn database(&self) -> &DB {
        &self.database
    }

    pub(crate) fn with_commit_lock<T>(
        &self,
        operation: impl FnOnce() -> Result<T, FilesystemError>,
    ) -> Result<T, FilesystemError> {
        let _guard = self
            .write_state
            .lock()
            .map_err(|_| FilesystemError::Database)?;
        operation()
    }

    fn active_branch_identifier(&self) -> BranchIdentifier {
        BranchIdentifier::new(self.active_branch_identifier.load(Ordering::SeqCst))
    }

    pub(crate) fn last_event_sequence(&self) -> Result<EventSequence, FilesystemError> {
        read_metadata_u64(&self.database, METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE)
            .map(EventSequence::new)
    }

    pub(crate) fn list_events(
        &self,
        after: Option<EventSequence>,
        limit: EventPageLimit,
    ) -> Result<EventPage, FilesystemError> {
        let events = self.events()?;
        let start = match after.and_then(|sequence| sequence.get().checked_add(1)) {
            Some(sequence) => sequence,
            None if after.is_some() => return Ok(EventPage::new(Vec::new(), None)),
            None => 0,
        };
        let start_key = encode_u64(start);
        let snapshot = self.database.snapshot();
        let mut iterator = snapshot.raw_iterator_cf(events);
        let mut records = Vec::new();
        let mut next_after = None;

        iterator.seek(start_key);
        while iterator.valid() {
            if records.len() == limit.get() as usize {
                next_after = records.last().map(EventRecord::sequence);
                break;
            }
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            let value = iterator.value().ok_or(FilesystemError::Database)?;
            let event = decode_stored_event_at_key(key, value)?;
            records.push(event.record);
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;

        Ok(EventPage::new(records, next_after))
    }

    pub(crate) fn get_event(
        &self,
        sequence: EventSequence,
    ) -> Result<Option<EventRecord>, FilesystemError> {
        self.event(sequence)
    }

    pub(crate) fn current_branch(&self) -> Result<BranchRecord, FilesystemError> {
        let active = self
            .active_branch_state
            .lock()
            .map_err(|_| FilesystemError::Database)?;
        branch_record_from_stored(active.branch.clone())
    }

    pub(crate) fn list_branches(
        &self,
        after: Option<BranchIdentifier>,
        limit: BranchPageLimit,
    ) -> Result<BranchPage, FilesystemError> {
        let branches = self.branches()?;
        let start = match after {
            Some(identifier) => match identifier.get().checked_add(1) {
                Some(start) => start,
                None => return Ok(BranchPage::new(Vec::new(), None)),
            },
            None => BRANCH_IDENTIFIER_INITIAL.get(),
        };
        let mut iterator = self.database.raw_iterator_cf(branches);
        let mut records = Vec::new();
        let mut next_after = None;

        iterator.seek(encode_u64(start));
        while iterator.valid() {
            if records.len() == limit.get() as usize {
                next_after = records.last().map(BranchRecord::branch_identifier);
                break;
            }
            let value = iterator.value().ok_or(FilesystemError::Database)?;
            let stored = decode_branch(value)?;
            records.push(branch_record_from_stored(stored)?);
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;
        Ok(BranchPage::new(records, next_after))
    }

    pub(crate) fn create_branch(
        &self,
        name: &BranchName,
        from: BranchPosition,
    ) -> Result<BranchRecord, FilesystemError> {
        let mut write_state = self
            .write_state
            .lock()
            .map_err(|_| FilesystemError::Database)?;
        let branch_names = self.branch_names()?;
        if self.branch_name_exists(name)? {
            return Err(FilesystemError::Integrity);
        }
        let source = self.branch_record(from.branch_identifier())?;
        if source.status() != BranchStatus::Open
            || from.ordinal() > source.head_position().ordinal()
        {
            return Err(FilesystemError::Integrity);
        }
        let fork_sequence = if source.head_position() == from {
            source.head_sequence()
        } else {
            self.branch_sequence_at_position(from)?
                .ok_or(FilesystemError::Integrity)?
        };
        let source_root = self.branch_root_at_position(from)?;
        if source_root.sequence != fork_sequence.get() {
            return Err(FilesystemError::Integrity);
        }
        let branch_identifier = BranchIdentifier::new(write_state.next_branch_identifier);
        if self
            .database
            .get_pinned_cf(self.branches()?, encode_u64(branch_identifier.get()))
            .map_err(|_| FilesystemError::Database)?
            .is_some()
        {
            return Err(FilesystemError::Integrity);
        }
        let next_branch_identifier = write_state
            .next_branch_identifier
            .checked_add(1)
            .ok_or(FilesystemError::Integrity)?;
        let stored = StoredBranch {
            identifier: branch_identifier.get(),
            name: name.as_str().to_owned(),
            status: BranchStatus::Open,
            head_sequence: fork_sequence.get(),
            head_ordinal: from.ordinal(),
            namespace_identifier: source_root.namespace_identifier.clone(),
            fork_branch_identifier: Some(from.branch_identifier().get()),
            fork_ordinal: Some(from.ordinal()),
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.branches()?,
            encode_u64(branch_identifier.get()),
            encode_branch(&stored)?,
        );
        batch.put_cf(
            branch_names,
            name.as_str().as_bytes(),
            encode_u64(branch_identifier.get()),
        );
        self.put_branch_root(
            &mut batch,
            BranchPosition::new(branch_identifier, from.ordinal()),
            fork_sequence,
            &source_root.namespace_identifier,
        )?;
        self.put_metadata_u64(
            &mut batch,
            METADATA_KEY_NEXT_BRANCH_IDENTIFIER,
            next_branch_identifier,
        )
        .map_err(|_| FilesystemError::Database)?;
        write_batch(&self.database, batch)?;
        write_state.next_branch_identifier = next_branch_identifier;
        branch_record_from_stored(stored)
    }

    pub(crate) fn switch_branch(&self, name: &BranchName) -> Result<BranchRecord, FilesystemError> {
        let mut write_state = self
            .write_state
            .lock()
            .map_err(|_| FilesystemError::Database)?;
        let branch_identifier = self.branch_identifier_by_name(name)?;
        let stored = self.stored_branch(branch_identifier)?;
        if stored.status != BranchStatus::Open {
            return Err(FilesystemError::Integrity);
        }
        let namespace_root = namespace_root_in_database(&self.database, &stored.namespace_identifier)?;
        let namespace = self.namespace_state(&stored.namespace_identifier)?;
        let record = branch_record_from_stored(stored.clone())?;
        let mut batch = WriteBatch::default();
        self.put_metadata_u64(
            &mut batch,
            METADATA_KEY_ACTIVE_BRANCH_IDENTIFIER,
            branch_identifier.get(),
        )
        .map_err(|_| FilesystemError::Database)?;
        write_batch(&self.database, batch)?;
        write_state.active_branch_identifier = branch_identifier.get();
        write_state.active_branch_head_sequence = record.head_sequence().get();
        write_state.active_branch_head_ordinal = record.head_position().ordinal();
        *self
            .active_branch_state
            .lock()
            .map_err(|_| FilesystemError::Database)? =
            ActiveBranchState::new(stored, namespace_root, namespace);
        self.active_branch_identifier
            .store(branch_identifier.get(), Ordering::SeqCst);
        Ok(record)
    }

    pub(crate) fn delete_branch(&self, name: &BranchName) -> Result<(), FilesystemError> {
        let write_state = self
            .write_state
            .lock()
            .map_err(|_| FilesystemError::Database)?;
        let branch_identifier = self.branch_identifier_by_name(name)?;
        if branch_identifier == BRANCH_IDENTIFIER_INITIAL
            || branch_identifier.get() == write_state.active_branch_identifier
        {
            return Err(FilesystemError::Integrity);
        }
        let mut stored = self.stored_branch(branch_identifier)?;
        if stored.identifier != branch_identifier.get()
            || stored.name != name.as_str()
            || stored.status != BranchStatus::Open
        {
            return Err(FilesystemError::Integrity);
        }
        stored.status = BranchStatus::Deleted;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.branches()?,
            encode_u64(branch_identifier.get()),
            encode_branch(&stored)?,
        );
        batch.delete_cf(self.branch_names()?, name.as_str().as_bytes());
        let result = write_batch(&self.database, batch);
        drop(write_state);
        result
    }

    pub(crate) fn list_file_events(
        &self,
        file_identifier: FileIdentifier,
        after: Option<EventSequence>,
        limit: EventPageLimit,
    ) -> Result<EventPage, FilesystemError> {
        let branch = self.active_branch_identifier();
        let branch_file_events = self.branch_file_events()?;
        let (start, minimum_sequence): (u64, Option<EventSequence>) = match after {
            Some(sequence) if sequence.get() == u64::MAX => {
                return Ok(EventPage::new(Vec::new(), None));
            }
            Some(sequence) => match self.event(sequence)? {
                Some(event) if event.branch_identifier() == Some(branch) => {
                    let position = event.branch_position().ok_or(FilesystemError::Integrity)?;
                    if position.branch_identifier() != branch {
                        return Err(FilesystemError::Integrity);
                    }
                    (
                        position
                            .ordinal()
                            .checked_add(1)
                            .ok_or(FilesystemError::Integrity)?,
                        None,
                    )
                }
                _ => (0, Some(sequence)),
            },
            None => (0, None),
        };
        let prefix = encode_branch_file_prefix(branch.get(), file_identifier.get());
        let snapshot = self.database.snapshot();
        let mut iterator = snapshot.raw_iterator_cf(branch_file_events);
        let mut records = Vec::new();
        let mut next_after = None;

        iterator.seek(encode_branch_file_position_key(
            branch.get(),
            file_identifier.get(),
            start,
        ));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let (key_branch, key_file, ordinal) = decode_branch_file_position_key(key)?;
            if key_branch != branch.get() || key_file != file_identifier.get() {
                return Err(FilesystemError::Integrity);
            }
            let sequence = decode_u64(iterator.value().ok_or(FilesystemError::Database)?)
                .map(EventSequence::new)?;
            let record = self.event(sequence)?.ok_or(FilesystemError::Integrity)?;
            validate_branch_file_event_record(&record, branch, file_identifier, ordinal)?;
            self.validate_file_event_index(file_identifier, sequence)?;
            if minimum_sequence.is_some_and(|after| sequence <= after) {
                iterator.next();
                continue;
            }
            if records.len() == limit.get() as usize {
                next_after = records.last().map(EventRecord::sequence);
                break;
            }
            records.push(record);
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;
        Ok(EventPage::new(records, next_after))
    }

    pub(crate) fn list_branch_events(
        &self,
        branch: BranchIdentifier,
        after: Option<BranchPosition>,
        limit: EventPageLimit,
    ) -> Result<BranchEventPage, FilesystemError> {
        let branch_events = self.branch_events()?;
        let start = branch_position_start(branch, after)?;
        let prefix = encode_u64(branch.get());
        let snapshot = self.database.snapshot();
        let mut iterator = snapshot.raw_iterator_cf(branch_events);
        let mut records = Vec::new();
        let mut next_after = None;

        iterator.seek(encode_branch_position_key(branch.get(), start));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if records.len() == limit.get() as usize {
                next_after = records.last().and_then(EventRecord::branch_position);
                break;
            }
            let ordinal = decode_branch_position_ordinal(key)?;
            let sequence = decode_u64(iterator.value().ok_or(FilesystemError::Database)?)
                .map(EventSequence::new)?;
            let record = self.event(sequence)?.ok_or(FilesystemError::Integrity)?;
            validate_branch_event_record(&record, branch, ordinal)?;
            records.push(record);
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;

        Ok(BranchEventPage::new(records, next_after))
    }

    pub(crate) fn list_branch_file_events(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        after: Option<BranchPosition>,
        limit: EventPageLimit,
    ) -> Result<BranchEventPage, FilesystemError> {
        let branch_file_events = self.branch_file_events()?;
        let start = branch_position_start(branch, after)?;
        let prefix = encode_branch_file_prefix(branch.get(), file_identifier.get());
        let snapshot = self.database.snapshot();
        let mut iterator = snapshot.raw_iterator_cf(branch_file_events);
        let mut records = Vec::new();
        let mut next_after = None;

        iterator.seek(encode_branch_file_position_key(
            branch.get(),
            file_identifier.get(),
            start,
        ));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if records.len() == limit.get() as usize {
                next_after = records.last().and_then(EventRecord::branch_position);
                break;
            }
            let (key_branch, key_file, ordinal) = decode_branch_file_position_key(key)?;
            if key_branch != branch.get() || key_file != file_identifier.get() {
                return Err(FilesystemError::Integrity);
            }
            let sequence = decode_u64(iterator.value().ok_or(FilesystemError::Database)?)
                .map(EventSequence::new)?;
            let record = self.event(sequence)?.ok_or(FilesystemError::Integrity)?;
            validate_branch_file_event_record(&record, branch, file_identifier, ordinal)?;
            records.push(record);
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;

        Ok(BranchEventPage::new(records, next_after))
    }

    pub(crate) fn file_snapshot_at_or_before(
        &self,
        file_identifier: FileIdentifier,
        sequence: EventSequence,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        self.file_snapshot_metadata_for_sequence_at_or_before(
            self.active_branch_identifier(),
            file_identifier,
            sequence,
        )
    }

    pub(crate) fn file_snapshot_on_branch_at_or_before(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        position: BranchPosition,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        if position.branch_identifier() != branch {
            return Err(FilesystemError::Integrity);
        }
        self.file_snapshot_metadata_at_or_before(branch, file_identifier, position)
    }

    pub(crate) fn read_file_snapshot_range(
        &self,
        snapshot: &FileSnapshot,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        self.read_extent_range(
            ExtentSet::Snapshot {
                branch: snapshot.branch_position().branch_identifier(),
                file_identifier: snapshot.file_identifier(),
                ordinal: snapshot.branch_position().ordinal(),
            },
            offset,
            length.min(snapshot.file_size().saturating_sub(offset)),
        )
    }

    pub(crate) fn read_file_event_payload_range(
        &self,
        sequence: EventSequence,
        part: FileEventPayloadPart,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        let Some(event) = self.event(sequence)? else {
            return Ok(Vec::new());
        };
        let payload_length = event_payload_part_length(&event, part).unwrap_or(0);
        if offset >= payload_length {
            return Ok(Vec::new());
        }
        self.read_extent_range(
            ExtentSet::EventPayload {
                sequence,
                part: event_payload_part_byte(part),
            },
            offset,
            length.min(payload_length - offset),
        )
    }

    pub(crate) fn lookup(&self, parent: u64, name: &OsStr) -> FuseResult<EntrySnapshot> {
        let entry = self
            .directory_entry(parent, name.as_bytes())
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        self.entry_snapshot(entry.inode)
    }

    pub(crate) fn getattr(&self, inode: u64) -> FuseResult<EntrySnapshot> {
        self.entry_snapshot(inode)
    }

    pub(crate) fn setattr_metadata(
        &self,
        inode_number: u64,
        metadata: SetattrMetadata,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        validate_metadata_change(&inode, metadata)?;

        let now = StoredTime::now();
        let original_inode = inode.clone();
        if let Some(mode) = metadata.mode {
            inode.mode = permission_mode(mode, 0);
        }
        if let Some(uid) = metadata.uid {
            inode.uid = uid;
        }
        if let Some(gid) = metadata.gid {
            inode.gid = gid;
        }
        if metadata.uid.is_some() || metadata.gid.is_some() {
            inode.mode &= !(setuid_mode_bit() | setgid_mode_bit());
        }
        if let Some(atime) = metadata.atime {
            inode.atime = StoredTime::from_system_time(atime);
        }
        if let Some(mtime) = metadata.mtime {
            inode.mtime = StoredTime::from_system_time(mtime);
        }

        let size_changed = metadata
            .size
            .is_some_and(|size| size != original_inode.size);
        if size_changed {
            if inode.kind != StoredNodeKind::RegularFile {
                return Err(FuseError::Errno(fuser::Errno::EINVAL));
            }
            inode.size = original_inode.size;
            inode.content_manifest_identifier = original_inode.content_manifest_identifier.clone();
            if metadata.mtime.is_none() {
                inode.mtime = now;
            }
            inode.ctime = now;
            return self.commit_file_size_change(
                &mut write_state,
                inode_number,
                inode,
                metadata
                    .size
                    .expect("size changed only when size is present"),
            );
        }

        if inode == original_inode {
            return Ok(EntrySnapshot {
                ttl: FUSE_ATTRIBUTE_TTL,
                attr: inode_attr(inode_number, &inode),
            });
        }

        let event_sequence = write_state.next_event_sequence()?;
        inode.ctime = now;
        let mut batch = WriteBatch::default();
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let event = EventRecord::new(
            event_sequence,
            EventKind::MetadataChanged,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, inode.kind),
            self.active_inode_event_path(inode_number, &inode).ok(),
            None,
            None,
        );
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    pub(crate) fn readlink(&self, inode: u64) -> FuseResult<Vec<u8>> {
        let inode = self
            .inode(inode)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::Symlink {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        Ok(inode.symlink_target.unwrap_or_default())
    }

    #[cfg(test)]
    pub(crate) fn create_node(
        &self,
        parent: u64,
        name: &OsStr,
        kind: CreateNodeKind,
    ) -> FuseResult<EntrySnapshot> {
        self.create_node_with_metadata(
            parent,
            name,
            kind,
            CreateNodeMetadata::default_for_kind(kind),
        )
    }

    pub(crate) fn create_node_with_metadata(
        &self,
        parent: u64,
        name: &OsStr,
        kind: CreateNodeKind,
        metadata: CreateNodeMetadata,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut parent_inode = self.require_directory(parent)?;
        let name_bytes = validate_name(name)?;
        if self
            .directory_entry(parent, &name_bytes)
            .map_err(to_fuse_integrity)?
            .is_some()
        {
            return Err(FuseError::Errno(fuser::Errno::EEXIST));
        }

        let inode_number = write_state.next_inode_number()?;
        if self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .is_some()
        {
            return Err(FuseError::Integrity);
        }
        let event_sequence = write_state.next_event_sequence()?;
        let next_inode_number = next_inode_number_after(inode_number)?;
        let now = StoredTime::now();
        let path = self.active_child_event_path(parent, &name_bytes)?;
        let (stored_kind, rdev, size, symlink_target, event_kind) = match kind {
            CreateNodeKind::Directory => (
                StoredNodeKind::Directory,
                0,
                0,
                None,
                EventKind::DirectoryCreated,
            ),
            CreateNodeKind::RegularFile => (
                StoredNodeKind::RegularFile,
                0,
                0,
                None,
                EventKind::FileCreated,
            ),
            CreateNodeKind::Special { mode, rdev } => (
                special_kind_from_mode(mode)?,
                rdev,
                0,
                None,
                EventKind::NodeCreated,
            ),
        };
        let (uid, gid, mode) = created_inode_metadata(&parent_inode, stored_kind, metadata);
        let inode = StoredInode {
            kind: stored_kind,
            size,
            nlink: match stored_kind {
                StoredNodeKind::Directory => 2,
                _ => 1,
            },
            uid,
            gid,
            mode,
            rdev,
            atime: now,
            mtime: now,
            ctime: now,
            parent,
            name: name_bytes.clone(),
            symlink_target,
            content_manifest_identifier: if stored_kind == StoredNodeKind::RegularFile {
                Some(empty_content_manifest_identifier().map_err(to_fuse_integrity)?)
            } else {
                None
            },
        };
        let event = EventRecord::new(
            event_sequence,
            event_kind,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, stored_kind),
            Some(path),
            None,
            None,
        );

        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let mut batch = WriteBatch::default();
        let entry = StoredDirectoryEntry {
            inode: inode_number,
            kind: stored_kind,
        };
        mutation.put_directory_entry(parent, &name_bytes, &entry);
        if stored_kind == StoredNodeKind::Directory {
            parent_inode.nlink = parent_inode.nlink.saturating_add(1);
        }
        touch_loaded_directory(&mut parent_inode, now);
        mutation.put_inode(parent, &parent_inode);
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.put_metadata_u64(
            &mut batch,
            METADATA_KEY_NEXT_INODE_NUMBER,
            next_inode_number,
        )?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            Some(next_inode_number),
            active_commit,
        )?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    #[cfg(test)]
    pub(crate) fn create_symlink(
        &self,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
    ) -> FuseResult<EntrySnapshot> {
        self.create_symlink_with_metadata(
            parent,
            link_name,
            target,
            CreateNodeMetadata {
                uid: 0,
                gid: 0,
                mode: u32::from(INODE_MODE_DEFAULT_SYMLINK),
                umask: 0,
            },
        )
    }

    pub(crate) fn create_symlink_with_metadata(
        &self,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        metadata: CreateNodeMetadata,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut parent_inode = self.require_directory(parent)?;
        let name_bytes = validate_name(link_name)?;
        if self
            .directory_entry(parent, &name_bytes)
            .map_err(to_fuse_integrity)?
            .is_some()
        {
            return Err(FuseError::Errno(fuser::Errno::EEXIST));
        }

        let inode_number = write_state.next_inode_number()?;
        if self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .is_some()
        {
            return Err(FuseError::Integrity);
        }
        let event_sequence = write_state.next_event_sequence()?;
        let next_inode_number = next_inode_number_after(inode_number)?;
        let now = StoredTime::now();
        let path = self.active_child_event_path(parent, &name_bytes)?;
        let target_bytes = target.as_os_str().as_bytes().to_vec();
        let (uid, gid, mode) =
            created_inode_metadata(&parent_inode, StoredNodeKind::Symlink, metadata);
        let inode = StoredInode {
            kind: StoredNodeKind::Symlink,
            size: target_bytes.len() as u64,
            nlink: 1,
            uid,
            gid,
            mode,
            rdev: 0,
            atime: now,
            mtime: now,
            ctime: now,
            parent,
            name: name_bytes.clone(),
            symlink_target: Some(target_bytes),
            content_manifest_identifier: None,
        };
        let event = EventRecord::new(
            event_sequence,
            EventKind::SymbolicLinkCreated,
            UtcDateTime::now(),
            None,
            Some(path),
            None,
            None,
        );

        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let mut batch = WriteBatch::default();
        let entry = StoredDirectoryEntry {
            inode: inode_number,
            kind: StoredNodeKind::Symlink,
        };
        mutation.put_directory_entry(parent, &name_bytes, &entry);
        touch_loaded_directory(&mut parent_inode, now);
        mutation.put_inode(parent, &parent_inode);
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.put_metadata_u64(
            &mut batch,
            METADATA_KEY_NEXT_INODE_NUMBER,
            next_inode_number,
        )?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            Some(next_inode_number),
            active_commit,
        )?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    pub(crate) fn unlink(&self, parent: u64, name: &OsStr) -> FuseResult<()> {
        self.remove_directory_entry(parent, name, false)
    }

    pub(crate) fn rmdir(&self, parent: u64, name: &OsStr) -> FuseResult<()> {
        self.remove_directory_entry(parent, name, true)
    }

    pub(crate) fn rename(
        &self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        no_replace: bool,
    ) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let name_bytes = validate_name(name)?;
        let new_name_bytes = validate_name(new_name)?;
        let mut old_parent_inode = self.require_directory(parent)?;
        let mut new_parent_inode = if new_parent == parent {
            None
        } else {
            Some(self.require_directory(new_parent)?)
        };
        let entry = self
            .directory_entry(parent, &name_bytes)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let mut inode = self
            .inode(entry.inode)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if entry.kind == StoredNodeKind::Directory
            && self.directory_is_self_or_descendant(entry.inode, new_parent)?
        {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        let existing_destination = self
            .directory_entry(new_parent, &new_name_bytes)
            .map_err(to_fuse_integrity)?;
        if no_replace && existing_destination.is_some() {
            return Err(FuseError::Errno(fuser::Errno::EEXIST));
        }

        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        let new_path = self.active_child_event_path(new_parent, &new_name_bytes)?;
        let mut batch = WriteBatch::default();
        let mut removed_destination = None;

        if let Some(destination) =
            existing_destination.filter(|destination| destination.inode != entry.inode)
        {
            let destination_inode = self
                .inode(destination.inode)
                .map_err(to_fuse_integrity)?
                .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
            if destination_inode.kind == StoredNodeKind::Directory {
                if entry.kind != StoredNodeKind::Directory {
                    return Err(FuseError::Errno(fuser::Errno::EISDIR));
                }
                if !self.directory_is_empty(destination.inode)? {
                    return Err(FuseError::Errno(fuser::Errno::ENOTEMPTY));
                }
            } else if entry.kind == StoredNodeKind::Directory {
                return Err(FuseError::Errno(fuser::Errno::ENOTDIR));
            }
            removed_destination = Some((destination.clone(), destination_inode.clone()));
            let destination_parent_inode = if new_parent == parent {
                &mut old_parent_inode
            } else {
                new_parent_inode.as_mut().ok_or(FuseError::Integrity)?
            };
            update_parent_after_entry_removal(&destination_inode, destination_parent_inode, now);
        }

        if entry.kind == StoredNodeKind::Directory && parent != new_parent {
            old_parent_inode.nlink = old_parent_inode.nlink.saturating_sub(1);
            let new_parent_inode = new_parent_inode.as_mut().ok_or(FuseError::Integrity)?;
            new_parent_inode.nlink = new_parent_inode.nlink.saturating_add(1);
        }
        inode.parent = new_parent;
        inode.name = new_name_bytes.clone();
        inode.ctime = now;
        touch_loaded_directory(&mut old_parent_inode, now);
        if let Some(new_parent_inode) = &mut new_parent_inode {
            touch_loaded_directory(new_parent_inode, now);
        }

        let event = EventRecord::new(
            event_sequence,
            EventKind::NodeRenamed,
            UtcDateTime::now(),
            file_identifier_for_kind(entry.inode, entry.kind),
            Some(new_path),
            None,
            None,
        );
        let mut mutation = StoredMutation::default();
        mutation.delete_directory_entry(parent, &name_bytes);
        if let Some((destination, destination_inode)) = removed_destination {
            mutation.delete_directory_entry(new_parent, &new_name_bytes);
            if destination_inode.kind == StoredNodeKind::Directory || destination_inode.nlink <= 1 {
                mutation.delete_inode(destination.inode);
            } else {
                let mut destination_inode = destination_inode;
                destination_inode.nlink = destination_inode.nlink.saturating_sub(1);
                destination_inode.ctime = now;
                mutation.put_inode(destination.inode, &destination_inode);
            }
        }
        mutation.put_directory_entry(new_parent, &new_name_bytes, &entry);
        mutation.put_inode(entry.inode, &inode);
        mutation.put_inode(parent, &old_parent_inode);
        if let Some(new_parent_inode) = &new_parent_inode {
            mutation.put_inode(new_parent, new_parent_inode);
        }
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )
    }

    pub(crate) fn hard_link(
        &self,
        inode_number: u64,
        new_parent: u64,
        new_name: &OsStr,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind == StoredNodeKind::Directory {
            return Err(FuseError::Errno(fuser::Errno::EPERM));
        }
        let mut parent_inode = self.require_directory(new_parent)?;
        let name_bytes = validate_name(new_name)?;
        if self
            .directory_entry(new_parent, &name_bytes)
            .map_err(to_fuse_integrity)?
            .is_some()
        {
            return Err(FuseError::Errno(fuser::Errno::EEXIST));
        }

        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        inode.nlink = inode.nlink.saturating_add(1);
        inode.ctime = now;
        let path = self.active_child_event_path(new_parent, &name_bytes)?;
        let mut batch = WriteBatch::default();
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let entry = StoredDirectoryEntry {
            inode: inode_number,
            kind: inode.kind,
        };
        mutation.put_directory_entry(new_parent, &name_bytes, &entry);
        touch_loaded_directory(&mut parent_inode, now);
        mutation.put_inode(new_parent, &parent_inode);
        let event = EventRecord::new(
            event_sequence,
            EventKind::HardLinkCreated,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, inode.kind),
            Some(path),
            None,
            None,
        );
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    pub(crate) fn open_file(&self, inode_number: u64, truncate: bool) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        if truncate && inode.size != 0 {
            let now = StoredTime::now();
            inode.mtime = now;
            inode.ctime = now;
            self.commit_file_size_change(&mut write_state, inode_number, inode, 0)?;
        }
        Ok(())
    }

    pub(crate) fn read_file(
        &self,
        inode_number: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<Vec<u8>> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        if offset >= inode.size || size == 0 {
            return Ok(Vec::new());
        }
        let readable = u64::from(size).min(inode.size - offset) as usize;
        self.read_extent_range(
            ExtentSet::Current {
                branch: self.active_branch_identifier(),
                file_identifier: FileIdentifier::new(inode_number),
            },
            offset,
            readable as u64,
        )
        .map_err(to_fuse_integrity)
    }

    #[cfg(test)]
    pub(crate) fn write_file(
        &self,
        inode_number: u64,
        offset: u64,
        data: &[u8],
    ) -> FuseResult<FuseWrite> {
        self.write_file_with_metadata(inode_number, offset, data, false, false)
    }

    pub(crate) fn write_file_with_metadata(
        &self,
        inode_number: u64,
        offset: u64,
        data: &[u8],
        append: bool,
        clear_suid_sgid: bool,
    ) -> FuseResult<FuseWrite> {
        // Keep writes proportional to the changed byte range: appends never
        // materialize the old manifest, and overwrites read only old extents
        // needed for the overwritten payload.
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        let offset = if append { inode.size } else { offset };
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or(FuseError::Errno(fuser::Errno::EFBIG))?;
        let new_size = inode.size.max(end);
        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        let old_size = inode.size;
        let previous_content_manifest_identifier = inode
            .content_manifest_identifier
            .clone()
            .ok_or(FuseError::Integrity)?;
        let overwritten_end = end.min(old_size);
        let is_append_write = offset >= old_size;
        let written_chunks = chunk_bytes(data)?;
        let written_current_extents = stored_extents_from_pending(offset, &written_chunks)?;
        let written_payload_extents = stored_extents_from_pending(0, &written_chunks)?;
        let overwritten_extents = if is_append_write {
            Vec::new()
        } else {
            let extents =
                self.content_manifest_extents_in_range(
                    &previous_content_manifest_identifier,
                    offset,
                    overwritten_end,
                )
                .map_err(to_fuse_integrity)?;
            if offset < overwritten_end && !data.is_empty() {
                slice_extents(&extents, offset, overwritten_end, 0).map_err(to_fuse_integrity)?
            } else {
                Vec::new()
            }
        };
        self.validate_stored_extents(&overwritten_extents)?;
        let mut batch = WriteBatch::default();
        self.put_pending_extents(
            &mut batch,
            ExtentSet::Current {
                branch: self.active_branch_identifier(),
                file_identifier: FileIdentifier::new(inode_number),
            },
            data,
            &written_current_extents,
            &written_chunks,
        )?;
        let content_manifest_identifier = if is_append_write {
            self.put_appended_content_manifest(
                &mut batch,
                &previous_content_manifest_identifier,
                old_size,
                &written_current_extents,
            )?
        } else {
            self.put_replaced_content_manifest(
                &mut batch,
                &previous_content_manifest_identifier,
                old_size,
                offset,
                end,
                &written_current_extents,
            )?
        };
        inode.size = new_size;
        inode.content_manifest_identifier = Some(content_manifest_identifier.clone());
        inode.mtime = now;
        inode.ctime = now;
        if clear_suid_sgid {
            inode.mode &= !(setuid_mode_bit() | setgid_mode_bit());
        }
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);

        let event = EventRecord::new(
            event_sequence,
            EventKind::FileWritten,
            UtcDateTime::now(),
            Some(FileIdentifier::new(inode_number)),
            self.active_inode_event_path(inode_number, &inode).ok(),
            Some(offset),
            Some(data.len() as u64),
        )
        .with_payload(EventPayload::FileWrite {
            old_file_size: old_size,
            new_file_size: new_size,
            overwritten_byte_length: overwritten_end.saturating_sub(offset),
            written_byte_length: data.len() as u64,
        });
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit = self.commit_event_with_payloads(
            &mut batch,
            &active_state,
            event_sequence,
            &event,
            if is_append_write {
                CommitEventPayloads::file_write_append(
                    overwritten_extents,
                    written_payload_extents,
                    &content_manifest_identifier,
                )
            } else {
                CommitEventPayloads::file_write_without_extent_update(
                    overwritten_extents,
                    written_payload_extents,
                    &content_manifest_identifier,
                )
            },
            mutation,
        )?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )?;

        Ok(FuseWrite {
            written: data.len() as u32,
        })
    }

    pub(crate) fn opendir(&self, inode_number: u64) -> FuseResult<()> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::Directory {
            return Err(FuseError::Errno(fuser::Errno::ENOTDIR));
        }
        Ok(())
    }

    pub(crate) fn readdir<F>(&self, inode_number: u64, offset: u64, mut emit: F) -> FuseResult<()>
    where
        F: FnMut(DirectoryEntrySnapshot) -> bool,
    {
        let active = self.lock_active_branch_state()?;
        let inode = active
            .namespace
            .inodes
            .get(&inode_number)
            .cloned()
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::Directory {
            return Err(FuseError::Errno(fuser::Errno::ENOTDIR));
        }

        if offset == 0
            && !emit(DirectoryEntrySnapshot {
                inode: inode_number,
                offset: 1,
                kind: fuser::FileType::Directory,
                name: OsString::from("."),
            })
        {
            return Ok(());
        }
        if offset <= 1
            && !emit(DirectoryEntrySnapshot {
                inode: if inode_number == INODE_ROOT {
                    INODE_ROOT
                } else {
                    inode.parent
                },
                offset: 2,
                kind: fuser::FileType::Directory,
                name: OsString::from(".."),
            })
        {
            return Ok(());
        }

        for (entry_offset, ((parent, name), entry)) in (FUSE_DIRECTORY_ENTRY_FIRST_OFFSET..)
            .zip(active.namespace.directory_entries.range((inode_number, Vec::new())..))
        {
            if *parent != inode_number {
                break;
            }
            if entry_offset > offset
                && !emit(DirectoryEntrySnapshot {
                    inode: entry.inode,
                    offset: entry_offset + 1,
                    kind: fuser_file_type(entry.kind),
                    name: OsString::from_vec(name.clone()),
                })
            {
                return Ok(());
            }
        }
        Ok(())
    }

    pub(crate) fn readdirplus<F>(
        &self,
        inode_number: u64,
        offset: u64,
        mut emit: F,
    ) -> FuseResult<()>
    where
        F: FnMut(DirectoryEntryPlusSnapshot) -> bool,
    {
        let active = self.lock_active_branch_state()?;
        let inode = active
            .namespace
            .inodes
            .get(&inode_number)
            .cloned()
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::Directory {
            return Err(FuseError::Errno(fuser::Errno::ENOTDIR));
        }

        if offset == 0
            && !emit(DirectoryEntryPlusSnapshot {
                inode: inode_number,
                offset: 1,
                name: OsString::from("."),
                entry: EntrySnapshot {
                    ttl: FUSE_ATTRIBUTE_TTL,
                    attr: inode_attr(inode_number, &inode),
                },
            })
        {
            return Ok(());
        }
        if offset <= 1 {
            let parent_number = if inode_number == INODE_ROOT {
                INODE_ROOT
            } else {
                inode.parent
            };
            let parent_inode = active
                .namespace
                .inodes
                .get(&parent_number)
                .cloned()
                .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
            if !emit(DirectoryEntryPlusSnapshot {
                inode: parent_number,
                offset: 2,
                name: OsString::from(".."),
                entry: EntrySnapshot {
                    ttl: FUSE_ATTRIBUTE_TTL,
                    attr: inode_attr(parent_number, &parent_inode),
                },
            }) {
                return Ok(());
            }
        }

        for (entry_offset, ((parent, name), entry)) in (FUSE_DIRECTORY_ENTRY_FIRST_OFFSET..)
            .zip(active.namespace.directory_entries.range((inode_number, Vec::new())..))
        {
            if *parent != inode_number {
                break;
            }
            if entry_offset > offset {
                let entry_inode = active
                    .namespace
                    .inodes
                    .get(&entry.inode)
                    .cloned()
                    .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
                if !emit(DirectoryEntryPlusSnapshot {
                    inode: entry.inode,
                    offset: entry_offset + 1,
                    name: OsString::from_vec(name.clone()),
                    entry: EntrySnapshot {
                        ttl: FUSE_ATTRIBUTE_TTL,
                        attr: inode_attr(entry.inode, &entry_inode),
                    },
                }) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn access(
        &self,
        inode_number: u64,
        request_uid: u32,
        request_gid: u32,
        mask: fuser::AccessFlags,
    ) -> FuseResult<()> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if access_allowed(&inode, request_uid, request_gid, mask) {
            Ok(())
        } else {
            Err(FuseError::Errno(fuser::Errno::EACCES))
        }
    }

    pub(crate) fn statfs(&self, database_directory: &Path) -> FuseResult<FileSystemStatistics> {
        let backing = backing_filesystem_statistics(database_directory)?;
        let next_inode_number = read_metadata_u64(&self.database, METADATA_KEY_NEXT_INODE_NUMBER)
            .map_err(|_| FuseError::Integrity)?;
        statistics_from_backing(backing, next_inode_number)
    }

    pub(crate) fn synchronize(&self) -> FuseResult<()> {
        let _write_state = self.lock_write_state()?;
        self.database
            .flush_wal(true)
            .map_err(|_| FuseError::Database)
    }

    pub(crate) fn setxattr(
        &self,
        inode_number: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
    ) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        validate_extended_attribute_inode(&inode)?;
        let name_bytes = validate_extended_attribute_name(name)?;
        validate_extended_attribute_value(value)?;
        let create = flags & libc::XATTR_CREATE != 0;
        let replace = flags & libc::XATTR_REPLACE != 0;
        let supported = libc::XATTR_CREATE | libc::XATTR_REPLACE;
        if flags & !supported != 0 || (create && replace) {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        let existing = self.extended_attribute(inode_number, &name_bytes)?;
        if create && existing.is_some() {
            return Err(FuseError::Errno(fuser::Errno::EEXIST));
        }
        if replace && existing.is_none() {
            return Err(FuseError::Errno(fuser::Errno::NO_XATTR));
        }
        if existing.as_deref() == Some(value) {
            return Ok(());
        }

        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        inode.ctime = now;
        let mut batch = WriteBatch::default();
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        mutation.put_extended_attribute(inode_number, &name_bytes, value);
        let event = EventRecord::new(
            event_sequence,
            EventKind::ExtendedAttributeSet,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, inode.kind),
            self.active_inode_event_path(inode_number, &inode).ok(),
            None,
            Some(value.len() as u64),
        );
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )
    }

    pub(crate) fn getxattr(&self, inode_number: u64, name: &OsStr) -> FuseResult<Vec<u8>> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        validate_extended_attribute_inode(&inode)?;
        let name_bytes = validate_extended_attribute_name(name)?;
        self.extended_attribute(inode_number, &name_bytes)?
            .ok_or(FuseError::Errno(fuser::Errno::NO_XATTR))
    }

    pub(crate) fn listxattr(&self, inode_number: u64) -> FuseResult<ExtendedAttributeList> {
        let active = self.lock_active_branch_state()?;
        active
            .namespace
            .inodes
            .get(&inode_number)
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))
            .and_then(|inode| {
                validate_extended_attribute_inode(inode)?;
                Ok(inode)
            })?;
        let mut bytes = Vec::new();
        for ((attribute_inode, name), _value) in active
            .namespace
            .extended_attributes
            .range((inode_number, Vec::new())..)
        {
            if *attribute_inode != inode_number {
                break;
            }
            bytes.extend_from_slice(name);
            bytes.push(0);
            if bytes.len() > XATTR_LIST_MAX_BYTES {
                return Err(FuseError::Errno(fuser::Errno::E2BIG));
            }
        }
        Ok(ExtendedAttributeList { bytes })
    }

    pub(crate) fn removexattr(&self, inode_number: u64, name: &OsStr) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        validate_extended_attribute_inode(&inode)?;
        let name_bytes = validate_extended_attribute_name(name)?;
        if self
            .extended_attribute(inode_number, &name_bytes)?
            .is_none()
        {
            return Err(FuseError::Errno(fuser::Errno::NO_XATTR));
        }

        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        inode.ctime = now;
        let mut batch = WriteBatch::default();
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        mutation.delete_extended_attribute(inode_number, &name_bytes);
        let event = EventRecord::new(
            event_sequence,
            EventKind::ExtendedAttributeRemoved,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, inode.kind),
            self.active_inode_event_path(inode_number, &inode).ok(),
            None,
            None,
        );
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )
    }

    pub(crate) fn volume_name(&self) -> Result<String, FilesystemError> {
        read_metadata_string(&self.database, METADATA_KEY_VOLUME_NAME)
    }

    fn lock_write_state(&self) -> FuseResult<MutexGuard<'_, WriteState>> {
        self.write_state.lock().map_err(|_| FuseError::Database)
    }

    fn lock_active_branch_state(&self) -> FuseResult<MutexGuard<'_, ActiveBranchState>> {
        self.active_branch_state
            .lock()
            .map_err(|_| FuseError::Database)
    }

    fn active_child_event_path(&self, parent: u64, name: &[u8]) -> FuseResult<String> {
        let active = self.lock_active_branch_state()?;
        child_event_path(parent, name, &active.namespace)
    }

    fn active_inode_event_path(
        &self,
        inode_number: u64,
        inode: &StoredInode,
    ) -> FuseResult<String> {
        let active = self.lock_active_branch_state()?;
        inode_event_path(inode_number, inode, &active.namespace)
    }

    fn commit_mutation(
        &self,
        write_state: &mut WriteState,
        active_state: &mut ActiveBranchState,
        batch: WriteBatch,
        event_sequence: EventSequence,
        next_inode_number: Option<u64>,
        active_commit: ActiveBranchCommit,
    ) -> FuseResult<()> {
        write_batch_with_durability(&self.database, batch, WriteDurability::Buffered)
            .map_err(|_| FuseError::Database)?;
        write_state.record_committed_event(event_sequence);
        if let Some(next_inode_number) = next_inode_number {
            write_state.record_committed_inode_number(next_inode_number);
        }
        active_state.apply_commit(active_commit);
        Ok(())
    }

    fn commit_file_size_change(
        &self,
        write_state: &mut WriteState,
        inode_number: u64,
        mut inode: StoredInode,
        new_size: u64,
    ) -> FuseResult<EntrySnapshot> {
        // File growth is metadata-only for content; shrinking touches only the
        // discarded range needed for the overwritten payload.
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        let old_size = inode.size;
        if old_size == new_size {
            return Ok(EntrySnapshot {
                ttl: FUSE_ATTRIBUTE_TTL,
                attr: inode_attr(inode_number, &inode),
            });
        }

        let previous_content_manifest_identifier = inode
            .content_manifest_identifier
            .clone()
            .ok_or(FuseError::Integrity)?;
        let (offset, overwritten_byte_length, overwritten_extents) = if new_size < old_size {
            let extents = self
                .content_manifest_extents_in_range(
                    &previous_content_manifest_identifier,
                    new_size,
                    old_size,
                )
                .map_err(to_fuse_integrity)?;
            (
                new_size,
                old_size - new_size,
                slice_extents(&extents, new_size, old_size, 0).map_err(to_fuse_integrity)?,
            )
        } else {
            (old_size, 0, Vec::new())
        };
        self.validate_stored_extents(&overwritten_extents)?;

        let event_sequence = write_state.next_event_sequence()?;
        let mut batch = WriteBatch::default();
        let content_manifest_identifier = if new_size > old_size {
            previous_content_manifest_identifier
        } else {
            self.put_content_manifest_range(
                &mut batch,
                &previous_content_manifest_identifier,
                0,
                new_size,
            )?
        };
        inode.size = new_size;
        inode.content_manifest_identifier = Some(content_manifest_identifier.clone());
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);

        let event = EventRecord::new(
            event_sequence,
            EventKind::FileWritten,
            UtcDateTime::now(),
            Some(FileIdentifier::new(inode_number)),
            self.active_inode_event_path(inode_number, &inode).ok(),
            Some(offset),
            Some(0),
        )
        .with_payload(EventPayload::FileWrite {
            old_file_size: old_size,
            new_file_size: new_size,
            overwritten_byte_length,
            written_byte_length: 0,
        });
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit = self.commit_event_with_payloads(
            &mut batch,
            &active_state,
            event_sequence,
            &event,
            if new_size > old_size {
                CommitEventPayloads::file_write_without_extent_update(
                    overwritten_extents,
                    Vec::new(),
                    &content_manifest_identifier,
                )
            } else {
                CommitEventPayloads::file_write_without_extent_update(
                    overwritten_extents,
                    Vec::new(),
                    &content_manifest_identifier,
                )
            },
            mutation,
        )?;
        self.commit_mutation(
            &mut *write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    fn remove_directory_entry(&self, parent: u64, name: &OsStr, directory: bool) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let name_bytes = validate_name(name)?;
        let mut parent_inode = self.require_directory(parent)?;
        let entry = self
            .directory_entry(parent, &name_bytes)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let inode = self
            .inode(entry.inode)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;

        if directory {
            if inode.kind != StoredNodeKind::Directory {
                return Err(FuseError::Errno(fuser::Errno::ENOTDIR));
            }
            if !self.directory_is_empty(entry.inode)? {
                return Err(FuseError::Errno(fuser::Errno::ENOTEMPTY));
            }
        } else if inode.kind == StoredNodeKind::Directory {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }

        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        let inode_before = inode.clone();
        let path = self.active_child_event_path(parent, &name_bytes)?;
        let mut batch = WriteBatch::default();
        update_parent_after_entry_removal(&inode, &mut parent_inode, now);
        let event = EventRecord::new(
            event_sequence,
            if directory {
                EventKind::DirectoryRemoved
            } else {
                EventKind::NodeUnlinked
            },
            UtcDateTime::now(),
            file_identifier_for_kind(entry.inode, entry.kind),
            Some(path),
            None,
            None,
        );
        let mut mutation = StoredMutation::default();
        mutation.delete_directory_entry(parent, &name_bytes);
        mutation.put_inode(parent, &parent_inode);
        if inode_before.kind == StoredNodeKind::Directory || inode_before.nlink <= 1 {
            mutation.delete_inode(entry.inode);
        } else {
            let mut linked_inode = inode_before;
            linked_inode.nlink = linked_inode.nlink.saturating_sub(1);
            linked_inode.ctime = now;
            mutation.put_inode(entry.inode, &linked_inode);
        }
        let mut active_state = self.lock_active_branch_state()?;
        let active_commit =
            self.commit_event(&mut batch, &active_state, event_sequence, &event, mutation)?;
        self.commit_mutation(
            &mut write_state,
            &mut active_state,
            batch,
            event_sequence,
            None,
            active_commit,
        )
    }

    fn entry_snapshot(&self, inode_number: u64) -> FuseResult<EntrySnapshot> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    fn require_directory(&self, inode_number: u64) -> FuseResult<StoredInode> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind == StoredNodeKind::Directory {
            Ok(inode)
        } else {
            Err(FuseError::Errno(fuser::Errno::ENOTDIR))
        }
    }

    fn directory_is_empty(&self, inode_number: u64) -> FuseResult<bool> {
        let active = self.lock_active_branch_state()?;
        Ok(!active
            .namespace
            .directory_entries
            .keys()
            .any(|(parent, _name)| *parent == inode_number))
    }

    fn directory_is_self_or_descendant(
        &self,
        ancestor: u64,
        mut inode_number: u64,
    ) -> FuseResult<bool> {
        for _ in 0..INODE_LOGICAL_CAPACITY.min(1024) {
            if inode_number == ancestor {
                return Ok(true);
            }
            if inode_number == INODE_ROOT {
                return Ok(false);
            }
            let inode = self
                .inode(inode_number)
                .map_err(to_fuse_integrity)?
                .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
            inode_number = inode.parent;
        }
        Err(FuseError::Integrity)
    }

    fn inode(&self, inode_number: u64) -> Result<Option<StoredInode>, FilesystemError> {
        Ok(self
            .active_branch_state
            .lock()
            .map_err(|_| FilesystemError::Database)?
            .namespace
            .inodes
            .get(&inode_number)
            .cloned())
    }

    fn event(&self, sequence: EventSequence) -> Result<Option<EventRecord>, FilesystemError> {
        self.stored_event(sequence)
            .map(|event| event.map(|event| event.record))
    }

    fn stored_event(
        &self,
        sequence: EventSequence,
    ) -> Result<Option<StoredEvent>, FilesystemError> {
        let events = self.events()?;
        let key = encode_u64(sequence.get());
        self.database
            .get_pinned_cf(events, key)
            .map_err(|_| FilesystemError::Database)?
            .map(|value| decode_stored_event_at_key(&key, &value))
            .transpose()
    }

    fn validate_file_event_index(
        &self,
        file_identifier: FileIdentifier,
        sequence: EventSequence,
    ) -> Result<(), FilesystemError> {
        let file_events = self.file_events()?;
        let value = self
            .database
            .get_pinned_cf(
                file_events,
                encode_file_sequence_key(file_identifier.get(), sequence.get()),
            )
            .map_err(|_| FilesystemError::Database)?
            .ok_or(FilesystemError::Integrity)?;
        if !value.is_empty() {
            return Err(FilesystemError::Integrity);
        }
        Ok(())
    }

    fn branch_identifier_by_name(
        &self,
        name: &BranchName,
    ) -> Result<BranchIdentifier, FilesystemError> {
        let branch_names = self.branch_names()?;
        self.database
            .get_pinned_cf(branch_names, name.as_str().as_bytes())
            .map_err(|_| FilesystemError::Database)?
            .ok_or(FilesystemError::Integrity)
            .and_then(|value| decode_u64(&value))
            .map(BranchIdentifier::new)
    }

    fn branch_name_exists(&self, name: &BranchName) -> Result<bool, FilesystemError> {
        let branches = self.branches()?;
        let mut iterator = self.database.raw_iterator_cf(branches);
        iterator.seek_to_first();
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if key.len() != 8 {
                return Err(FilesystemError::Integrity);
            }
            let stored = decode_branch(iterator.value().ok_or(FilesystemError::Database)?)?;
            if stored.name == name.as_str() {
                return Ok(true);
            }
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;
        Ok(false)
    }

    fn stored_branch(
        &self,
        branch_identifier: BranchIdentifier,
    ) -> Result<StoredBranch, FilesystemError> {
        let branches = self.branches()?;
        self.database
            .get_pinned_cf(branches, encode_u64(branch_identifier.get()))
            .map_err(|_| FilesystemError::Database)?
            .ok_or(FilesystemError::Integrity)
            .and_then(|value| decode_branch(&value))
    }

    fn namespace_state_for_branch(
        &self,
        branch_identifier: BranchIdentifier,
    ) -> Result<MaterializedBranchState, FilesystemError> {
        let branch = self.stored_branch(branch_identifier)?;
        self.namespace_state(&branch.namespace_identifier)
    }

    fn namespace_state(
        &self,
        namespace_identifier: &[u8],
    ) -> Result<MaterializedBranchState, FilesystemError> {
        namespace_state_for_identifier_in_database(&self.database, namespace_identifier)
    }

    fn put_namespace_mutation(
        &self,
        batch: &mut WriteBatch,
        active_state: &ActiveBranchState,
        mutation: &StoredMutation,
    ) -> Result<(Vec<u8>, StoredNamespaceRoot), FilesystemError> {
        put_namespace_mutation_from_active_in_database(
            &self.database,
            batch,
            &active_state.namespace_root,
            &active_state.namespace,
            mutation,
        )
    }

    fn branch_record(
        &self,
        branch_identifier: BranchIdentifier,
    ) -> Result<BranchRecord, FilesystemError> {
        branch_record_from_stored(self.stored_branch(branch_identifier)?)
    }

    fn branch_sequence_at_position(
        &self,
        position: BranchPosition,
    ) -> Result<Option<EventSequence>, FilesystemError> {
        let branch_events = self.branch_events()?;
        self.database
            .get_pinned_cf(
                branch_events,
                encode_branch_position_key(position.branch_identifier().get(), position.ordinal()),
            )
            .map_err(|_| FilesystemError::Database)?
            .map(|value| decode_u64(&value).map(EventSequence::new))
            .transpose()
    }

    fn file_snapshot_metadata_at_or_before(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        position: BranchPosition,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        let manifests = self.file_snapshot_manifests()?;
        let prefix = encode_snapshot_file_prefix(branch.get(), file_identifier.get());
        let mut iterator = self.database.raw_iterator_cf(manifests);
        iterator.seek_for_prev(encode_file_snapshot_manifest_key(
            branch.get(),
            file_identifier.get(),
            position.ordinal(),
        ));
        if iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if key.starts_with(&prefix) {
                let (key_branch, key_file, ordinal) = decode_file_snapshot_manifest_key(key)?;
                if key_branch != branch.get() || key_file != file_identifier.get() {
                    return Err(FilesystemError::Integrity);
                }
                let metadata =
                    decode_snapshot_metadata(iterator.value().ok_or(FilesystemError::Database)?)?;
                return Ok(Some(FileSnapshot::new(
                    file_identifier,
                    EventSequence::new(metadata.sequence),
                    BranchPosition::new(branch, ordinal),
                    metadata.file_size,
                )));
            }
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;

        self.branch_root_snapshot_at_or_before_position(file_identifier, position)
    }

    fn snapshot_metadata_from_branch_root(
        &self,
        file_identifier: FileIdentifier,
        root: &StoredBranchRoot,
    ) -> Result<Option<StoredSnapshotMetadata>, FilesystemError> {
        let state = self.namespace_state(&root.namespace_identifier)?;
        let Some(inode) = state.inodes.get(&file_identifier.get()) else {
            return Ok(None);
        };
        if inode.kind != StoredNodeKind::RegularFile {
            return Ok(None);
        }
        Ok(Some(StoredSnapshotMetadata {
            file_size: inode.size,
            sequence: root.sequence,
            content_manifest_identifier: inode
                .content_manifest_identifier
                .clone()
                .unwrap_or(empty_content_manifest_identifier()?),
        }))
    }

    fn branch_root_snapshot_at_or_before_position(
        &self,
        file_identifier: FileIdentifier,
        position: BranchPosition,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        let branch_roots = self.branch_roots()?;
        let prefix = encode_u64(position.branch_identifier().get());
        let mut iterator = self.database.raw_iterator_cf(branch_roots);
        iterator.seek_for_prev(encode_branch_position_key(
            position.branch_identifier().get(),
            position.ordinal(),
        ));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let ordinal = decode_branch_position_ordinal(key)?;
            let root = decode_branch_root(iterator.value().ok_or(FilesystemError::Database)?)?;
            if let Some(metadata) =
                self.snapshot_metadata_from_branch_root(file_identifier, &root)?
            {
                return Ok(Some(FileSnapshot::new(
                    file_identifier,
                    EventSequence::new(metadata.sequence),
                    BranchPosition::new(position.branch_identifier(), ordinal),
                    metadata.file_size,
                )));
            }
            iterator.prev();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;
        Ok(None)
    }

    fn branch_root_snapshot_at_or_before_sequence(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        sequence: EventSequence,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        let branch_roots = self.branch_roots()?;
        let prefix = encode_u64(branch.get());
        let mut iterator = self.database.raw_iterator_cf(branch_roots);
        iterator.seek_for_prev(encode_branch_position_key(branch.get(), u64::MAX));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let ordinal = decode_branch_position_ordinal(key)?;
            let root = decode_branch_root(iterator.value().ok_or(FilesystemError::Database)?)?;
            if root.sequence <= sequence.get()
                && let Some(metadata) =
                    self.snapshot_metadata_from_branch_root(file_identifier, &root)?
            {
                return Ok(Some(FileSnapshot::new(
                    file_identifier,
                    EventSequence::new(metadata.sequence),
                    BranchPosition::new(branch, ordinal),
                    metadata.file_size,
                )));
            }
            iterator.prev();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;
        Ok(None)
    }

    fn file_snapshot_metadata_for_sequence_at_or_before(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        sequence: EventSequence,
    ) -> Result<Option<FileSnapshot>, FilesystemError> {
        let manifests = self.file_snapshot_manifests()?;
        let prefix = encode_snapshot_file_prefix(branch.get(), file_identifier.get());
        let mut iterator = self.database.raw_iterator_cf(manifests);
        iterator.seek_for_prev(encode_file_snapshot_manifest_key(
            branch.get(),
            file_identifier.get(),
            u64::MAX,
        ));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let (key_branch, key_file, ordinal) = decode_file_snapshot_manifest_key(key)?;
            if key_branch != branch.get() || key_file != file_identifier.get() {
                return Err(FilesystemError::Integrity);
            }
            let metadata =
                decode_snapshot_metadata(iterator.value().ok_or(FilesystemError::Database)?)?;
            if metadata.sequence <= sequence.get() {
                return Ok(Some(FileSnapshot::new(
                    file_identifier,
                    EventSequence::new(metadata.sequence),
                    BranchPosition::new(branch, ordinal),
                    metadata.file_size,
                )));
            }
            iterator.prev();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;

        self.branch_root_snapshot_at_or_before_sequence(branch, file_identifier, sequence)
    }

    fn directory_entry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<StoredDirectoryEntry>, FilesystemError> {
        Ok(self
            .active_branch_state
            .lock()
            .map_err(|_| FilesystemError::Database)?
            .namespace
            .directory_entries
            .get(&(parent, name.to_vec()))
            .cloned())
    }

    fn extended_attribute(&self, inode_number: u64, name: &[u8]) -> FuseResult<Option<Vec<u8>>> {
        Ok(self
            .lock_active_branch_state()?
            .namespace
            .extended_attributes
            .get(&(inode_number, name.to_vec()))
            .cloned())
    }

    fn file_content_manifest_identifier(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
    ) -> Result<Vec<u8>, FilesystemError> {
        if branch == self.active_branch_identifier() {
            return self.active_file_content_manifest_identifier(file_identifier);
        }

        let state = self.namespace_state_for_branch(branch)?;
        state
            .inodes
            .get(&file_identifier.get())
            .and_then(|inode| inode.content_manifest_identifier.clone())
            .ok_or(FilesystemError::Integrity)
    }

    fn active_file_content_manifest_identifier(
        &self,
        file_identifier: FileIdentifier,
    ) -> Result<Vec<u8>, FilesystemError> {
        self.active_branch_state
            .lock()
            .map_err(|_| FilesystemError::Database)?
            .namespace
            .inodes
            .get(&file_identifier.get())
            .and_then(|inode| inode.content_manifest_identifier.clone())
            .ok_or(FilesystemError::Integrity)
    }

    fn branch_root_at_position(
        &self,
        position: BranchPosition,
    ) -> Result<StoredBranchRoot, FilesystemError> {
        self.database
            .get_pinned_cf(
                self.branch_roots()?,
                encode_branch_position_key(position.branch_identifier().get(), position.ordinal()),
            )
            .map_err(|_| FilesystemError::Database)?
            .ok_or(FilesystemError::Integrity)
            .and_then(|value| decode_branch_root(&value))
    }

    fn put_branch_root(
        &self,
        batch: &mut WriteBatch,
        position: BranchPosition,
        sequence: EventSequence,
        namespace_identifier: &[u8],
    ) -> Result<(), FilesystemError> {
        let root = StoredBranchRoot {
            sequence: sequence.get(),
            namespace_identifier: namespace_identifier.to_vec(),
        };
        batch.put_cf(
            self.branch_roots()?,
            encode_branch_position_key(position.branch_identifier().get(), position.ordinal()),
            encode_branch_root(&root)?,
        );
        Ok(())
    }

    fn commit_event(
        &self,
        batch: &mut WriteBatch,
        active_state: &ActiveBranchState,
        event_sequence: EventSequence,
        event: &EventRecord,
        mutation: StoredMutation,
    ) -> FuseResult<ActiveBranchCommit> {
        self.commit_event_with_payloads(
            batch,
            active_state,
            event_sequence,
            event,
            CommitEventPayloads::none(),
            mutation,
        )
    }

    fn commit_event_with_payloads(
        &self,
        batch: &mut WriteBatch,
        active_state: &ActiveBranchState,
        event_sequence: EventSequence,
        event: &EventRecord,
        payloads: CommitEventPayloads<'_>,
        mutation: StoredMutation,
    ) -> FuseResult<ActiveBranchCommit> {
        let CommitEventPayloads {
            event_payload_extents,
            snapshot_content_manifest_identifier,
        } = payloads;
        let events = self.events().map_err(|_| FuseError::Integrity)?;
        let metadata = self.metadata().map_err(|_| FuseError::Integrity)?;
        let branches = self.branches().map_err(|_| FuseError::Integrity)?;
        let branch_identifier = BranchIdentifier::new(active_state.branch.identifier);
        if branch_identifier != self.active_branch_identifier() {
            return Err(FuseError::Integrity);
        }
        let mut branch = active_state.branch.clone();
        let first_parent = EventSequence::new(branch.head_sequence);
        let branch_position = BranchPosition::new(branch_identifier, branch.head_ordinal + 1);
        let (namespace_identifier, namespace_root) = self
            .put_namespace_mutation(batch, active_state, &mutation)
            .map_err(|_| FuseError::Integrity)?;
        let event = event
            .clone()
            .with_branch(branch_identifier, branch_position, first_parent);
        let event_key = encode_u64(event_sequence.get());
        if self
            .database
            .get_pinned_cf(events, event_key)
            .map_err(|_| FuseError::Database)?
            .is_some()
        {
            return Err(FuseError::Integrity);
        }
        branch.head_sequence = event_sequence.get();
        branch.head_ordinal = branch_position.ordinal();
        branch.namespace_identifier = namespace_identifier.clone();
        batch.put_cf(
            events,
            event_key,
            encode_stored_event(&StoredEvent {
                record: event.clone(),
                mutation: mutation.clone(),
            })
            .map_err(|_| FuseError::Database)?,
        );
        batch.put_cf(
            branches,
            encode_u64(branch_identifier.get()),
            encode_branch(&branch).map_err(|_| FuseError::Database)?,
        );
        self.put_event_payloads(batch, event_sequence, &event, event_payload_extents)?;
        self.put_file_event_index(batch, event_sequence, &event)?;
        self.put_branch_event_index(batch, event_sequence, branch_position)?;
        self.put_branch_file_event_index(batch, event_sequence, &event)?;
        self.put_branch_root(
            batch,
            branch_position,
            event_sequence,
            &namespace_identifier,
        )
        .map_err(|_| FuseError::Integrity)?;
        let snapshot_inode = event
            .file_identifier()
            .and_then(|file_identifier| {
                namespace_inode_after_mutation(
                    &active_state.namespace,
                    &mutation,
                    file_identifier.get(),
                )
            });
        self.put_file_snapshot_for_event(
            batch,
            event_sequence,
            &event,
            snapshot_inode,
            None,
            snapshot_content_manifest_identifier,
        )?;
        batch.put_cf(
            metadata,
            METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE,
            encode_u64(event_sequence.get()),
        );
        Ok(ActiveBranchCommit {
            branch,
            namespace_root,
            mutation,
        })
    }

    fn put_event_payloads(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        event: &EventRecord,
        payload_extents: EventPayloadExtents,
    ) -> FuseResult<()> {
        match payload_extents {
            EventPayloadExtents::None => Ok(()),
            EventPayloadExtents::FileWrite {
                overwritten_extents,
                written_extents,
            } => {
                self.put_event_payload_manifest(
                    batch,
                    event_sequence,
                    EVENT_PAYLOAD_PART_OVERWRITTEN,
                    event.overwritten_byte_length().unwrap_or(0),
                    &overwritten_extents,
                )?;
                self.put_event_payload_manifest(
                    batch,
                    event_sequence,
                    EVENT_PAYLOAD_PART_WRITTEN,
                    event.written_byte_length().unwrap_or(0),
                    &written_extents,
                )?;
                Ok(())
            }
        }
    }

    fn put_event_payload_manifest(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        part: u8,
        byte_length: u64,
        extents: &[StoredExtent],
    ) -> FuseResult<()> {
        if byte_length == 0 {
            return Ok(());
        }
        validate_extent_bounds(extents, byte_length)?;
        let content_manifest_identifier = self.put_content_manifest(batch, extents)?;
        let manifest = StoredEventPayloadManifest {
            byte_length,
            content_manifest_identifier,
        };
        batch.put_cf(
            self.event_payload_manifests()
                .map_err(|_| FuseError::Integrity)?,
            encode_event_payload_manifest_key(event_sequence.get(), part),
            encode_event_payload_manifest(&manifest).map_err(|_| FuseError::Database)?,
        );
        Ok(())
    }

    fn put_pending_extents(
        &self,
        batch: &mut WriteBatch,
        _extent_set: ExtentSet,
        bytes: &[u8],
        extents: &[StoredExtent],
        pending: &[PendingExtent],
    ) -> FuseResult<()> {
        if extents.len() != pending.len() {
            return Err(FuseError::Integrity);
        }
        for (stored, pending) in extents.iter().zip(pending) {
            if stored.chunk_identifier != pending.chunk_identifier {
                return Err(FuseError::Integrity);
            }
            self.put_verified_content_chunk(
                batch,
                &stored.chunk_identifier,
                &bytes[pending.byte_range()],
            )?;
        }
        Ok(())
    }

    fn validate_stored_extents(&self, extents: &[StoredExtent]) -> FuseResult<()> {
        let content_chunks = self.content_chunks().map_err(|_| FuseError::Integrity)?;
        for extent in extents {
            let chunk = self
                .database
                .get_pinned_cf(content_chunks, &extent.chunk_identifier)
                .map_err(|_| FuseError::Database)?
                .ok_or(FuseError::Integrity)?;
            validate_content_chunk(&extent.chunk_identifier, &chunk).map_err(to_fuse_integrity)?;
            let chunk_end = extent
                .chunk_offset
                .checked_add(extent.length)
                .ok_or(FuseError::Integrity)?;
            if existing_byte_length_for_storage(chunk_end).map_err(to_fuse_integrity)? > chunk.len()
            {
                return Err(FuseError::Integrity);
            }
        }
        Ok(())
    }

    fn put_verified_content_chunk(
        &self,
        batch: &mut WriteBatch,
        chunk_identifier: &[u8],
        bytes: &[u8],
    ) -> FuseResult<()> {
        validate_prefixed_blake3_identifier_shape(chunk_identifier, CONTENT_CHUNK_HASH_BLAKE3)
            .map_err(to_fuse_integrity)?;
        let content_chunks = self.content_chunks().map_err(|_| FuseError::Integrity)?;
        if let Some(existing) = self
            .database
            .get_pinned_cf(content_chunks, chunk_identifier)
            .map_err(|_| FuseError::Database)?
        {
            validate_content_chunk(chunk_identifier, &existing).map_err(to_fuse_integrity)?;
            if existing.as_ref() != bytes {
                return Err(FuseError::Integrity);
            }
        } else {
            batch.put_cf(content_chunks, chunk_identifier, bytes);
        }
        Ok(())
    }

    fn put_content_manifest(
        &self,
        batch: &mut WriteBatch,
        extents: &[StoredExtent],
    ) -> FuseResult<Vec<u8>> {
        let bytes = encode_content_manifest(extents).map_err(|_| FuseError::Database)?;
        let identifier = content_manifest_identifier_for_bytes(&bytes);
        if !extents.is_empty() {
            let manifests = self
                .content_manifest_nodes()
                .map_err(|_| FuseError::Integrity)?;
            batch.put_cf(manifests, &identifier, bytes);
        }
        Ok(identifier)
    }

    fn put_appended_content_manifest(
        &self,
        batch: &mut WriteBatch,
        previous_identifier: &[u8],
        previous_length: u64,
        appended_extents: &[StoredExtent],
    ) -> FuseResult<Vec<u8>> {
        ContentManifestIdentifier::from_bytes(previous_identifier).map_err(to_fuse_integrity)?;
        if appended_extents.is_empty() {
            return Ok(previous_identifier.to_vec());
        }

        let appended_identifier = self.put_content_manifest(batch, appended_extents)?;
        let empty_identifier = empty_content_manifest_identifier().map_err(to_fuse_integrity)?;
        if previous_identifier == empty_identifier.as_slice() {
            return Ok(appended_identifier);
        }

        let appended_child =
            content_manifest_child_for_extents(appended_extents, appended_identifier)?;
        self.put_content_manifest_with_appended_child(
            batch,
            previous_identifier,
            0,
            previous_length,
            appended_child,
        )
    }

    fn put_content_manifest_with_appended_child(
        &self,
        batch: &mut WriteBatch,
        current_identifier: &[u8],
        current_start: u64,
        current_end: u64,
        appended_child: StoredContentManifestChild,
    ) -> FuseResult<Vec<u8>> {
        ContentManifestIdentifier::from_bytes(current_identifier).map_err(to_fuse_integrity)?;
        let empty_identifier = empty_content_manifest_identifier().map_err(to_fuse_integrity)?;
        if current_identifier == empty_identifier.as_slice() {
            return Ok(appended_child.identifier);
        }

        let Some(mut children) = self
            .content_manifest_concat_children(current_identifier)
            .map_err(to_fuse_integrity)?
        else {
            return self.put_wrapped_content_manifest_with_appended_child(
                batch,
                current_identifier,
                current_start,
                current_end,
                appended_child,
            );
        };

        if children.len() < CONTENT_MANIFEST_CONCAT_MAX_CHILDREN {
            children.push(appended_child);
            return self.put_content_manifest_node(
                batch,
                &StoredContentManifestNode::Concat { children },
            );
        }

        let Some(last_child) = children.last_mut() else {
            return Err(FuseError::Integrity);
        };
        let last_child_end = content_manifest_child_end(last_child).map_err(to_fuse_integrity)?;
        if last_child_end != current_end {
            return self.put_wrapped_content_manifest_with_appended_child(
                batch,
                current_identifier,
                current_start,
                current_end,
                appended_child,
            );
        }

        let appended_child_end =
            content_manifest_child_end(&appended_child).map_err(to_fuse_integrity)?;
        last_child.identifier = self.put_content_manifest_with_appended_child(
            batch,
            &last_child.identifier,
            last_child.logical_offset,
            last_child_end,
            appended_child,
        )?;
        last_child.length = appended_child_end
            .checked_sub(last_child.logical_offset)
            .ok_or(FuseError::Integrity)?;
        self.put_content_manifest_node(batch, &StoredContentManifestNode::Concat { children })
    }

    fn put_wrapped_content_manifest_with_appended_child(
        &self,
        batch: &mut WriteBatch,
        current_identifier: &[u8],
        current_start: u64,
        current_end: u64,
        appended_child: StoredContentManifestChild,
    ) -> FuseResult<Vec<u8>> {
        let mut children = Vec::with_capacity(2);
        if current_start < current_end {
            children.push(StoredContentManifestChild {
                logical_offset: current_start,
                length: current_end
                    .checked_sub(current_start)
                    .ok_or(FuseError::Integrity)?,
                identifier: current_identifier.to_vec(),
            });
        }
        children.push(appended_child);
        self.put_content_manifest_children(batch, children)
    }

    fn put_replaced_content_manifest(
        &self,
        batch: &mut WriteBatch,
        previous_identifier: &[u8],
        previous_length: u64,
        offset: u64,
        end: u64,
        written_extents: &[StoredExtent],
    ) -> FuseResult<Vec<u8>> {
        ContentManifestIdentifier::from_bytes(previous_identifier).map_err(to_fuse_integrity)?;
        let mut children = Vec::new();
        if offset > 0 {
            children.push(StoredContentManifestChild {
                logical_offset: 0,
                length: offset.min(previous_length),
                identifier: previous_identifier.to_vec(),
            });
        }
        if !written_extents.is_empty() {
            let written_identifier = self.put_content_manifest(batch, written_extents)?;
            children.push(content_manifest_child_for_extents(
                written_extents,
                written_identifier,
            )?);
        }
        if end < previous_length {
            children.push(StoredContentManifestChild {
                logical_offset: end,
                length: previous_length - end,
                identifier: previous_identifier.to_vec(),
            });
        }
        self.put_content_manifest_children(batch, children)
    }

    fn put_content_manifest_range(
        &self,
        batch: &mut WriteBatch,
        identifier: &[u8],
        offset: u64,
        length: u64,
    ) -> FuseResult<Vec<u8>> {
        ContentManifestIdentifier::from_bytes(identifier).map_err(to_fuse_integrity)?;
        if length == 0 {
            return empty_content_manifest_identifier().map_err(to_fuse_integrity);
        }
        self.put_content_manifest_children(
            batch,
            vec![StoredContentManifestChild {
                logical_offset: offset,
                length,
                identifier: identifier.to_vec(),
            }],
        )
    }

    fn put_content_manifest_children(
        &self,
        batch: &mut WriteBatch,
        mut children: Vec<StoredContentManifestChild>,
    ) -> FuseResult<Vec<u8>> {
        children.retain(|child| child.length != 0);
        if children.is_empty() {
            return empty_content_manifest_identifier().map_err(to_fuse_integrity);
        }
        self.put_content_manifest_node(batch, &StoredContentManifestNode::Concat { children })
    }

    fn put_content_manifest_node(
        &self,
        batch: &mut WriteBatch,
        node: &StoredContentManifestNode,
    ) -> FuseResult<Vec<u8>> {
        validate_stored_content_manifest_node(node).map_err(to_fuse_integrity)?;
        let bytes = encode_content_manifest_node(node).map_err(|_| FuseError::Database)?;
        let identifier = content_manifest_identifier_for_bytes(&bytes);
        let manifests = self
            .content_manifest_nodes()
            .map_err(|_| FuseError::Integrity)?;
        batch.put_cf(manifests, &identifier, bytes);
        Ok(identifier)
    }

    fn content_manifest_concat_children(
        &self,
        content_manifest_identifier: &[u8],
    ) -> Result<Option<Vec<StoredContentManifestChild>>, FilesystemError> {
        let content_manifest_identifier =
            ContentManifestIdentifier::from_bytes(content_manifest_identifier)?;
        let empty_identifier = empty_content_manifest_identifier()?;
        if content_manifest_identifier.as_bytes() == empty_identifier.as_slice() {
            return Ok(Some(Vec::new()));
        }

        let manifests = self.content_manifest_nodes()?;
        let value = self
            .database
            .get_pinned_cf(manifests, content_manifest_identifier.as_bytes())
            .map_err(|_| FilesystemError::Database)?
            .ok_or(FilesystemError::Integrity)?;
        validate_content_manifest(content_manifest_identifier.as_bytes(), &value)?;
        let Some(node) = decode_versioned_content_manifest_node(&value)? else {
            return Ok(None);
        };
        let StoredContentManifestNode::Concat { children } = node;
        Ok(Some(children))
    }

    fn content_manifest_extents_in_range(
        &self,
        content_manifest_identifier: &[u8],
        start: u64,
        end: u64,
    ) -> Result<Vec<StoredExtent>, FilesystemError> {
        let content_manifest_identifier =
            ContentManifestIdentifier::from_bytes(content_manifest_identifier)?;
        let empty_identifier =
            empty_content_manifest_identifier().map_err(|_| FilesystemError::Integrity)?;
        if content_manifest_identifier.as_bytes() == empty_identifier.as_slice() || start >= end {
            return Ok(Vec::new());
        }
        let mut extents = Vec::new();
        self.collect_content_manifest_extents_in_range(
            content_manifest_identifier.as_bytes(),
            &empty_identifier,
            start,
            end,
            &mut extents,
            0,
        )?;
        Ok(extents)
    }

    fn collect_content_manifest_extents_in_range(
        &self,
        content_manifest_identifier: &[u8],
        empty_identifier: &[u8],
        start: u64,
        end: u64,
        extents: &mut Vec<StoredExtent>,
        depth: usize,
    ) -> Result<(), FilesystemError> {
        if depth > CONTENT_MANIFEST_MAX_DEPTH {
            return Err(FilesystemError::Integrity);
        }
        let content_manifest_identifier =
            ContentManifestIdentifier::from_bytes(content_manifest_identifier)?;
        if content_manifest_identifier.as_bytes() == empty_identifier {
            return Ok(());
        }

        let manifests = self.content_manifest_nodes()?;
        let value = self
            .database
            .get_pinned_cf(manifests, content_manifest_identifier.as_bytes())
            .map_err(|_| FilesystemError::Database)?
            .ok_or(FilesystemError::Integrity)?;
        validate_content_manifest(content_manifest_identifier.as_bytes(), &value)?;
        match decode_content_manifest_entry(&value)? {
            DecodedContentManifest::Extents(decoded) => {
                extents.extend(slice_extents(&decoded, start, end, start)?);
                Ok(())
            }
            DecodedContentManifest::Node(StoredContentManifestNode::Concat { children }) => {
                for child in children {
                    let child_end = child
                        .logical_offset
                        .checked_add(child.length)
                        .ok_or(FilesystemError::Integrity)?;
                    if child_end <= start || child.logical_offset >= end {
                        continue;
                    }
                    let child_start = start.max(child.logical_offset);
                    let child_stop = end.min(child_end);
                    self.collect_content_manifest_extents_in_range(
                        &child.identifier,
                        empty_identifier,
                        child_start,
                        child_stop,
                        extents,
                        depth + 1,
                    )?;
                }
                Ok(())
            }
        }
    }

    fn read_extent_range(
        &self,
        extent_set: ExtentSet,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        // Range reads traverse only manifest children that intersect the
        // requested range, then read only the intersecting content chunks.
        if length == 0 {
            return Ok(Vec::new());
        }
        let length = existing_byte_length_for_storage(length)?;
        let mut bytes = vec![0; length];
        let end = offset
            .checked_add(length as u64)
            .ok_or(FilesystemError::Integrity)?;
        let extents = self.extent_manifest_range(extent_set, offset, end)?;
        for extent in extents {
            if extent.logical_offset >= end {
                break;
            }
            let extent_end = extent
                .logical_offset
                .checked_add(extent.length)
                .ok_or(FilesystemError::Integrity)?;
            if extent_end > offset {
                let read_start = offset.max(extent.logical_offset);
                let read_end = end.min(extent_end);
                let destination_start = existing_byte_length_for_storage(read_start - offset)?;
                let chunk_start = existing_byte_length_for_storage(
                    extent.chunk_offset + read_start - extent.logical_offset,
                )?;
                let read_len = existing_byte_length_for_storage(read_end - read_start)?;
                let chunk_end = chunk_start
                    .checked_add(read_len)
                    .ok_or(FilesystemError::Integrity)?;
                let chunk = self
                    .database
                    .get_pinned_cf(self.content_chunks()?, &extent.chunk_identifier)
                    .map_err(|_| FilesystemError::Database)?
                    .ok_or(FilesystemError::Integrity)?;
                validate_content_chunk(&extent.chunk_identifier, &chunk)?;
                if chunk_end > chunk.len() {
                    return Err(FilesystemError::Integrity);
                }
                bytes[destination_start..destination_start + read_len]
                    .copy_from_slice(&chunk[chunk_start..chunk_end]);
            }
        }
        Ok(bytes)
    }

    fn extent_manifest_range(
        &self,
        extent_set: ExtentSet,
        start: u64,
        end: u64,
    ) -> Result<Vec<StoredExtent>, FilesystemError> {
        match extent_set {
            ExtentSet::Current {
                branch,
                file_identifier,
            } => {
                let identifier = self.file_content_manifest_identifier(branch, file_identifier)?;
                self.content_manifest_extents_in_range(&identifier, start, end)
            }
            ExtentSet::Snapshot {
                branch,
                file_identifier,
                ordinal,
            } => {
                let manifest = self.file_snapshot_manifest(
                    branch,
                    file_identifier,
                    BranchPosition::new(branch, ordinal),
                )?;
                self.content_manifest_extents_in_range(
                    &manifest.content_manifest_identifier,
                    start,
                    end,
                )
            }
            ExtentSet::EventPayload { sequence, part } => {
                let manifests = self.event_payload_manifests()?;
                let value = self
                    .database
                    .get_pinned_cf(
                        manifests,
                        encode_event_payload_manifest_key(sequence.get(), part),
                    )
                    .map_err(|_| FilesystemError::Database)?
                    .ok_or(FilesystemError::Integrity)?;
                let manifest = decode_event_payload_manifest(&value)?;
                self.content_manifest_extents_in_range(
                    &manifest.content_manifest_identifier,
                    start,
                    end,
                )
            }
        }
    }

    fn file_snapshot_manifest(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        position: BranchPosition,
    ) -> Result<StoredSnapshotMetadata, FilesystemError> {
        if position.branch_identifier() != branch {
            return Err(FilesystemError::Integrity);
        }
        let manifests = self.file_snapshot_manifests()?;
        if let Some(value) = self
            .database
            .get_pinned_cf(
                manifests,
                encode_file_snapshot_manifest_key(
                    branch.get(),
                    file_identifier.get(),
                    position.ordinal(),
                ),
            )
            .map_err(|_| FilesystemError::Database)?
        {
            return decode_snapshot_metadata(&value);
        }
        let root = self.branch_root_at_position(position)?;
        self.snapshot_metadata_from_branch_root(file_identifier, &root)?
            .ok_or(FilesystemError::Integrity)
    }

    fn put_file_event_index(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        event: &EventRecord,
    ) -> FuseResult<()> {
        let file_events = self.file_events().map_err(|_| FuseError::Integrity)?;
        for file_identifier in event_file_identifiers(event) {
            batch.put_cf(
                file_events,
                encode_file_sequence_key(file_identifier.get(), event_sequence.get()),
                [],
            );
        }
        Ok(())
    }

    fn put_branch_event_index(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        branch_position: BranchPosition,
    ) -> FuseResult<()> {
        let branch_events = self.branch_events().map_err(|_| FuseError::Integrity)?;
        batch.put_cf(
            branch_events,
            encode_branch_position_key(
                branch_position.branch_identifier().get(),
                branch_position.ordinal(),
            ),
            encode_u64(event_sequence.get()),
        );
        Ok(())
    }

    fn put_branch_file_event_index(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        event: &EventRecord,
    ) -> FuseResult<()> {
        let Some(branch_position) = event.branch_position() else {
            return Err(FuseError::Integrity);
        };
        let branch_file_events = self
            .branch_file_events()
            .map_err(|_| FuseError::Integrity)?;
        for file_identifier in event_file_identifiers(event) {
            batch.put_cf(
                branch_file_events,
                encode_branch_file_position_key(
                    branch_position.branch_identifier().get(),
                    file_identifier.get(),
                    branch_position.ordinal(),
                ),
                encode_u64(event_sequence.get()),
            );
        }
        Ok(())
    }

    fn put_file_snapshot_for_event(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        event: &EventRecord,
        final_inode: Option<&StoredInode>,
        snapshot_extents: Option<&[StoredExtent]>,
        snapshot_content_manifest_identifier: Option<&[u8]>,
    ) -> FuseResult<()> {
        let Some(branch_position) = event.branch_position() else {
            return Err(FuseError::Integrity);
        };
        if !matches!(
            event.kind(),
            EventKind::NodeCreated | EventKind::FileCreated | EventKind::FileWritten
        ) {
            return Ok(());
        };
        if let Some(file_identifier) = event.file_identifier() {
            self.put_file_snapshot_for_identifier(
                batch,
                FileSnapshotWriteContext {
                    event_sequence,
                    event,
                    file_identifier,
                    branch_position,
                    final_inode,
                    snapshot_extents,
                    snapshot_content_manifest_identifier,
                },
            )?;
        }
        Ok(())
    }

    fn put_file_snapshot_for_identifier(
        &self,
        batch: &mut WriteBatch,
        context: FileSnapshotWriteContext<'_>,
    ) -> FuseResult<()> {
        let file_size =
            event_snapshot_file_size(context.event, context.file_identifier).unwrap_or(0);
        let content_manifest_identifier = if let Some(identifier) =
            context.snapshot_content_manifest_identifier
        {
            identifier.to_vec()
        } else if let Some(extents) = context.snapshot_extents {
            self.put_content_manifest(batch, extents)?
        } else {
            context
                .final_inode
                .and_then(|inode| inode.content_manifest_identifier.clone())
                .unwrap_or(empty_content_manifest_identifier().map_err(to_fuse_integrity)?)
        };
        let metadata = StoredSnapshotMetadata {
            file_size,
            sequence: context.event_sequence.get(),
            content_manifest_identifier,
        };
        batch.put_cf(
            self.file_snapshot_manifests()
                .map_err(|_| FuseError::Integrity)?,
            encode_file_snapshot_manifest_key(
                context.branch_position.branch_identifier().get(),
                context.file_identifier.get(),
                context.branch_position.ordinal(),
            ),
            encode_snapshot_metadata(&metadata).map_err(|_| FuseError::Database)?,
        );
        Ok(())
    }

    fn put_metadata_u64(&self, batch: &mut WriteBatch, key: &[u8], value: u64) -> FuseResult<()> {
        let metadata = self.metadata().map_err(|_| FuseError::Integrity)?;
        batch.put_cf(metadata, key, encode_u64(value));
        Ok(())
    }

    fn events(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_EVENTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn file_events(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_FILE_EVENTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn branches(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_BRANCHES)
            .ok_or(FilesystemError::Integrity)
    }

    fn branch_names(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_BRANCH_NAMES)
            .ok_or(FilesystemError::Integrity)
    }

    fn branch_events(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_BRANCH_EVENTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn branch_file_events(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_BRANCH_FILE_EVENTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn content_chunks(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_CONTENT_CHUNKS)
            .ok_or(FilesystemError::Integrity)
    }

    fn content_manifest_nodes(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_CONTENT_MANIFEST_NODES)
            .ok_or(FilesystemError::Integrity)
    }

    #[cfg(test)]
    fn namespace_nodes(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_NAMESPACE_NODES)
            .ok_or(FilesystemError::Integrity)
    }

    fn branch_roots(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_BRANCH_ROOTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn file_snapshot_manifests(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_FILE_SNAPSHOT_MANIFESTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn event_payload_manifests(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_EVENT_PAYLOAD_MANIFESTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn metadata(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
            .ok_or(FilesystemError::Integrity)
    }
}

fn is_new_database_directory(path: &Path) -> Result<bool, FilesystemError> {
    match fs::metadata(path) {
        Ok(metadata) if !metadata.is_dir() => Err(FilesystemError::Database),
        Ok(_) => {
            let mut entries = fs::read_dir(path).map_err(|_| FilesystemError::Database)?;
            match entries.next() {
                Some(Ok(_)) => Ok(false),
                Some(Err(_)) => Err(FilesystemError::Database),
                None => Ok(true),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(_) => Err(FilesystemError::Database),
    }
}

fn database_options(create_missing: bool, block_cache: &Cache) -> Options {
    let mut options = Options::default();
    options.create_if_missing(create_missing);
    options.create_missing_column_families(create_missing);
    apply_rocksdb_performance_options(&mut options, block_cache, false);
    options
}

fn required_column_family_names() -> BTreeSet<String> {
    COLUMN_FAMILY_REQUIRED
        .iter()
        .map(|name| (*name).to_owned())
        .collect()
}

fn existing_column_family_names(
    options: &Options,
    database_directory: &Path,
) -> Result<BTreeSet<String>, FilesystemError> {
    DB::list_cf(options, database_directory)
        .map(|names| names.into_iter().collect())
        .map_err(|_| FilesystemError::Database)
}

fn column_family_descriptors(
    names: BTreeSet<String>,
    block_cache: &Cache,
) -> Vec<ColumnFamilyDescriptor> {
    names
        .into_iter()
        .map(|name| {
            ColumnFamilyDescriptor::new(name.clone(), column_family_options(&name, block_cache))
        })
        .collect()
}

fn column_family_options(name: &str, block_cache: &Cache) -> Options {
    let mut options = Options::default();
    let uses_fixed_prefix = matches!(
        name,
        COLUMN_FAMILY_FILE_EVENTS
            | COLUMN_FAMILY_BRANCH_EVENTS
            | COLUMN_FAMILY_BRANCH_FILE_EVENTS
            | COLUMN_FAMILY_BRANCH_ROOTS
            | COLUMN_FAMILY_FILE_SNAPSHOT_MANIFESTS
            | COLUMN_FAMILY_EVENT_PAYLOAD_MANIFESTS
    );
    apply_rocksdb_performance_options(&mut options, block_cache, uses_fixed_prefix);
    match name {
        COLUMN_FAMILY_FILE_EVENTS | COLUMN_FAMILY_BRANCH_EVENTS | COLUMN_FAMILY_BRANCH_ROOTS => {
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
        }
        COLUMN_FAMILY_BRANCH_FILE_EVENTS | COLUMN_FAMILY_FILE_SNAPSHOT_MANIFESTS => {
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(16));
        }
        COLUMN_FAMILY_EVENT_PAYLOAD_MANIFESTS => {
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(9));
        }
        _ => {}
    }
    if matches!(name, COLUMN_FAMILY_CONTENT_CHUNKS) {
        options.set_enable_blob_files(true);
        options.set_min_blob_size(ROCKSDB_BLOB_MIN_SIZE);
        options.set_blob_cache(block_cache);
    }
    options
}

fn apply_rocksdb_performance_options(
    options: &mut Options,
    block_cache: &Cache,
    uses_fixed_prefix: bool,
) {
    let mut table_options = BlockBasedOptions::default();
    table_options.set_block_cache(block_cache);
    table_options.set_bloom_filter(ROCKSDB_BLOOM_FILTER_BITS_PER_KEY, false);
    table_options.set_cache_index_and_filter_blocks(true);
    table_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
    table_options.set_pin_top_level_index_and_filter(true);
    if uses_fixed_prefix {
        table_options.set_index_type(BlockBasedIndexType::HashSearch);
    }
    table_options.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
    table_options.set_optimize_filters_for_memory(true);

    options.set_block_based_table_factory(&table_options);
    options.set_optimize_filters_for_hits(true);
    options.set_level_compaction_dynamic_level_bytes(true);
    options.set_bytes_per_sync(ROCKSDB_BYTES_PER_SYNC);
    options.set_wal_bytes_per_sync(ROCKSDB_BYTES_PER_SYNC);
    options.set_write_buffer_size(ROCKSDB_WRITE_BUFFER_SIZE);
    options.set_max_write_buffer_number(ROCKSDB_MAX_WRITE_BUFFER_NUMBER);
}

fn initialize_new_database(database: &DB) -> Result<(), FilesystemError> {
    let metadata = database
        .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
        .ok_or(FilesystemError::Integrity)?;
    let events = database
        .cf_handle(COLUMN_FAMILY_EVENTS)
        .ok_or(FilesystemError::Integrity)?;
    let branches = database
        .cf_handle(COLUMN_FAMILY_BRANCHES)
        .ok_or(FilesystemError::Integrity)?;
    let branch_names = database
        .cf_handle(COLUMN_FAMILY_BRANCH_NAMES)
        .ok_or(FilesystemError::Integrity)?;
    let branch_events = database
        .cf_handle(COLUMN_FAMILY_BRANCH_EVENTS)
        .ok_or(FilesystemError::Integrity)?;
    let branch_roots = database
        .cf_handle(COLUMN_FAMILY_BRANCH_ROOTS)
        .ok_or(FilesystemError::Integrity)?;
    let initial_branch_position = BranchPosition::new(BRANCH_IDENTIFIER_INITIAL, 0);
    let initialized = EventRecord::new(
        EVENT_SEQUENCE_INITIAL,
        EventKind::FilesystemInitialized,
        UtcDateTime::now(),
        None,
        None,
        None,
        None,
    )
    .with_branch(
        BRANCH_IDENTIFIER_INITIAL,
        initial_branch_position,
        EVENT_SEQUENCE_INITIAL,
    );
    let now = StoredTime::now();
    let root = StoredInode {
        kind: StoredNodeKind::Directory,
        size: 0,
        nlink: 2,
        uid: current_effective_uid(),
        gid: current_effective_gid(),
        mode: INODE_MODE_ROOT,
        rdev: 0,
        atime: now,
        mtime: now,
        ctime: now,
        parent: INODE_ROOT,
        name: Vec::new(),
        symlink_target: None,
        content_manifest_identifier: None,
    };

    let mut batch = WriteBatch::default();
    let mut initial_state = MaterializedBranchState::default();
    initial_state.inodes.insert(INODE_ROOT, root.clone());
    let namespace_identifier =
        put_namespace_state_in_database(database, &mut batch, &initial_state)?;
    let main_branch = StoredBranch {
        identifier: BRANCH_IDENTIFIER_INITIAL.get(),
        name: BRANCH_NAME_INITIAL.to_owned(),
        status: BranchStatus::Open,
        head_sequence: EVENT_SEQUENCE_INITIAL.get(),
        head_ordinal: 0,
        namespace_identifier: namespace_identifier.clone(),
        fork_branch_identifier: None,
        fork_ordinal: None,
    };
    batch.put_cf(
        metadata,
        METADATA_KEY_STORAGE_SCHEMA_VERSION,
        encode_u64(STORAGE_SCHEMA_VERSION_CURRENT),
    );
    batch.put_cf(
        metadata,
        METADATA_KEY_NEXT_INODE_NUMBER,
        encode_u64(INODE_FIRST_ALLOCATED),
    );
    batch.put_cf(
        metadata,
        METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE,
        encode_u64(EVENT_SEQUENCE_INITIAL.get()),
    );
    batch.put_cf(
        metadata,
        METADATA_KEY_NEXT_BRANCH_IDENTIFIER,
        encode_u64(BRANCH_IDENTIFIER_INITIAL.get() + 1),
    );
    batch.put_cf(
        metadata,
        METADATA_KEY_ACTIVE_BRANCH_IDENTIFIER,
        encode_u64(BRANCH_IDENTIFIER_INITIAL.get()),
    );
    batch.put_cf(
        metadata,
        METADATA_KEY_VOLUME_NAME,
        METADATA_VOLUME_NAME_DEFAULT.as_bytes(),
    );
    let mut initial_mutation = StoredMutation::default();
    initial_mutation.put_inode(INODE_ROOT, &root);
    batch.put_cf(
        events,
        encode_u64(EVENT_SEQUENCE_INITIAL.get()),
        encode_stored_event(&StoredEvent {
            record: initialized,
            mutation: initial_mutation,
        })?,
    );
    batch.put_cf(
        branches,
        encode_u64(BRANCH_IDENTIFIER_INITIAL.get()),
        encode_branch(&main_branch)?,
    );
    batch.put_cf(
        branch_names,
        BRANCH_NAME_INITIAL.as_bytes(),
        encode_u64(BRANCH_IDENTIFIER_INITIAL.get()),
    );
    batch.put_cf(
        branch_events,
        encode_branch_position_key(BRANCH_IDENTIFIER_INITIAL.get(), 0),
        encode_u64(EVENT_SEQUENCE_INITIAL.get()),
    );
    batch.put_cf(
        branch_roots,
        encode_branch_position_key(BRANCH_IDENTIFIER_INITIAL.get(), 0),
        encode_branch_root(&StoredBranchRoot {
            sequence: EVENT_SEQUENCE_INITIAL.get(),
            namespace_identifier,
        })?,
    );
    write_batch(database, batch)
}

fn load_write_state(database: &DB) -> Result<WriteState, FilesystemError> {
    let next_inode_number = read_metadata_u64(database, METADATA_KEY_NEXT_INODE_NUMBER)?;
    let last_event_sequence =
        read_metadata_u64(database, METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE)?;
    let next_branch_identifier = read_metadata_u64(database, METADATA_KEY_NEXT_BRANCH_IDENTIFIER)?;
    let active_branch_identifier =
        read_metadata_u64(database, METADATA_KEY_ACTIVE_BRANCH_IDENTIFIER)?;
    read_metadata_string(database, METADATA_KEY_VOLUME_NAME)?;
    let branches = database
        .cf_handle(COLUMN_FAMILY_BRANCHES)
        .ok_or(FilesystemError::Integrity)?;
    let active_branch = database
        .get_pinned_cf(branches, encode_u64(active_branch_identifier))
        .map_err(|_| FilesystemError::Database)?
        .ok_or(FilesystemError::Integrity)
        .and_then(|value| decode_branch(&value))?;
    Ok(WriteState {
        next_inode_number,
        last_event_sequence,
        next_branch_identifier,
        active_branch_identifier,
        active_branch_head_sequence: active_branch.head_sequence,
        active_branch_head_ordinal: active_branch.head_ordinal,
    })
}

struct StorageSchemaMigration {
    source_version: u64,
    target_version: u64,
    migrate: fn(&mut DB, &mut WriteBatch) -> Result<(), FilesystemError>,
}

const STORAGE_SCHEMA_MIGRATIONS: &[StorageSchemaMigration] = &[];

fn migrate_database_to_current_schema(
    database: &mut DB,
    block_cache: &Cache,
) -> Result<(), FilesystemError> {
    let mut schema_version = read_storage_schema_version(database)?;
    validate_supported_storage_schema_version(schema_version)?;
    validate_database_column_families(
        database,
        &required_column_family_names_for_schema(schema_version)?,
    )?;

    loop {
        if schema_version == STORAGE_SCHEMA_VERSION_CURRENT {
            break;
        }

        let migration = storage_schema_migration(schema_version)?;
        create_column_families_for_schema(database, migration.target_version, block_cache)?;

        let mut batch = WriteBatch::default();
        (migration.migrate)(database, &mut batch)?;
        let metadata = database
            .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
            .ok_or(FilesystemError::Integrity)?;
        batch.put_cf(
            metadata,
            METADATA_KEY_STORAGE_SCHEMA_VERSION,
            encode_u64(migration.target_version),
        );
        write_batch(database, batch)?;
        schema_version = migration.target_version;
    }

    validate_database_column_families(
        database,
        &required_column_family_names_for_schema(STORAGE_SCHEMA_VERSION_CURRENT)?,
    )
}

fn storage_schema_migration(
    schema_version: u64,
) -> Result<&'static StorageSchemaMigration, FilesystemError> {
    STORAGE_SCHEMA_MIGRATIONS
        .iter()
        .find(|migration| migration.source_version == schema_version)
        .filter(|migration| {
            migration.target_version > migration.source_version
                && validate_supported_storage_schema_version(migration.target_version).is_ok()
        })
        .ok_or(FilesystemError::Integrity)
}

fn create_column_families_for_schema(
    database: &mut DB,
    schema_version: u64,
    block_cache: &Cache,
) -> Result<(), FilesystemError> {
    for name in required_column_family_names_for_schema(schema_version)? {
        if database.cf_handle(&name).is_none() {
            database
                .create_cf(&name, &column_family_options(&name, block_cache))
                .map_err(|_| FilesystemError::Database)?;
        }
    }
    Ok(())
}

fn required_column_family_names_for_schema(
    schema_version: u64,
) -> Result<BTreeSet<String>, FilesystemError> {
    validate_supported_storage_schema_version(schema_version)?;
    Ok(required_column_family_names())
}

fn validate_database_column_families(
    database: &DB,
    required_column_families: &BTreeSet<String>,
) -> Result<(), FilesystemError> {
    for name in required_column_families {
        if database.cf_handle(name).is_none() {
            return Err(FilesystemError::Integrity);
        }
    }
    Ok(())
}

fn validate_supported_storage_schema_version(schema_version: u64) -> Result<(), FilesystemError> {
    if !(STORAGE_SCHEMA_VERSION_BASELINE..=STORAGE_SCHEMA_VERSION_CURRENT).contains(&schema_version)
    {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn read_storage_schema_version(database: &DB) -> Result<u64, FilesystemError> {
    read_metadata_u64(database, METADATA_KEY_STORAGE_SCHEMA_VERSION)
}

fn validate_existing_database(database: &DB) -> Result<(), FilesystemError> {
    let schema_version = read_storage_schema_version(database)?;
    if schema_version != STORAGE_SCHEMA_VERSION_CURRENT {
        return Err(FilesystemError::Integrity);
    }
    let next_inode_number = read_metadata_u64(database, METADATA_KEY_NEXT_INODE_NUMBER)?;
    let last_event_sequence =
        read_metadata_u64(database, METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE)?;
    validate_last_event_sequence(database, last_event_sequence)?;
    let next_branch_identifier = read_metadata_u64(database, METADATA_KEY_NEXT_BRANCH_IDENTIFIER)?;
    let active_branch_identifier =
        read_metadata_u64(database, METADATA_KEY_ACTIVE_BRANCH_IDENTIFIER)?;
    validate_allocator_metadata(database, next_inode_number, next_branch_identifier)?;
    let active_branch =
        stored_branch_in_database(database, BranchIdentifier::new(active_branch_identifier))?;
    let active_root = branch_root_at_position_in_database(
        database,
        BranchPosition::new(
            BranchIdentifier::new(active_branch_identifier),
            active_branch.head_ordinal,
        ),
    )?;
    if active_root.sequence != active_branch.head_sequence
        || active_root.namespace_identifier != active_branch.namespace_identifier
    {
        return Err(FilesystemError::Integrity);
    }
    let state =
        namespace_state_for_identifier_in_database(database, &active_branch.namespace_identifier)?;
    let root = state
        .inodes
        .get(&INODE_ROOT)
        .ok_or(FilesystemError::Integrity)?;
    if root.kind != StoredNodeKind::Directory {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn validate_allocator_metadata(
    database: &DB,
    next_inode_number: u64,
    next_branch_identifier: u64,
) -> Result<(), FilesystemError> {
    if next_inode_number < INODE_FIRST_ALLOCATED
        || next_inode_number <= max_stored_inode_number(database)?
    {
        return Err(FilesystemError::Integrity);
    }
    if next_branch_identifier <= max_stored_branch_identifier(database)? {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn max_stored_inode_number(database: &DB) -> Result<u64, FilesystemError> {
    let branch_roots = database
        .cf_handle(COLUMN_FAMILY_BRANCH_ROOTS)
        .ok_or(FilesystemError::Integrity)?;
    let mut maximum = INODE_ROOT;
    let mut seen_roots = BTreeSet::new();
    let mut iterator = database.raw_iterator_cf(branch_roots);
    iterator.seek_to_first();
    while iterator.valid() {
        let value = iterator.value().ok_or(FilesystemError::Database)?;
        let root = decode_branch_root(value)?;
        if seen_roots.insert(root.namespace_identifier.clone()) {
            let state =
                namespace_state_for_identifier_in_database(database, &root.namespace_identifier)?;
            for inode_number in state.inodes.keys() {
                maximum = maximum.max(*inode_number);
            }
        }
        iterator.next();
    }
    iterator.status().map_err(|_| FilesystemError::Database)?;
    Ok(maximum)
}

fn max_stored_branch_identifier(database: &DB) -> Result<u64, FilesystemError> {
    let branches = database
        .cf_handle(COLUMN_FAMILY_BRANCHES)
        .ok_or(FilesystemError::Integrity)?;
    let mut maximum = 0;
    let mut iterator = database.raw_iterator_cf(branches);
    iterator.seek_to_first();
    while iterator.valid() {
        let key = iterator.key().ok_or(FilesystemError::Database)?;
        maximum = maximum.max(decode_u64(key)?);
        iterator.next();
    }
    iterator.status().map_err(|_| FilesystemError::Database)?;
    Ok(maximum)
}

fn validate_last_event_sequence(
    database: &DB,
    last_event_sequence: u64,
) -> Result<(), FilesystemError> {
    let events = database
        .cf_handle(COLUMN_FAMILY_EVENTS)
        .ok_or(FilesystemError::Integrity)?;
    let mut iterator = database.raw_iterator_cf(events);
    iterator.seek_to_last();
    if !iterator.valid() {
        return Err(FilesystemError::Integrity);
    }
    let key = iterator.key().ok_or(FilesystemError::Database)?;
    if key.len() != 8 || decode_u64(key)? != last_event_sequence {
        return Err(FilesystemError::Integrity);
    }
    let value = iterator.value().ok_or(FilesystemError::Database)?;
    decode_stored_event_at_key(key, value)?;
    iterator.status().map_err(|_| FilesystemError::Database)
}

fn read_metadata_u64(database: &DB, key: &[u8]) -> Result<u64, FilesystemError> {
    let metadata = database
        .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
        .ok_or(FilesystemError::Integrity)?;
    database
        .get_pinned_cf(metadata, key)
        .map_err(|_| FilesystemError::Database)?
        .ok_or(FilesystemError::Integrity)
        .and_then(|value| decode_u64(&value))
}

fn read_metadata_string(database: &DB, key: &[u8]) -> Result<String, FilesystemError> {
    let metadata = database
        .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
        .ok_or(FilesystemError::Integrity)?;
    let value = database
        .get_pinned_cf(metadata, key)
        .map_err(|_| FilesystemError::Database)?
        .ok_or(FilesystemError::Integrity)?;
    std::str::from_utf8(&value)
        .map(str::to_owned)
        .map_err(|_| FilesystemError::Integrity)
}

fn chunk_bytes(bytes: &[u8]) -> FuseResult<Vec<PendingExtent>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut extents = Vec::new();
    if bytes.len() <= CONTENT_CHUNK_MIN_SIZE {
        extents.push(pending_extent(bytes, 0, bytes.len())?);
        return Ok(extents);
    }
    for chunk in fastcdc::v2020::FastCDC::with_level_and_seed(
        bytes,
        CONTENT_CHUNK_MIN_SIZE,
        CONTENT_CHUNK_TARGET_SIZE,
        CONTENT_CHUNK_MAX_SIZE,
        fastcdc::v2020::Normalization::Level1,
        CONTENT_CHUNK_FASTCDC_SEED,
    ) {
        extents.push(pending_extent(
            bytes,
            chunk.offset,
            chunk.offset + chunk.length,
        )?);
    }
    Ok(extents)
}

fn stored_extents_from_pending(
    logical_offset: u64,
    pending_extents: &[PendingExtent],
) -> FuseResult<Vec<StoredExtent>> {
    let mut extents = Vec::new();
    for extent in pending_extents {
        extents.push(StoredExtent {
            logical_offset: logical_offset
                .checked_add(extent.logical_offset)
                .ok_or(FuseError::Integrity)?,
            length: extent.length,
            chunk_identifier: extent.chunk_identifier.clone(),
            chunk_offset: 0,
        });
    }
    Ok(extents)
}

fn slice_extents(
    extents: &[StoredExtent],
    start: u64,
    end: u64,
    logical_base: u64,
) -> Result<Vec<StoredExtent>, FilesystemError> {
    if start >= end {
        return Ok(Vec::new());
    }
    let mut sliced = Vec::new();
    for extent in extents {
        let extent_end = extent
            .logical_offset
            .checked_add(extent.length)
            .ok_or(FilesystemError::Integrity)?;
        let slice_start = start.max(extent.logical_offset);
        let slice_end = end.min(extent_end);
        if slice_start >= slice_end {
            continue;
        }
        sliced.push(StoredExtent {
            logical_offset: logical_base
                .checked_add(slice_start - start)
                .ok_or(FilesystemError::Integrity)?,
            length: slice_end - slice_start,
            chunk_identifier: extent.chunk_identifier.clone(),
            chunk_offset: extent
                .chunk_offset
                .checked_add(slice_start - extent.logical_offset)
                .ok_or(FilesystemError::Integrity)?,
        });
    }
    Ok(sliced)
}

#[cfg(test)]
fn current_extents_after_write(
    existing: &[StoredExtent],
    old_size: u64,
    offset: u64,
    new_size: u64,
    written_extents: &[StoredExtent],
) -> FuseResult<Vec<StoredExtent>> {
    let written_length = written_extents
        .iter()
        .map(|extent| extent.length)
        .try_fold(0_u64, |total, length| total.checked_add(length))
        .ok_or(FuseError::Integrity)?;
    let end = offset
        .checked_add(written_length)
        .ok_or(FuseError::Errno(fuser::Errno::EFBIG))?;
    if offset >= old_size {
        return appended_current_extents(existing.to_vec(), new_size, written_extents);
    }
    let mut extents =
        slice_extents(existing, 0, offset.min(old_size), 0).map_err(to_fuse_integrity)?;
    extents.extend_from_slice(written_extents);
    if end < old_size {
        extents.extend(slice_extents(existing, end, old_size, end).map_err(to_fuse_integrity)?);
    }
    extents.retain(|extent| extent.logical_offset < new_size && extent.length != 0);
    Ok(extents)
}

#[cfg(test)]
fn appended_current_extents(
    mut existing: Vec<StoredExtent>,
    new_size: u64,
    written_extents: &[StoredExtent],
) -> FuseResult<Vec<StoredExtent>> {
    existing.extend_from_slice(written_extents);
    existing.retain(|extent| extent.logical_offset < new_size && extent.length != 0);
    Ok(existing)
}

fn content_manifest_child_for_extents(
    extents: &[StoredExtent],
    identifier: Vec<u8>,
) -> FuseResult<StoredContentManifestChild> {
    let first = extents.first().ok_or(FuseError::Integrity)?;
    let mut end = first
        .logical_offset
        .checked_add(first.length)
        .ok_or(FuseError::Integrity)?;
    for extent in &extents[1..] {
        end = end.max(
            extent
                .logical_offset
                .checked_add(extent.length)
                .ok_or(FuseError::Integrity)?,
        );
    }
    Ok(StoredContentManifestChild {
        logical_offset: first.logical_offset,
        length: end
            .checked_sub(first.logical_offset)
            .ok_or(FuseError::Integrity)?,
        identifier,
    })
}

fn content_manifest_child_end(
    child: &StoredContentManifestChild,
) -> Result<u64, FilesystemError> {
    child
        .logical_offset
        .checked_add(child.length)
        .ok_or(FilesystemError::Integrity)
}

fn validate_extent_bounds(extents: &[StoredExtent], byte_length: u64) -> FuseResult<()> {
    for extent in extents {
        let end = extent
            .logical_offset
            .checked_add(extent.length)
            .ok_or(FuseError::Integrity)?;
        if end > byte_length {
            return Err(FuseError::Integrity);
        }
    }
    Ok(())
}

fn pending_extent(bytes: &[u8], start: usize, end: usize) -> FuseResult<PendingExtent> {
    let mut identifier = Vec::with_capacity(33);
    identifier.push(CONTENT_CHUNK_HASH_BLAKE3);
    identifier.extend_from_slice(blake3::hash(&bytes[start..end]).as_bytes());
    Ok(PendingExtent {
        logical_offset: start as u64,
        length: (end - start) as u64,
        byte_start: start,
        byte_end: end,
        chunk_identifier: identifier,
    })
}

fn validate_content_chunk(identifier: &[u8], bytes: &[u8]) -> Result<(), FilesystemError> {
    if identifier.len() != 33 || identifier.first() != Some(&CONTENT_CHUNK_HASH_BLAKE3) {
        return Err(FilesystemError::Integrity);
    }
    if &identifier[1..] != blake3::hash(bytes).as_bytes() {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn put_namespace_state_in_database(
    database: &DB,
    batch: &mut WriteBatch,
    state: &MaterializedBranchState,
) -> Result<Vec<u8>, FilesystemError> {
    let root = StoredNamespaceRoot {
        inodes: put_namespace_map_from_entries(
            database,
            batch,
            NamespaceMapKind::Inodes,
            namespace_inode_entries(&state.inodes)?,
        )?,
        directory_entries: put_namespace_map_from_entries(
            database,
            batch,
            NamespaceMapKind::DirectoryEntries,
            namespace_directory_entry_entries(&state.directory_entries)?,
        )?,
        extended_attributes: put_namespace_map_from_entries(
            database,
            batch,
            NamespaceMapKind::ExtendedAttributes,
            namespace_extended_attribute_entries(&state.extended_attributes),
        )?,
    };
    put_namespace_node_in_database(database, batch, &StoredNamespaceNode::Root(root))
}

#[cfg(test)]
fn put_namespace_mutation_in_database(
    database: &DB,
    batch: &mut WriteBatch,
    namespace_identifier: &[u8],
    mutation: &StoredMutation,
) -> Result<Vec<u8>, FilesystemError> {
    let mut root = namespace_root_in_database(database, namespace_identifier)?;
    root.inodes = put_namespace_map_mutations(
        database,
        batch,
        NamespaceMapKind::Inodes,
        &root.inodes,
        namespace_inode_mutations(mutation)?,
    )?;
    root.directory_entries = put_namespace_map_mutations(
        database,
        batch,
        NamespaceMapKind::DirectoryEntries,
        &root.directory_entries,
        namespace_directory_entry_mutations(mutation)?,
    )?;
    root.extended_attributes = put_namespace_map_mutations(
        database,
        batch,
        NamespaceMapKind::ExtendedAttributes,
        &root.extended_attributes,
        namespace_extended_attribute_mutations(mutation),
    )?;
    put_namespace_node_in_database(database, batch, &StoredNamespaceNode::Root(root))
}

fn put_namespace_mutation_from_active_in_database(
    database: &DB,
    batch: &mut WriteBatch,
    active_root: &StoredNamespaceRoot,
    active_state: &MaterializedBranchState,
    mutation: &StoredMutation,
) -> Result<(Vec<u8>, StoredNamespaceRoot), FilesystemError> {
    let mut root = active_root.clone();
    root.inodes = put_namespace_map_mutations_with_active_entries(
        database,
        batch,
        NamespaceMapKind::Inodes,
        &root.inodes,
        namespace_inode_entries(&active_state.inodes)?,
        namespace_inode_mutations(mutation)?,
    )?;
    root.directory_entries = put_namespace_map_mutations_with_active_entries(
        database,
        batch,
        NamespaceMapKind::DirectoryEntries,
        &root.directory_entries,
        namespace_directory_entry_entries(&active_state.directory_entries)?,
        namespace_directory_entry_mutations(mutation)?,
    )?;
    root.extended_attributes = put_namespace_map_mutations_with_active_entries(
        database,
        batch,
        NamespaceMapKind::ExtendedAttributes,
        &root.extended_attributes,
        namespace_extended_attribute_entries(&active_state.extended_attributes),
        namespace_extended_attribute_mutations(mutation),
    )?;
    let identifier =
        put_namespace_node_in_database(database, batch, &StoredNamespaceNode::Root(root.clone()))?;
    Ok((identifier, root))
}

fn namespace_root_in_database(
    database: &DB,
    namespace_identifier: &[u8],
) -> Result<StoredNamespaceRoot, FilesystemError> {
    match namespace_node_in_database(database, namespace_identifier)? {
        StoredNamespaceNode::Root(root) => Ok(root),
        StoredNamespaceNode::Branch(_) | StoredNamespaceNode::Leaf(_) => {
            Err(FilesystemError::Integrity)
        }
    }
}

fn namespace_node_in_database(
    database: &DB,
    namespace_identifier: &[u8],
) -> Result<StoredNamespaceNode, FilesystemError> {
    let namespace_identifier = NamespaceIdentifier::from_bytes(namespace_identifier)?;
    let namespace_nodes = database
        .cf_handle(COLUMN_FAMILY_NAMESPACE_NODES)
        .ok_or(FilesystemError::Integrity)?;
    let value = database
        .get_pinned_cf(namespace_nodes, namespace_identifier.as_bytes())
        .map_err(|_| FilesystemError::Database)?
        .ok_or(FilesystemError::Integrity)?;
    validate_namespace(namespace_identifier.as_bytes(), &value)?;
    decode_namespace_node(&value)
}

fn put_namespace_node_in_database(
    database: &DB,
    batch: &mut WriteBatch,
    node: &StoredNamespaceNode,
) -> Result<Vec<u8>, FilesystemError> {
    let bytes = encode_namespace_node(node)?;
    let identifier = namespace_identifier_for_bytes(&bytes);
    let namespace_nodes = database
        .cf_handle(COLUMN_FAMILY_NAMESPACE_NODES)
        .ok_or(FilesystemError::Integrity)?;
    batch.put_cf(namespace_nodes, &identifier, bytes);
    Ok(identifier)
}

fn put_namespace_map_mutations(
    database: &DB,
    batch: &mut WriteBatch,
    kind: NamespaceMapKind,
    root_identifier: &[u8],
    mutations: Vec<NamespaceMapMutation>,
) -> Result<Vec<u8>, FilesystemError> {
    if mutations.len() > 1 {
        let mut entries = namespace_map_entries_in_database(database, root_identifier)?;
        for mutation in mutations {
            match mutation {
                NamespaceMapMutation::Put { key, value } => {
                    entries.insert(key, value);
                }
                NamespaceMapMutation::Delete { key } => {
                    entries.remove(&key);
                }
            }
        }
        return put_namespace_map_from_entries(
            database,
            batch,
            kind,
            entries
                .into_iter()
                .map(|(key, value)| StoredNamespaceEntry { key, value })
                .collect(),
        );
    }

    let mut identifier = root_identifier.to_vec();
    for mutation in mutations {
        identifier = match mutation {
            NamespaceMapMutation::Put { key, value } => {
                let path = namespace_map_entry_path(kind, &key);
                put_namespace_map_entry_at(
                    database,
                    batch,
                    kind,
                    &identifier,
                    StoredNamespaceEntry { key, value },
                    path,
                    0,
                )?
            }
            NamespaceMapMutation::Delete { key } => {
                let path = namespace_map_entry_path(kind, &key);
                delete_namespace_map_entry_at(database, batch, kind, &identifier, &key, path, 0)?.0
            }
        };
    }
    Ok(identifier)
}

fn put_namespace_map_mutations_with_active_entries(
    database: &DB,
    batch: &mut WriteBatch,
    kind: NamespaceMapKind,
    root_identifier: &[u8],
    active_entries: Vec<StoredNamespaceEntry>,
    mutations: Vec<NamespaceMapMutation>,
) -> Result<Vec<u8>, FilesystemError> {
    if mutations.is_empty() {
        return Ok(root_identifier.to_vec());
    }
    if active_entries.len() <= NAMESPACE_MAP_LEAF_MAX_ENTRIES || mutations.len() > 1 {
        let mut entries = BTreeMap::new();
        for entry in active_entries {
            if entries.insert(entry.key, entry.value).is_some() {
                return Err(FilesystemError::Integrity);
            }
        }
        for mutation in mutations {
            match mutation {
                NamespaceMapMutation::Put { key, value } => {
                    entries.insert(key, value);
                }
                NamespaceMapMutation::Delete { key } => {
                    entries.remove(&key);
                }
            }
        }
        return put_namespace_map_from_entries(
            database,
            batch,
            kind,
            entries
                .into_iter()
                .map(|(key, value)| StoredNamespaceEntry { key, value })
                .collect(),
        );
    }
    put_namespace_map_mutations(database, batch, kind, root_identifier, mutations)
}

fn namespace_map_entries_in_database(
    database: &DB,
    root_identifier: &[u8],
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, FilesystemError> {
    let mut entries = BTreeMap::new();
    collect_namespace_map_entries(database, root_identifier, &mut entries)?;
    Ok(entries)
}

fn collect_namespace_map_entries(
    database: &DB,
    node_identifier: &[u8],
    entries: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<(), FilesystemError> {
    match namespace_node_in_database(database, node_identifier)? {
        StoredNamespaceNode::Leaf(leaf_entries) => {
            validate_namespace_entries(&leaf_entries)?;
            for entry in leaf_entries {
                if entries.insert(entry.key, entry.value).is_some() {
                    return Err(FilesystemError::Integrity);
                }
            }
            Ok(())
        }
        StoredNamespaceNode::Branch(children) => {
            validate_namespace_children(&children)?;
            for child in children {
                collect_namespace_map_entries(database, &child.identifier, entries)?;
            }
            Ok(())
        }
        StoredNamespaceNode::Root(_) => Err(FilesystemError::Integrity),
    }
}

fn put_namespace_map_from_entries(
    database: &DB,
    batch: &mut WriteBatch,
    kind: NamespaceMapKind,
    entries: Vec<StoredNamespaceEntry>,
) -> Result<Vec<u8>, FilesystemError> {
    put_namespace_map_entries_at(database, batch, kind, entries, 0)
}

fn put_namespace_map_entry_at(
    database: &DB,
    batch: &mut WriteBatch,
    kind: NamespaceMapKind,
    node_identifier: &[u8],
    entry: StoredNamespaceEntry,
    path: [u8; 32],
    depth: usize,
) -> Result<Vec<u8>, FilesystemError> {
    match namespace_node_in_database(database, node_identifier)? {
        StoredNamespaceNode::Leaf(mut entries) => {
            validate_namespace_entries(&entries)?;
            match entries
                .binary_search_by(|existing| existing.key.as_slice().cmp(entry.key.as_slice()))
            {
                Ok(index) => entries[index].value = entry.value,
                Err(index) => entries.insert(index, entry),
            }
            put_namespace_map_entries_at(database, batch, kind, entries, depth)
        }
        StoredNamespaceNode::Branch(mut children) => {
            validate_namespace_children(&children)?;
            let nibble = namespace_path_nibble(path, depth)?;
            match children.binary_search_by_key(&nibble, |child| child.nibble) {
                Ok(index) => {
                    let child_identifier = put_namespace_map_entry_at(
                        database,
                        batch,
                        kind,
                        &children[index].identifier,
                        entry,
                        path,
                        depth + 1,
                    )?;
                    children[index].identifier = child_identifier;
                }
                Err(index) => {
                    let child_identifier = put_namespace_map_entries_at(
                        database,
                        batch,
                        kind,
                        vec![entry],
                        depth + 1,
                    )?;
                    children.insert(
                        index,
                        StoredNamespaceChild {
                            nibble,
                            identifier: child_identifier,
                        },
                    );
                }
            }
            put_namespace_node_in_database(database, batch, &StoredNamespaceNode::Branch(children))
        }
        StoredNamespaceNode::Root(_) => Err(FilesystemError::Integrity),
    }
}

fn delete_namespace_map_entry_at(
    database: &DB,
    batch: &mut WriteBatch,
    kind: NamespaceMapKind,
    node_identifier: &[u8],
    key: &[u8],
    path: [u8; 32],
    depth: usize,
) -> Result<(Vec<u8>, bool), FilesystemError> {
    match namespace_node_in_database(database, node_identifier)? {
        StoredNamespaceNode::Leaf(mut entries) => {
            validate_namespace_entries(&entries)?;
            if let Ok(index) = entries.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
                entries.remove(index);
            }
            let is_empty = entries.is_empty();
            let identifier = put_namespace_map_entries_at(database, batch, kind, entries, depth)?;
            Ok((identifier, is_empty))
        }
        StoredNamespaceNode::Branch(mut children) => {
            validate_namespace_children(&children)?;
            let nibble = namespace_path_nibble(path, depth)?;
            if let Ok(index) = children.binary_search_by_key(&nibble, |child| child.nibble) {
                let (child_identifier, child_is_empty) = delete_namespace_map_entry_at(
                    database,
                    batch,
                    kind,
                    &children[index].identifier,
                    key,
                    path,
                    depth + 1,
                )?;
                if child_is_empty {
                    children.remove(index);
                } else {
                    children[index].identifier = child_identifier;
                }
            }
            if children.is_empty() {
                let identifier = put_namespace_map_entries_at(database, batch, kind, Vec::new(), depth)?;
                return Ok((identifier, true));
            }
            let identifier =
                put_namespace_node_in_database(database, batch, &StoredNamespaceNode::Branch(children))?;
            Ok((identifier, false))
        }
        StoredNamespaceNode::Root(_) => Err(FilesystemError::Integrity),
    }
}

fn put_namespace_map_entries_at(
    database: &DB,
    batch: &mut WriteBatch,
    kind: NamespaceMapKind,
    entries: Vec<StoredNamespaceEntry>,
    depth: usize,
) -> Result<Vec<u8>, FilesystemError> {
    validate_namespace_entries(&entries)?;
    if entries.len() <= NAMESPACE_MAP_LEAF_MAX_ENTRIES || depth >= 64 {
        return put_namespace_node_in_database(database, batch, &StoredNamespaceNode::Leaf(entries));
    }

    let mut grouped: BTreeMap<u8, Vec<StoredNamespaceEntry>> = BTreeMap::new();
    for entry in entries {
        let path = namespace_map_entry_path(kind, &entry.key);
        grouped
            .entry(namespace_path_nibble(path, depth)?)
            .or_default()
            .push(entry);
    }

    let mut children = Vec::with_capacity(grouped.len());
    for (nibble, entries) in grouped {
        children.push(StoredNamespaceChild {
            nibble,
            identifier: put_namespace_map_entries_at(database, batch, kind, entries, depth + 1)?,
        });
    }
    put_namespace_node_in_database(database, batch, &StoredNamespaceNode::Branch(children))
}

fn stored_branch_in_database(
    database: &DB,
    branch_identifier: BranchIdentifier,
) -> Result<StoredBranch, FilesystemError> {
    let branches = database
        .cf_handle(COLUMN_FAMILY_BRANCHES)
        .ok_or(FilesystemError::Integrity)?;
    database
        .get_pinned_cf(branches, encode_u64(branch_identifier.get()))
        .map_err(|_| FilesystemError::Database)?
        .ok_or(FilesystemError::Integrity)
        .and_then(|value| decode_branch(&value))
}

fn branch_root_at_position_in_database(
    database: &DB,
    position: BranchPosition,
) -> Result<StoredBranchRoot, FilesystemError> {
    let branch_roots = database
        .cf_handle(COLUMN_FAMILY_BRANCH_ROOTS)
        .ok_or(FilesystemError::Integrity)?;
    database
        .get_pinned_cf(
            branch_roots,
            encode_branch_position_key(position.branch_identifier().get(), position.ordinal()),
        )
        .map_err(|_| FilesystemError::Database)?
        .ok_or(FilesystemError::Integrity)
        .and_then(|value| decode_branch_root(&value))
}

fn namespace_state_for_identifier_in_database(
    database: &DB,
    namespace_identifier: &[u8],
) -> Result<MaterializedBranchState, FilesystemError> {
    let root = namespace_root_in_database(database, namespace_identifier)?;
    let mut state = MaterializedBranchState::default();
    load_namespace_map_into_state(database, NamespaceMapKind::Inodes, &root.inodes, &mut state)?;
    load_namespace_map_into_state(
        database,
        NamespaceMapKind::DirectoryEntries,
        &root.directory_entries,
        &mut state,
    )?;
    load_namespace_map_into_state(
        database,
        NamespaceMapKind::ExtendedAttributes,
        &root.extended_attributes,
        &mut state,
    )?;
    Ok(state)
}

fn load_namespace_map_into_state(
    database: &DB,
    kind: NamespaceMapKind,
    root_identifier: &[u8],
    state: &mut MaterializedBranchState,
) -> Result<(), FilesystemError> {
    match namespace_node_in_database(database, root_identifier)? {
        StoredNamespaceNode::Leaf(entries) => {
            validate_namespace_entries(&entries)?;
            for entry in entries {
                insert_namespace_entry_into_state(kind, entry, state)?;
            }
            Ok(())
        }
        StoredNamespaceNode::Branch(children) => {
            validate_namespace_children(&children)?;
            for child in children {
                load_namespace_map_into_state(database, kind, &child.identifier, state)?;
            }
            Ok(())
        }
        StoredNamespaceNode::Root(_) => Err(FilesystemError::Integrity),
    }
}

fn insert_namespace_entry_into_state(
    kind: NamespaceMapKind,
    entry: StoredNamespaceEntry,
    state: &mut MaterializedBranchState,
) -> Result<(), FilesystemError> {
    match kind {
        NamespaceMapKind::Inodes => {
            let inode_number = decode_namespace_inode_key(&entry.key)?;
            let inode = decode_namespace_inode_value(&entry.value)?;
            state.inodes.insert(inode_number, inode);
        }
        NamespaceMapKind::DirectoryEntries => {
            let key = decode_namespace_pair_key(&entry.key)?;
            let entry = decode_namespace_directory_entry_value(&entry.value)?;
            state.directory_entries.insert(key, entry);
        }
        NamespaceMapKind::ExtendedAttributes => {
            let key = decode_namespace_pair_key(&entry.key)?;
            state.extended_attributes.insert(key, entry.value);
        }
    }
    Ok(())
}

fn namespace_inode_entries(
    inodes: &BTreeMap<u64, StoredInode>,
) -> Result<Vec<StoredNamespaceEntry>, FilesystemError> {
    inodes
        .iter()
        .map(|(inode_number, inode)| {
            Ok(StoredNamespaceEntry {
                key: encode_namespace_inode_key(*inode_number),
                value: encode_namespace_inode_value(inode)?,
            })
        })
        .collect()
}

fn namespace_directory_entry_entries(
    entries: &BTreeMap<(u64, Vec<u8>), StoredDirectoryEntry>,
) -> Result<Vec<StoredNamespaceEntry>, FilesystemError> {
    entries
        .iter()
        .map(|((parent, name), entry)| {
            Ok(StoredNamespaceEntry {
                key: encode_namespace_pair_key(*parent, name),
                value: encode_namespace_directory_entry_value(entry)?,
            })
        })
        .collect()
}

fn namespace_extended_attribute_entries(
    attributes: &BTreeMap<(u64, Vec<u8>), Vec<u8>>,
) -> Vec<StoredNamespaceEntry> {
    attributes
        .iter()
        .map(|((inode_number, name), value)| StoredNamespaceEntry {
            key: encode_namespace_pair_key(*inode_number, name),
            value: value.clone(),
        })
        .collect()
}

fn namespace_inode_mutations(
    mutation: &StoredMutation,
) -> Result<Vec<NamespaceMapMutation>, FilesystemError> {
    let mut mutations =
        Vec::with_capacity(mutation.inode_deletes.len() + mutation.inode_puts.len());
    for inode_number in &mutation.inode_deletes {
        mutations.push(NamespaceMapMutation::Delete {
            key: encode_namespace_inode_key(*inode_number),
        });
    }
    for put in &mutation.inode_puts {
        mutations.push(NamespaceMapMutation::Put {
            key: encode_namespace_inode_key(put.inode_number),
            value: encode_namespace_inode_value(&put.inode)?,
        });
    }
    Ok(mutations)
}

fn namespace_directory_entry_mutations(
    mutation: &StoredMutation,
) -> Result<Vec<NamespaceMapMutation>, FilesystemError> {
    let mut mutations = Vec::with_capacity(
        mutation.directory_entry_deletes.len() + mutation.directory_entry_puts.len(),
    );
    for delete in &mutation.directory_entry_deletes {
        mutations.push(NamespaceMapMutation::Delete {
            key: encode_namespace_pair_key(delete.parent, &delete.name),
        });
    }
    for put in &mutation.directory_entry_puts {
        mutations.push(NamespaceMapMutation::Put {
            key: encode_namespace_pair_key(put.parent, &put.name),
            value: encode_namespace_directory_entry_value(&put.entry)?,
        });
    }
    Ok(mutations)
}

fn namespace_extended_attribute_mutations(
    mutation: &StoredMutation,
) -> Vec<NamespaceMapMutation> {
    let mut mutations = Vec::with_capacity(
        mutation.extended_attribute_deletes.len() + mutation.extended_attribute_puts.len(),
    );
    for delete in &mutation.extended_attribute_deletes {
        mutations.push(NamespaceMapMutation::Delete {
            key: encode_namespace_pair_key(delete.inode_number, &delete.name),
        });
    }
    for put in &mutation.extended_attribute_puts {
        mutations.push(NamespaceMapMutation::Put {
            key: encode_namespace_pair_key(put.inode_number, &put.name),
            value: put.value.clone(),
        });
    }
    mutations
}

fn encode_namespace_inode_key(inode_number: u64) -> Vec<u8> {
    encode_u64(inode_number).to_vec()
}

fn decode_namespace_inode_key(key: &[u8]) -> Result<u64, FilesystemError> {
    decode_u64(key)
}

fn encode_namespace_pair_key(inode_number: u64, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + name.len());
    key.extend_from_slice(&encode_u64(inode_number));
    key.extend_from_slice(name);
    key
}

fn decode_namespace_pair_key(key: &[u8]) -> Result<(u64, Vec<u8>), FilesystemError> {
    if key.len() < 8 {
        return Err(FilesystemError::Integrity);
    }
    Ok((decode_u64(&key[..8])?, key[8..].to_vec()))
}

fn encode_namespace_inode_value(inode: &StoredInode) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(inode).map_err(|_| FilesystemError::Database)
}

fn decode_namespace_inode_value(value: &[u8]) -> Result<StoredInode, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_namespace_directory_entry_value(
    entry: &StoredDirectoryEntry,
) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(entry).map_err(|_| FilesystemError::Database)
}

fn decode_namespace_directory_entry_value(
    value: &[u8],
) -> Result<StoredDirectoryEntry, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn namespace_map_entry_path(kind: NamespaceMapKind, key: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[kind.tag()]);
    hasher.update(key);
    *hasher.finalize().as_bytes()
}

fn namespace_path_nibble(path: [u8; 32], depth: usize) -> Result<u8, FilesystemError> {
    let byte = path.get(depth / 2).ok_or(FilesystemError::Integrity)?;
    if depth.is_multiple_of(2) {
        Ok(byte >> 4)
    } else {
        Ok(byte & 0x0f)
    }
}

fn validate_namespace_entries(entries: &[StoredNamespaceEntry]) -> Result<(), FilesystemError> {
    for pair in entries.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(FilesystemError::Integrity);
        }
    }
    Ok(())
}

fn validate_namespace_children(children: &[StoredNamespaceChild]) -> Result<(), FilesystemError> {
    for child in children {
        if child.nibble > 0x0f {
            return Err(FilesystemError::Integrity);
        }
        NamespaceIdentifier::from_bytes(&child.identifier)?;
    }
    for pair in children.windows(2) {
        if pair[0].nibble >= pair[1].nibble {
            return Err(FilesystemError::Integrity);
        }
    }
    Ok(())
}

impl NamespaceMapKind {
    fn tag(self) -> u8 {
        match self {
            Self::Inodes => NAMESPACE_MAP_TAG_INODES,
            Self::DirectoryEntries => NAMESPACE_MAP_TAG_DIRECTORY_ENTRIES,
            Self::ExtendedAttributes => NAMESPACE_MAP_TAG_EXTENDED_ATTRIBUTES,
        }
    }
}

fn child_event_path(
    parent: u64,
    name: &[u8],
    state: &MaterializedBranchState,
) -> FuseResult<String> {
    let mut bytes = inode_path_bytes(parent, state)?;
    if bytes.len() > 1 {
        bytes.push(b'/');
    }
    bytes.extend_from_slice(name);
    String::from_utf8(bytes).map_err(|_| FuseError::Errno(fuser::Errno::EINVAL))
}

fn inode_event_path(
    inode_number: u64,
    inode: &StoredInode,
    state: &MaterializedBranchState,
) -> FuseResult<String> {
    if inode_number == INODE_ROOT {
        return Ok("/".to_owned());
    }
    child_event_path(inode.parent, &inode.name, state)
}

fn inode_path_bytes(inode_number: u64, state: &MaterializedBranchState) -> FuseResult<Vec<u8>> {
    if inode_number == INODE_ROOT {
        return Ok(vec![b'/']);
    }
    let mut current = inode_number;
    let mut visited = BTreeSet::new();
    let mut components: Vec<Vec<u8>> = Vec::new();
    for _ in 0..1024 {
        if current == INODE_ROOT {
            let mut path = vec![b'/'];
            for component in components.iter().rev() {
                if path.len() > 1 {
                    path.push(b'/');
                }
                path.extend_from_slice(component);
            }
            return Ok(path);
        }
        if !visited.insert(current) {
            return Err(FuseError::Integrity);
        }
        let inode = state
            .inodes
            .get(&current)
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        components.push(inode.name.clone());
        current = inode.parent;
    }
    Err(FuseError::Integrity)
}

fn inode_attr(inode_number: u64, inode: &StoredInode) -> fuser::FileAttr {
    fuser::FileAttr {
        ino: fuser::INodeNo(inode_number),
        size: inode.size,
        blocks: inode.size.div_ceil(512),
        atime: inode.atime.to_system_time(),
        mtime: inode.mtime.to_system_time(),
        ctime: inode.ctime.to_system_time(),
        crtime: UNIX_EPOCH,
        kind: fuser_file_type(inode.kind),
        perm: inode.mode,
        nlink: inode.nlink,
        uid: inode.uid,
        gid: inode.gid,
        rdev: inode.rdev,
        blksize: FUSE_STATFS_BLOCK_SIZE,
        flags: 0,
    }
}

fn fuser_file_type(kind: StoredNodeKind) -> fuser::FileType {
    match kind {
        StoredNodeKind::NamedPipe => fuser::FileType::NamedPipe,
        StoredNodeKind::CharacterDevice => fuser::FileType::CharDevice,
        StoredNodeKind::BlockDevice => fuser::FileType::BlockDevice,
        StoredNodeKind::Directory => fuser::FileType::Directory,
        StoredNodeKind::RegularFile => fuser::FileType::RegularFile,
        StoredNodeKind::Symlink => fuser::FileType::Symlink,
        StoredNodeKind::Socket => fuser::FileType::Socket,
    }
}

fn created_inode_metadata(
    parent: &StoredInode,
    kind: StoredNodeKind,
    metadata: CreateNodeMetadata,
) -> (u32, u32, u16) {
    let parent_setgid = parent.mode & setgid_mode_bit() != 0;
    let gid = if parent_setgid {
        parent.gid
    } else {
        metadata.gid
    };
    let mut mode = permission_mode(metadata.mode, metadata.umask);
    if kind == StoredNodeKind::Directory && parent_setgid {
        mode |= setgid_mode_bit();
    }
    (metadata.uid, gid, mode)
}

fn validate_metadata_change(inode: &StoredInode, metadata: SetattrMetadata) -> FuseResult<()> {
    let owner_or_root = metadata.request_uid == 0 || metadata.request_uid == inode.uid;
    let uid_change = metadata.uid.is_some_and(|uid| uid != inode.uid);
    let gid_change = metadata.gid.is_some_and(|gid| gid != inode.gid);
    let mode_change = metadata
        .mode
        .map(|mode| permission_mode(mode, 0) != inode.mode)
        .unwrap_or(false);

    if uid_change && metadata.request_uid != 0 {
        return Err(FuseError::Errno(fuser::Errno::EPERM));
    }
    if gid_change
        && metadata.request_uid != 0
        && !(metadata.request_uid == inode.uid && metadata.gid == Some(metadata.request_gid))
    {
        return Err(FuseError::Errno(fuser::Errno::EPERM));
    }
    if mode_change && !owner_or_root {
        return Err(FuseError::Errno(fuser::Errno::EPERM));
    }
    Ok(())
}

fn access_allowed(
    inode: &StoredInode,
    request_uid: u32,
    request_gid: u32,
    mask: fuser::AccessFlags,
) -> bool {
    if mask.is_empty() || request_uid == 0 {
        return true;
    }
    let shift = if request_uid == inode.uid {
        6
    } else if request_gid == inode.gid {
        3
    } else {
        0
    };
    let permissions = ((inode.mode >> shift) & 0o7) as i32;
    if mask.contains(fuser::AccessFlags::R_OK) && permissions & libc::R_OK == 0 {
        return false;
    }
    if mask.contains(fuser::AccessFlags::W_OK) && permissions & libc::W_OK == 0 {
        return false;
    }
    if mask.contains(fuser::AccessFlags::X_OK) && permissions & libc::X_OK == 0 {
        return false;
    }
    true
}

fn permission_mode(mode: u32, umask: u32) -> u16 {
    ((mode & !umask) & INODE_MODE_PERMISSION_MASK) as u16
}

fn setuid_mode_bit() -> u16 {
    libc::S_ISUID as u16
}

fn setgid_mode_bit() -> u16 {
    libc::S_ISGID as u16
}

fn current_effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

fn current_effective_gid() -> u32 {
    rustix::process::getegid().as_raw()
}

fn special_kind_from_mode(mode: u32) -> FuseResult<StoredNodeKind> {
    match mode & libc::S_IFMT {
        value if value == libc::S_IFREG => Ok(StoredNodeKind::RegularFile),
        value if value == libc::S_IFIFO => Ok(StoredNodeKind::NamedPipe),
        value if value == libc::S_IFCHR => Ok(StoredNodeKind::CharacterDevice),
        value if value == libc::S_IFBLK => Ok(StoredNodeKind::BlockDevice),
        value if value == libc::S_IFSOCK => Ok(StoredNodeKind::Socket),
        _ => Err(FuseError::Errno(fuser::Errno::EINVAL)),
    }
}

fn file_identifier_for_kind(inode_number: u64, kind: StoredNodeKind) -> Option<FileIdentifier> {
    if kind == StoredNodeKind::RegularFile {
        Some(FileIdentifier::new(inode_number))
    } else {
        None
    }
}

fn validate_name(name: &OsStr) -> FuseResult<Vec<u8>> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(FuseError::Errno(fuser::Errno::ENOENT));
    }
    if bytes.len() > FUSE_MAX_NAME_LENGTH {
        return Err(FuseError::Errno(fuser::Errno::ENAMETOOLONG));
    }
    if bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
        return Err(FuseError::Errno(fuser::Errno::EINVAL));
    }
    if std::str::from_utf8(bytes).is_err() {
        return Err(FuseError::Errno(fuser::Errno::EINVAL));
    }
    Ok(bytes.to_vec())
}

fn validate_extended_attribute_name(name: &OsStr) -> FuseResult<Vec<u8>> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(FuseError::Errno(fuser::Errno::EINVAL));
    }
    if bytes.len() > XATTR_NAME_MAX_BYTES {
        return Err(FuseError::Errno(fuser::Errno::ERANGE));
    }
    if bytes.contains(&0) {
        return Err(FuseError::Errno(fuser::Errno::EINVAL));
    }
    if std::str::from_utf8(bytes).is_err() || !bytes.starts_with(XATTR_USER_PREFIX) {
        return Err(FuseError::Errno(fuser::Errno::ENOTSUP));
    }
    Ok(bytes.to_vec())
}

fn validate_extended_attribute_inode(inode: &StoredInode) -> FuseResult<()> {
    match inode.kind {
        StoredNodeKind::Directory | StoredNodeKind::RegularFile => Ok(()),
        _ => Err(FuseError::Errno(fuser::Errno::ENOTSUP)),
    }
}

fn validate_extended_attribute_value(value: &[u8]) -> FuseResult<()> {
    if value.len() > XATTR_VALUE_MAX_BYTES {
        return Err(FuseError::Errno(fuser::Errno::ERANGE));
    }
    Ok(())
}

fn backing_filesystem_statistics(path: &Path) -> FuseResult<BackingFileSystemStatistics> {
    let statistics = rustix::fs::statvfs(path).map_err(|_| FuseError::Database)?;
    Ok(BackingFileSystemStatistics {
        blocks: statistics.f_blocks,
        free_blocks: statistics.f_bfree,
        available_blocks: statistics.f_bavail,
        fragment_size: u32::try_from(statistics.f_frsize).map_err(|_| FuseError::Database)?,
    })
}

fn statistics_from_backing(
    backing: BackingFileSystemStatistics,
    next_inode_number: u64,
) -> FuseResult<FileSystemStatistics> {
    if next_inode_number < INODE_FIRST_ALLOCATED {
        return Err(FuseError::Integrity);
    }
    Ok(FileSystemStatistics {
        blocks: backing.blocks,
        free_blocks: backing.free_blocks,
        available_blocks: backing.available_blocks,
        files: INODE_LOGICAL_CAPACITY,
        free_files: u64::MAX.saturating_sub(next_inode_number),
        block_size: FUSE_STATFS_BLOCK_SIZE,
        maximum_name_length: FUSE_MAX_NAME_LENGTH as u32,
        fragment_size: backing.fragment_size,
    })
}

fn next_inode_number_after(inode_number: u64) -> FuseResult<u64> {
    inode_number
        .checked_add(1)
        .ok_or(FuseError::Errno(fuser::Errno::ENOSPC))
}

impl StoredTime {
    fn now() -> Self {
        Self::from_system_time(SystemTime::now())
    }

    fn from_system_time(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => Self {
                seconds: duration.as_secs().min(i64::MAX as u64) as i64,
                nanoseconds: duration.subsec_nanos(),
            },
            Err(error) => {
                let duration = error.duration();
                let seconds = duration.as_secs().min(i64::MAX as u64) as i64;
                Self {
                    seconds: -seconds,
                    nanoseconds: duration.subsec_nanos(),
                }
            }
        }
    }

    fn to_system_time(self) -> SystemTime {
        if self.seconds >= 0 {
            UNIX_EPOCH + Duration::new(self.seconds as u64, self.nanoseconds)
        } else {
            UNIX_EPOCH - Duration::new(self.seconds.unsigned_abs(), self.nanoseconds)
        }
    }
}

fn existing_byte_length_for_storage(length: u64) -> Result<usize, FilesystemError> {
    usize::try_from(length).map_err(|_| FilesystemError::Integrity)
}

fn to_fuse_integrity(_error: FilesystemError) -> FuseError {
    FuseError::Integrity
}

fn write_batch(database: &DB, batch: WriteBatch) -> Result<(), FilesystemError> {
    write_batch_with_durability(database, batch, WriteDurability::Synchronous)
}

fn write_batch_with_durability(
    database: &DB,
    batch: WriteBatch,
    durability: WriteDurability,
) -> Result<(), FilesystemError> {
    #[cfg(test)]
    record_write_batch_durability(durability);

    #[cfg(test)]
    if take_write_batch_fault(WriteBatchFault::BeforeWrite) {
        return Err(FilesystemError::Database);
    }

    let mut write_options = WriteOptions::default();
    write_options.set_sync(matches!(durability, WriteDurability::Synchronous));
    write_options.disable_wal(false);
    let result = database
        .write_opt(batch, &write_options)
        .map_err(|_| FilesystemError::Database);
    result?;

    #[cfg(test)]
    if take_write_batch_fault(WriteBatchFault::AfterWrite) {
        return Err(FilesystemError::Database);
    }

    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteBatchFault {
    BeforeWrite,
    AfterWrite,
}

#[cfg(test)]
thread_local! {
    static WRITE_BATCH_FAULT: Cell<Option<WriteBatchFault>> = const { Cell::new(None) };
    static WRITE_BATCH_DURABILITY: Cell<Option<WriteDurability>> = const { Cell::new(None) };
}

#[cfg(test)]
fn set_write_batch_fault(fault: WriteBatchFault) {
    WRITE_BATCH_FAULT.with(|slot| slot.set(Some(fault)));
}

#[cfg(test)]
fn take_write_batch_fault(fault: WriteBatchFault) -> bool {
    WRITE_BATCH_FAULT.with(|slot| {
        if slot.get() == Some(fault) {
            slot.set(None);
            true
        } else {
            false
        }
    })
}

#[cfg(test)]
fn record_write_batch_durability(durability: WriteDurability) {
    WRITE_BATCH_DURABILITY.with(|slot| slot.set(Some(durability)));
}

#[cfg(test)]
fn take_write_batch_durability() -> Option<WriteDurability> {
    WRITE_BATCH_DURABILITY.with(|slot| {
        let durability = slot.get();
        slot.set(None);
        durability
    })
}

fn encode_stored_event(event: &StoredEvent) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(event).map_err(|_| FilesystemError::Database)
}

fn decode_stored_event(value: &[u8]) -> Result<StoredEvent, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn decode_stored_event_at_key(key: &[u8], value: &[u8]) -> Result<StoredEvent, FilesystemError> {
    let sequence = decode_u64(key)?;
    let event = decode_stored_event(value)?;
    if event.record.sequence().get() != sequence {
        return Err(FilesystemError::Integrity);
    }
    Ok(event)
}

fn encode_branch(branch: &StoredBranch) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(branch).map_err(|_| FilesystemError::Database)
}

fn decode_branch(value: &[u8]) -> Result<StoredBranch, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_branch_root(root: &StoredBranchRoot) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(root).map_err(|_| FilesystemError::Database)
}

fn decode_branch_root(value: &[u8]) -> Result<StoredBranchRoot, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_snapshot_metadata(metadata: &StoredSnapshotMetadata) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(metadata).map_err(|_| FilesystemError::Database)
}

fn decode_snapshot_metadata(value: &[u8]) -> Result<StoredSnapshotMetadata, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_event_payload_manifest(
    manifest: &StoredEventPayloadManifest,
) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(manifest).map_err(|_| FilesystemError::Database)
}

fn decode_event_payload_manifest(
    value: &[u8],
) -> Result<StoredEventPayloadManifest, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_content_manifest(extents: &[StoredExtent]) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(extents).map_err(|_| FilesystemError::Database)
}

fn decode_content_manifest(value: &[u8]) -> Result<Vec<StoredExtent>, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_content_manifest_node(
    node: &StoredContentManifestNode,
) -> Result<Vec<u8>, FilesystemError> {
    validate_stored_content_manifest_node(node)?;
    let mut bytes = CONTENT_MANIFEST_NODE_MAGIC.to_vec();
    bytes.extend(postcard::to_allocvec(node).map_err(|_| FilesystemError::Database)?);
    Ok(bytes)
}

fn decode_content_manifest_entry(value: &[u8]) -> Result<DecodedContentManifest, FilesystemError> {
    match decode_versioned_content_manifest_node(value)? {
        Some(node) => Ok(DecodedContentManifest::Node(node)),
        None => decode_content_manifest(value).map(DecodedContentManifest::Extents),
    }
}

fn decode_versioned_content_manifest_node(
    value: &[u8],
) -> Result<Option<StoredContentManifestNode>, FilesystemError> {
    let Some(value) = value.strip_prefix(CONTENT_MANIFEST_NODE_MAGIC) else {
        return Ok(None);
    };
    let node = postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)?;
    validate_stored_content_manifest_node(&node)?;
    Ok(Some(node))
}

fn validate_stored_content_manifest_node(
    node: &StoredContentManifestNode,
) -> Result<(), FilesystemError> {
    match node {
        StoredContentManifestNode::Concat { children } => {
            if children.is_empty() || children.len() > CONTENT_MANIFEST_CONCAT_MAX_CHILDREN {
                return Err(FilesystemError::Integrity);
            }
            let mut previous_end = 0;
            for child in children {
                ContentManifestIdentifier::from_bytes(&child.identifier)?;
                if child.length == 0 || child.logical_offset < previous_end {
                    return Err(FilesystemError::Integrity);
                }
                previous_end = child
                    .logical_offset
                    .checked_add(child.length)
                    .ok_or(FilesystemError::Integrity)?;
            }
            Ok(())
        }
    }
}

fn content_manifest_identifier_for_bytes(bytes: &[u8]) -> Vec<u8> {
    prefixed_blake3_identifier(CONTENT_MANIFEST_HASH_BLAKE3, bytes)
}

fn empty_content_manifest_identifier() -> Result<Vec<u8>, FilesystemError> {
    encode_content_manifest(&[]).map(|bytes| content_manifest_identifier_for_bytes(&bytes))
}

fn validate_content_manifest(identifier: &[u8], bytes: &[u8]) -> Result<(), FilesystemError> {
    ContentManifestIdentifier::from_bytes(identifier)?;
    validate_prefixed_blake3_identifier(identifier, CONTENT_MANIFEST_HASH_BLAKE3, bytes)
}

fn encode_namespace_node(node: &StoredNamespaceNode) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(node).map_err(|_| FilesystemError::Database)
}

fn decode_namespace_node(value: &[u8]) -> Result<StoredNamespaceNode, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn namespace_identifier_for_bytes(bytes: &[u8]) -> Vec<u8> {
    prefixed_blake3_identifier(NAMESPACE_HASH_BLAKE3, bytes)
}

fn validate_namespace(identifier: &[u8], bytes: &[u8]) -> Result<(), FilesystemError> {
    NamespaceIdentifier::from_bytes(identifier)?;
    validate_prefixed_blake3_identifier(identifier, NAMESPACE_HASH_BLAKE3, bytes)
}

fn prefixed_blake3_identifier(prefix: u8, bytes: &[u8]) -> Vec<u8> {
    let mut identifier = Vec::with_capacity(33);
    identifier.push(prefix);
    identifier.extend_from_slice(blake3::hash(bytes).as_bytes());
    identifier
}

fn validate_prefixed_blake3_identifier(
    identifier: &[u8],
    prefix: u8,
    bytes: &[u8],
) -> Result<(), FilesystemError> {
    validate_prefixed_blake3_identifier_shape(identifier, prefix)?;
    if &identifier[1..] != blake3::hash(bytes).as_bytes() {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn validate_prefixed_blake3_identifier_shape(
    identifier: &[u8],
    prefix: u8,
) -> Result<(), FilesystemError> {
    if identifier.len() != 33 || identifier.first() != Some(&prefix) {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(value: &[u8]) -> Result<u64, FilesystemError> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| FilesystemError::Integrity)?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_branch_position_ordinal(key: &[u8]) -> Result<u64, FilesystemError> {
    if key.len() != 16 {
        return Err(FilesystemError::Integrity);
    }
    decode_u64(&key[8..])
}

fn decode_branch_file_position_key(key: &[u8]) -> Result<(u64, u64, u64), FilesystemError> {
    if key.len() != 24 {
        return Err(FilesystemError::Integrity);
    }
    Ok((
        decode_u64(&key[..8])?,
        decode_u64(&key[8..16])?,
        decode_u64(&key[16..])?,
    ))
}

fn encode_file_sequence_key(file_identifier: u64, event_sequence: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&file_identifier.to_be_bytes());
    key[8..].copy_from_slice(&event_sequence.to_be_bytes());
    key
}

fn encode_branch_position_key(branch_identifier: u64, ordinal: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&branch_identifier.to_be_bytes());
    key[8..].copy_from_slice(&ordinal.to_be_bytes());
    key
}

fn encode_branch_file_prefix(branch_identifier: u64, file_identifier: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&encode_u64(branch_identifier));
    key.extend_from_slice(&encode_u64(file_identifier));
    key
}

fn encode_branch_file_position_key(
    branch_identifier: u64,
    file_identifier: u64,
    ordinal: u64,
) -> [u8; 24] {
    let mut key = [0; 24];
    key[..8].copy_from_slice(&branch_identifier.to_be_bytes());
    key[8..16].copy_from_slice(&file_identifier.to_be_bytes());
    key[16..].copy_from_slice(&ordinal.to_be_bytes());
    key
}

fn encode_snapshot_file_prefix(branch_identifier: u64, file_identifier: u64) -> Vec<u8> {
    encode_branch_file_prefix(branch_identifier, file_identifier)
}

fn encode_file_snapshot_manifest_key(
    branch_identifier: u64,
    file_identifier: u64,
    ordinal: u64,
) -> [u8; 24] {
    encode_branch_file_position_key(branch_identifier, file_identifier, ordinal)
}

fn decode_file_snapshot_manifest_key(key: &[u8]) -> Result<(u64, u64, u64), FilesystemError> {
    decode_branch_file_position_key(key)
}

fn encode_event_payload_manifest_key(event_sequence: u64, part: u8) -> [u8; 9] {
    let mut key = [0; 9];
    key[..8].copy_from_slice(&event_sequence.to_be_bytes());
    key[8] = part;
    key
}

fn branch_record_from_stored(stored: StoredBranch) -> Result<BranchRecord, FilesystemError> {
    let branch_identifier = BranchIdentifier::new(stored.identifier);
    Ok(BranchRecord::new(
        branch_identifier,
        BranchName::new(stored.name).map_err(|_| FilesystemError::Integrity)?,
        stored.status,
        BranchPosition::new(branch_identifier, stored.head_ordinal),
        EventSequence::new(stored.head_sequence),
    ))
}

fn validate_branch_event_record(
    record: &EventRecord,
    branch: BranchIdentifier,
    ordinal: u64,
) -> Result<(), FilesystemError> {
    if record.branch_identifier() != Some(branch)
        || record.branch_position() != Some(BranchPosition::new(branch, ordinal))
    {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn validate_branch_file_event_record(
    record: &EventRecord,
    branch: BranchIdentifier,
    file_identifier: FileIdentifier,
    ordinal: u64,
) -> Result<(), FilesystemError> {
    validate_branch_event_record(record, branch, ordinal)?;
    if !event_file_identifiers(record).contains(&file_identifier) {
        return Err(FilesystemError::Integrity);
    }
    Ok(())
}

fn branch_position_start(
    branch: BranchIdentifier,
    after: Option<BranchPosition>,
) -> Result<u64, FilesystemError> {
    match after {
        Some(position) if position.branch_identifier() != branch => Err(FilesystemError::Integrity),
        Some(position) => position
            .ordinal()
            .checked_add(1)
            .ok_or(FilesystemError::Integrity),
        None => Ok(0),
    }
}

fn event_payload_part_byte(part: FileEventPayloadPart) -> u8 {
    match part {
        FileEventPayloadPart::Overwritten => EVENT_PAYLOAD_PART_OVERWRITTEN,
        FileEventPayloadPart::Written => EVENT_PAYLOAD_PART_WRITTEN,
    }
}

fn event_payload_part_length(event: &EventRecord, part: FileEventPayloadPart) -> Option<u64> {
    match part {
        FileEventPayloadPart::Overwritten => event.overwritten_byte_length(),
        FileEventPayloadPart::Written => event.written_byte_length(),
    }
}

fn event_file_identifiers(event: &EventRecord) -> Vec<FileIdentifier> {
    event.file_identifier().into_iter().collect()
}

fn event_snapshot_file_size(event: &EventRecord, file_identifier: FileIdentifier) -> Option<u64> {
    match event.payload() {
        EventPayload::FileWrite { new_file_size, .. }
            if event.file_identifier() == Some(file_identifier) =>
        {
            Some(*new_file_size)
        }
        EventPayload::None | EventPayload::FileWrite { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_operations_create_paginated_events_and_current_file_contents() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");

        let created = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("current"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();

        storage
            .write_file(inode_number, 0, b"older")
            .expect("older bytes are written");
        storage
            .write_file(inode_number, 0, b"newer")
            .expect("newer bytes are written");
        let bytes = storage
            .read_file(inode_number, 0, 5)
            .expect("current bytes are readable");
        assert_eq!(bytes, b"newer");

        let first_page = storage
            .list_events(None, EventPageLimit::new(2).expect("limit is valid"))
            .expect("events are listed");
        assert_eq!(first_page.records().len(), 2);
        let cursor = first_page.next_after().expect("another page exists");
        let second_page = storage
            .list_events(
                Some(cursor),
                EventPageLimit::new(10).expect("limit is valid"),
            )
            .expect("second page is listed");
        assert_eq!(second_page.next_after(), None);
    }

    #[test]
    fn current_inode_numbers_survive_reopen() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let created = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("stable"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_before = created.attr.ino;
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens");
        let looked_up = storage
            .lookup(INODE_ROOT, OsStr::new("stable"))
            .expect("file is looked up after reopen");
        assert_eq!(looked_up.attr.ino, inode_before);
    }

    #[test]
    fn write_state_advances_after_commits_and_reloads_from_metadata() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        assert_eq!(
            write_state_snapshot(&storage),
            WriteState {
                next_inode_number: INODE_FIRST_ALLOCATED,
                last_event_sequence: EVENT_SEQUENCE_INITIAL.get(),
                next_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get() + 1,
                active_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get(),
                active_branch_head_sequence: EVENT_SEQUENCE_INITIAL.get(),
                active_branch_head_ordinal: 0,
            }
        );

        let created = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("counter"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();
        assert_eq!(
            write_state_snapshot(&storage),
            WriteState {
                next_inode_number: INODE_FIRST_ALLOCATED + 1,
                last_event_sequence: EVENT_SEQUENCE_INITIAL.get() + 1,
                next_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get() + 1,
                active_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get(),
                active_branch_head_sequence: EVENT_SEQUENCE_INITIAL.get() + 1,
                active_branch_head_ordinal: 1,
            }
        );

        storage
            .write_file(inode_number, 0, b"contents")
            .expect("file is written");
        assert_eq!(
            write_state_snapshot(&storage),
            WriteState {
                next_inode_number: INODE_FIRST_ALLOCATED + 1,
                last_event_sequence: EVENT_SEQUENCE_INITIAL.get() + 2,
                next_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get() + 1,
                active_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get(),
                active_branch_head_sequence: EVENT_SEQUENCE_INITIAL.get() + 2,
                active_branch_head_ordinal: 2,
            }
        );
        assert_eq!(
            read_metadata_u64(storage.database(), METADATA_KEY_NEXT_INODE_NUMBER)
                .expect("next inode metadata is readable"),
            INODE_FIRST_ALLOCATED + 1
        );
        assert_eq!(
            read_metadata_u64(
                storage.database(),
                METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE
            )
            .expect("last event metadata is readable"),
            EVENT_SEQUENCE_INITIAL.get() + 2
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens");
        assert_eq!(
            write_state_snapshot(&storage),
            WriteState {
                next_inode_number: INODE_FIRST_ALLOCATED + 1,
                last_event_sequence: EVENT_SEQUENCE_INITIAL.get() + 2,
                next_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get() + 1,
                active_branch_identifier: BRANCH_IDENTIFIER_INITIAL.get(),
                active_branch_head_sequence: EVENT_SEQUENCE_INITIAL.get() + 2,
                active_branch_head_ordinal: 2,
            }
        );
    }

    #[test]
    fn failed_operations_do_not_advance_cached_write_state() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let initial = write_state_snapshot(&storage);

        let invalid_special = storage.create_node(
            INODE_ROOT,
            OsStr::new("invalid-special"),
            CreateNodeKind::Special { mode: 0, rdev: 0 },
        );
        assert!(invalid_special.is_err());
        assert_eq!(write_state_snapshot(&storage), initial);

        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            EventSequence::new(initial.last_event_sequence)
        );
    }

    #[test]
    fn storage_schema_version_0_is_current_and_1_is_rejected() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        assert_eq!(
            read_metadata_u64(storage.database(), METADATA_KEY_STORAGE_SCHEMA_VERSION),
            Ok(STORAGE_SCHEMA_VERSION_CURRENT)
        );

        let metadata = storage
            .database()
            .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
            .expect("metadata column family exists");
        let mut batch = WriteBatch::default();
        batch.put_cf(
            metadata,
            METADATA_KEY_STORAGE_SCHEMA_VERSION,
            encode_u64(1),
        );
        write_batch(storage.database(), batch).expect("newer schema version is written");
        drop(storage);

        assert!(matches!(
            open_database_path(&database),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn write_batch_fault_before_write_preserves_durable_and_cached_state() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let initial = write_state_snapshot(&storage);
        let initial_active = active_branch_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::BeforeWrite);
        assert!(matches!(
            storage.create_node(
                INODE_ROOT,
                OsStr::new("before-write-fault"),
                CreateNodeKind::RegularFile,
            ),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), initial);
        assert_eq!(active_branch_state_snapshot(&storage), initial_active);
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            EventSequence::new(initial.last_event_sequence)
        );
        assert!(
            storage
                .lookup(INODE_ROOT, OsStr::new("before-write-fault"))
                .is_err(),
            "node is absent after pre-write fault"
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens after fault");
        assert_eq!(write_state_snapshot(&storage), initial);
    }

    #[test]
    fn mounted_mutations_use_buffered_writes_and_direct_batches_remain_synchronous() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let _ = take_write_batch_durability();

        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("buffered-mutation"),
                CreateNodeKind::RegularFile,
            )
            .expect("mounted mutation commits");
        assert_eq!(
            take_write_batch_durability(),
            Some(WriteDurability::Buffered)
        );

        write_batch(storage.database(), WriteBatch::default())
            .expect("direct batch helper succeeds");
        assert_eq!(
            take_write_batch_durability(),
            Some(WriteDurability::Synchronous)
        );
    }

    #[test]
    fn write_batch_fault_after_write_recovers_cached_state_on_reopen() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let initial = write_state_snapshot(&storage);
        let initial_active = active_branch_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::AfterWrite);
        assert!(matches!(
            storage.create_node(
                INODE_ROOT,
                OsStr::new("after-write-fault"),
                CreateNodeKind::RegularFile,
            ),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), initial);
        assert_eq!(active_branch_state_snapshot(&storage), initial_active);
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            EventSequence::new(initial.last_event_sequence + 1)
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens after committed fault");
        assert_eq!(
            write_state_snapshot(&storage).last_event_sequence,
            initial.last_event_sequence + 1
        );
        storage
            .lookup(INODE_ROOT, OsStr::new("after-write-fault"))
            .expect("committed node is visible after reopen");
    }

    #[test]
    fn write_batch_fault_before_file_write_preserves_payload_and_snapshot_state() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let inode_number = create_regular_file(&storage, "before-write-file-fault");
        storage
            .write_file(inode_number, 0, b"abcdef")
            .expect("initial bytes are written");
        let before_fault = write_state_snapshot(&storage);
        let before_fault_active = active_branch_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::BeforeWrite);
        assert!(matches!(
            storage.write_file(inode_number, 2, b"XYZ"),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), before_fault);
        assert_eq!(
            active_branch_state_snapshot(&storage),
            before_fault_active
        );
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            EventSequence::new(before_fault.last_event_sequence)
        );
        assert_eq!(
            storage
                .read_file(inode_number, 0, 6)
                .expect("file remains readable after pre-write fault"),
            b"abcdef"
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens after fault");
        assert_eq!(
            storage
                .read_file(inode_number, 0, 6)
                .expect("file remains unchanged after reopen"),
            b"abcdef"
        );
        assert_eq!(
            write_state_snapshot(&storage).last_event_sequence,
            before_fault.last_event_sequence
        );
    }

    #[test]
    fn write_batch_fault_after_file_write_recovers_payload_and_snapshot_state_on_reopen() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let inode_number = create_regular_file(&storage, "after-write-file-fault");
        storage
            .write_file(inode_number, 0, b"abcdef")
            .expect("initial bytes are written");
        let before_fault = write_state_snapshot(&storage);
        let before_fault_active = active_branch_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::AfterWrite);
        assert!(matches!(
            storage.write_file(inode_number, 2, b"XYZ"),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), before_fault);
        assert_eq!(
            active_branch_state_snapshot(&storage),
            before_fault_active
        );
        let committed = EventSequence::new(before_fault.last_event_sequence + 1);
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            committed
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens after committed fault");
        assert_eq!(
            storage
                .read_file(inode_number, 0, 6)
                .expect("committed write is readable after reopen"),
            b"abXYZf"
        );
        let event = storage
            .get_event(committed)
            .expect("committed write event is readable")
            .expect("committed write event exists");
        assert_eq!(event.kind(), EventKind::FileWritten);
        assert_eq!(event.overwritten_byte_length(), Some(3));
        assert_eq!(event.written_byte_length(), Some(3));
        assert_eq!(
            storage
                .read_file_event_payload_range(committed, FileEventPayloadPart::Overwritten, 0, 3,)
                .expect("overwritten payload is readable"),
            b"cde"
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(committed, FileEventPayloadPart::Written, 0, 3)
                .expect("written payload is readable"),
            b"XYZ"
        );
        let snapshot = storage
            .file_snapshot_at_or_before(FileIdentifier::new(inode_number), committed)
            .expect("snapshot lookup succeeds")
            .expect("snapshot exists");
        assert_eq!(
            storage
                .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
                .expect("snapshot bytes are readable"),
            b"abXYZf"
        );
    }

    #[test]
    fn duplicate_event_keys_are_rejected_before_commit() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("existing-event"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");

        let duplicate_sequence = EventSequence::new(EVENT_SEQUENCE_INITIAL.get() + 1);
        let duplicate_event = EventRecord::new(
            duplicate_sequence,
            EventKind::NodeCreated,
            UtcDateTime::now(),
            None,
            None,
            None,
            None,
        );
        let mut batch = WriteBatch::default();
        let active_state = storage
            .lock_active_branch_state()
            .expect("active branch state lock is available");

        assert!(matches!(
            storage.commit_event(
                &mut batch,
                &active_state,
                duplicate_sequence,
                &duplicate_event,
                StoredMutation::default(),
            ),
            Err(FuseError::Integrity)
        ));
    }

    #[test]
    fn event_key_sequence_mismatch_is_rejected() {
        let sequence = EventSequence::new(EVENT_SEQUENCE_INITIAL.get() + 1);
        let event = EventRecord::new(
            sequence,
            EventKind::NodeCreated,
            UtcDateTime::now(),
            None,
            None,
            None,
            None,
        );
        let stored = StoredEvent {
            record: event,
            mutation: StoredMutation::default(),
        };
        let encoded = encode_stored_event(&stored).expect("event encodes");

        assert!(matches!(
            decode_stored_event_at_key(&encode_u64(sequence.get() + 1), &encoded),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn file_event_index_mismatches_are_rejected() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let created = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("indexed"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let file_identifier = FileIdentifier::new(created.attr.ino.into());
        let sequence = storage
            .last_event_sequence()
            .expect("last event sequence is readable");

        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .file_events()
                .expect("file event column family exists"),
            encode_file_sequence_key(file_identifier.get(), sequence.get()),
            encode_u64(EVENT_SEQUENCE_INITIAL.get()),
        );
        write_batch(storage.database(), batch).expect("corrupt file event index is written");

        assert!(matches!(
            storage.list_file_events(
                file_identifier,
                None,
                EventPageLimit::new(10).expect("limit is valid")
            ),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn stale_branch_name_indexes_are_rejected_before_delete() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let main = storage.current_branch().expect("main branch is returned");
        let first_name = BranchName::new("first").expect("branch name is valid");
        let second_name = BranchName::new("second").expect("branch name is valid");
        let first = storage
            .create_branch(&first_name, main.head_position())
            .expect("first branch is created");
        let second = storage
            .create_branch(&second_name, main.head_position())
            .expect("second branch is created");

        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .branch_names()
                .expect("branch names column family exists"),
            first_name.as_str().as_bytes(),
            encode_u64(second.branch_identifier().get()),
        );
        write_batch(storage.database(), batch).expect("corrupt branch name index is written");

        assert!(matches!(
            storage.delete_branch(&first_name),
            Err(FilesystemError::Integrity)
        ));
        assert_eq!(
            storage
                .branch_record(first.branch_identifier())
                .expect("first branch record is readable")
                .status(),
            BranchStatus::Open
        );
        assert_eq!(
            storage
                .branch_record(second.branch_identifier())
                .expect("second branch record is readable")
                .status(),
            BranchStatus::Open
        );
    }

    #[test]
    fn corrupt_branch_roots_are_rejected_during_branch_creation() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("materialized"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let main = storage.current_branch().expect("main branch is returned");

        let mut root = storage
            .branch_root_at_position(main.head_position())
            .expect("main branch root is readable");
        root.sequence = EVENT_SEQUENCE_INITIAL.get();
        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .branch_roots()
                .expect("branch roots column family exists"),
            encode_branch_position_key(
                main.branch_identifier().get(),
                main.head_position().ordinal(),
            ),
            encode_branch_root(&root).expect("corrupt branch root encodes"),
        );
        write_batch(storage.database(), batch).expect("corrupt branch root is written");

        assert!(matches!(
            storage.create_branch(
                &BranchName::new("corrupt-root").expect("branch name is valid"),
                main.head_position(),
            ),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn corrupt_content_chunks_are_rejected() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let created = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("content"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();
        storage
            .write_file(inode_number, 0, b"content bytes")
            .expect("file is written");
        let committed_sequence = storage
            .last_event_sequence()
            .expect("last event sequence is readable");

        let content_chunks = storage
            .content_chunks()
            .expect("content chunks column family exists");
        let mut iterator = storage.database().raw_iterator_cf(content_chunks);
        iterator.seek_to_first();
        let chunk_identifier = iterator.key().expect("content chunk key exists").to_vec();
        iterator.status().expect("content chunk iterator is valid");

        let mut batch = WriteBatch::default();
        batch.put_cf(content_chunks, &chunk_identifier, b"corrupt bytes");
        write_batch(storage.database(), batch).expect("corrupt content chunk is written");

        assert!(matches!(
            storage.read_file(inode_number, 0, 100),
            Err(FuseError::Integrity)
        ));
        assert!(matches!(
            storage.write_file(inode_number, 0, b"new"),
            Err(FuseError::Integrity)
        ));
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            committed_sequence
        );
    }

    #[test]
    fn append_and_tail_reads_do_not_validate_untouched_old_chunks() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "append-corruption");
        let original = patterned_bytes(96 * 1024, 17);
        let appended = patterned_bytes(24 * 1024, 41);
        storage
            .write_file(inode_number, 0, &original)
            .expect("original file is written");

        let content_chunks = storage
            .content_chunks()
            .expect("content chunks column family exists");
        let mut iterator = storage.database().raw_iterator_cf(content_chunks);
        iterator.seek_to_first();
        let chunk_identifier = iterator.key().expect("content chunk key exists").to_vec();
        iterator.status().expect("content chunk iterator is valid");

        let mut batch = WriteBatch::default();
        batch.put_cf(content_chunks, &chunk_identifier, b"corrupt bytes");
        write_batch(storage.database(), batch).expect("old content chunk is corrupted");

        storage
            .write_file(inode_number, original.len() as u64, &appended)
            .expect("append does not validate untouched corrupt content");
        assert_eq!(
            storage
                .read_file(inode_number, original.len() as u64, appended.len() as u32)
                .expect("appended tail is readable without old content"),
            appended
        );
        assert!(matches!(
            storage.write_file(inode_number, 0, b"overwrite"),
            Err(FuseError::Integrity)
        ));
    }

    #[test]
    fn many_small_appends_cross_manifest_child_limits_and_keep_ranges_readable() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "many-appends");
        let append_count = CONTENT_MANIFEST_CONCAT_MAX_CHILDREN * 2 + 3;
        let mut expected = Vec::with_capacity(append_count * 3);

        for index in 0..append_count {
            let bytes = [
                (index % 251) as u8,
                ((index * 3) % 251) as u8,
                ((index * 7) % 251) as u8,
            ];
            storage
                .write_file(inode_number, expected.len() as u64, &bytes)
                .expect("small append succeeds");
            expected.extend_from_slice(&bytes);
        }

        assert_eq!(
            storage
                .read_file(inode_number, 0, expected.len() as u32)
                .expect("full file is readable"),
            expected
        );
        let middle = expected.len() / 2;
        assert_eq!(
            storage
                .read_file(inode_number, middle as u64, 33)
                .expect("middle range is readable"),
            expected[middle..middle + 33]
        );
        let tail = expected.len() - 48;
        assert_eq!(
            storage
                .read_file(inode_number, tail as u64, 48)
                .expect("tail range is readable"),
            expected[tail..]
        );
    }

    #[test]
    fn large_snapshot_and_payload_ranges_cross_content_chunks() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let created = storage
            .create_node(INODE_ROOT, OsStr::new("large"), CreateNodeKind::RegularFile)
            .expect("file is created");
        let inode_number = created.attr.ino.into();
        let file_identifier = FileIdentifier::new(inode_number);
        let overwrite_offset = 96 * 1024;
        let original = patterned_bytes(300 * 1024, 11);
        let replacement = patterned_bytes(70 * 1024, 91);
        let mut expected_after_overwrite = original.clone();
        expected_after_overwrite[overwrite_offset..overwrite_offset + replacement.len()]
            .copy_from_slice(&replacement);

        storage
            .write_file(inode_number, 0, &original)
            .expect("large file is written");
        let initial_write = storage
            .last_event_sequence()
            .expect("last event sequence is readable");
        storage
            .write_file(inode_number, overwrite_offset as u64, &replacement)
            .expect("large file is overwritten");
        let overwrite = storage
            .last_event_sequence()
            .expect("last event sequence is readable");

        assert_eq!(
            storage
                .read_file_event_payload_range(
                    initial_write,
                    FileEventPayloadPart::Written,
                    0,
                    original.len() as u64,
                )
                .expect("large written payload is read"),
            original
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(
                    overwrite,
                    FileEventPayloadPart::Overwritten,
                    0,
                    replacement.len() as u64,
                )
                .expect("large overwritten payload is read"),
            original[overwrite_offset..overwrite_offset + replacement.len()]
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(
                    overwrite,
                    FileEventPayloadPart::Written,
                    0,
                    replacement.len() as u64,
                )
                .expect("large replacement payload is read"),
            replacement
        );

        let snapshot = storage
            .file_snapshot_at_or_before(file_identifier, overwrite)
            .expect("large snapshot lookup succeeds")
            .expect("large snapshot exists");
        assert_eq!(
            storage
                .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
                .expect("large snapshot is read"),
            expected_after_overwrite
        );
        assert_eq!(
            storage
                .read_file_snapshot_range(&snapshot, 200 * 1024, 64 * 1024)
                .expect("large snapshot range is read"),
            expected_after_overwrite[200 * 1024..264 * 1024]
        );
    }

    #[test]
    fn bounded_extent_write_model_matches_dense_bytes() {
        let mut dense = Vec::new();
        let mut extent_model = BoundedExtentModel::default();

        for operation in bounded_extent_model_operations() {
            let expectation = apply_bounded_file_operation(&mut dense, &operation);
            extent_model
                .apply(&operation)
                .expect("extent model operation succeeds");

            assert_eq!(
                extent_model
                    .materialize()
                    .expect("extent model materializes to dense bytes"),
                expectation.after
            );
            assert_extent_manifest_is_sorted_and_non_overlapping(
                &extent_model.extents,
                extent_model.file_size,
            );
        }
    }

    #[test]
    fn bounded_file_history_model_keeps_current_snapshot_payload_and_listing_views_consistent() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "history-model");
        let file_identifier = FileIdentifier::new(inode_number);
        let create_sequence = storage
            .last_event_sequence()
            .expect("create event sequence is readable");
        let mut history_bytes_by_sequence = BTreeMap::new();
        history_bytes_by_sequence.insert(create_sequence, Vec::new());

        assert_storage_history_entry(
            &storage,
            inode_number,
            file_identifier,
            create_sequence,
            &BoundedFileExpectation::created(),
            &history_bytes_by_sequence,
        );

        let mut dense = Vec::new();
        let mut expected_sequences = vec![create_sequence];
        let mut content_chunk_count_after_first_write = None;

        for (index, operation) in bounded_file_history_model_operations()
            .into_iter()
            .enumerate()
        {
            let expectation = apply_bounded_file_operation(&mut dense, &operation);
            run_bounded_file_operation(&storage, inode_number, &operation);

            let sequence = storage
                .last_event_sequence()
                .expect("operation event sequence is readable");
            history_bytes_by_sequence.insert(sequence, expectation.after.clone());
            expected_sequences.push(sequence);

            assert_storage_history_entry(
                &storage,
                inode_number,
                file_identifier,
                sequence,
                &expectation,
                &history_bytes_by_sequence,
            );
            assert_eq!(
                reconstruct_file_bytes_at_sequence(
                    &storage,
                    file_identifier,
                    EventSequence::new(sequence.get() - 1),
                ),
                expectation.before
            );
            assert_eq!(
                reconstruct_file_bytes_at_sequence(&storage, file_identifier, sequence),
                expectation.after
            );

            if index == 0 {
                content_chunk_count_after_first_write = Some(count_content_chunks(&storage));
            } else if index == 1 {
                assert_eq!(
                    count_content_chunks(&storage),
                    content_chunk_count_after_first_write
                        .expect("first write chunk count is recorded"),
                );
            }
        }

        let current_branch = storage
            .current_branch()
            .expect("current branch is readable");
        let file_events = collect_all_file_events(&storage, file_identifier);
        let branch_file_events = collect_all_branch_file_events(
            &storage,
            current_branch.branch_identifier(),
            file_identifier,
        );
        let branch_events_for_file =
            collect_all_branch_events(&storage, current_branch.branch_identifier())
                .into_iter()
                .filter(|event| event.file_identifier() == Some(file_identifier))
                .collect::<Vec<_>>();
        let all_events_for_file = collect_all_events(&storage)
            .into_iter()
            .filter(|event| {
                event.file_identifier() == Some(file_identifier)
                    && event.branch_identifier() == Some(current_branch.branch_identifier())
            })
            .collect::<Vec<_>>();

        assert_eq!(
            file_events
                .iter()
                .map(EventRecord::sequence)
                .collect::<Vec<_>>(),
            expected_sequences
        );
        assert_eq!(branch_file_events, file_events);
        assert_eq!(branch_events_for_file, file_events);
        assert_eq!(all_events_for_file, file_events);
        assert_eq!(
            current_branch.head_sequence(),
            *expected_sequences.last().expect("history contains events")
        );
        assert_eq!(
            current_branch.head_position(),
            file_events
                .last()
                .and_then(EventRecord::branch_position)
                .expect("last file event has a branch position"),
        );
        assert_eq!(
            reconstruct_file_bytes_at_sequence(
                &storage,
                file_identifier,
                current_branch.head_sequence(),
            ),
            dense
        );
    }

    #[test]
    fn allocator_metadata_below_stored_inode_state_is_rejected_on_open() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("allocated"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");

        let metadata = storage
            .database()
            .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
            .expect("metadata column family exists");
        let mut batch = WriteBatch::default();
        batch.put_cf(
            metadata,
            METADATA_KEY_NEXT_INODE_NUMBER,
            encode_u64(INODE_FIRST_ALLOCATED),
        );
        write_batch(storage.database(), batch).expect("corrupt allocator metadata is written");
        drop(storage);

        assert!(matches!(
            open_database_path(&database),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn filesystem_statistics_map_backing_blocks_and_logical_inode_capacity() {
        let statistics = statistics_from_backing(
            BackingFileSystemStatistics {
                blocks: 100,
                free_blocks: 40,
                available_blocks: 30,
                fragment_size: 8192,
            },
            INODE_FIRST_ALLOCATED,
        )
        .expect("statistics are mapped");

        assert_eq!(statistics.blocks, 100);
        assert_eq!(statistics.free_blocks, 40);
        assert_eq!(statistics.available_blocks, 30);
        assert_eq!(statistics.files, INODE_LOGICAL_CAPACITY);
        assert_eq!(statistics.free_files, u64::MAX - INODE_FIRST_ALLOCATED);
        assert_eq!(statistics.block_size, FUSE_STATFS_BLOCK_SIZE);
        assert_eq!(statistics.maximum_name_length, FUSE_MAX_NAME_LENGTH as u32);
        assert_eq!(statistics.fragment_size, 8192);
    }

    #[test]
    fn filesystem_statistics_do_not_append_events_and_track_inode_capacity() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");

        let initial_event_sequence = storage
            .last_event_sequence()
            .expect("last event sequence is readable");
        let initial_statistics = storage
            .statfs(&database)
            .expect("filesystem statistics are returned");
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            initial_event_sequence
        );

        let created = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("tracked"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();
        let after_create = storage
            .statfs(&database)
            .expect("filesystem statistics are returned after create");
        assert_eq!(after_create.free_files, initial_statistics.free_files - 1);

        storage
            .write_file(inode_number, 0, b"contents")
            .expect("file is written");
        let after_write = storage
            .statfs(&database)
            .expect("filesystem statistics are returned after write");
        assert_eq!(after_write.free_files, after_create.free_files);

        storage
            .hard_link(inode_number, INODE_ROOT, OsStr::new("hard-link"))
            .expect("hard link is created");
        let after_hard_link = storage
            .statfs(&database)
            .expect("filesystem statistics are returned after hard link");
        assert_eq!(after_hard_link.free_files, after_create.free_files);

        storage
            .rename(
                INODE_ROOT,
                OsStr::new("tracked"),
                INODE_ROOT,
                OsStr::new("renamed"),
                false,
            )
            .expect("node is renamed");
        let after_rename = storage
            .statfs(&database)
            .expect("filesystem statistics are returned after rename");
        assert_eq!(after_rename.free_files, after_create.free_files);
    }

    #[test]
    fn extended_attributes_are_branch_local_and_copied_on_branch_creation() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "xattr-file");
        let name = OsStr::new("user.eventfs.branch");

        storage
            .setxattr(inode_number, name, b"main", libc::XATTR_CREATE)
            .expect("xattr is set on main");
        let main = storage.current_branch().expect("main branch is readable");
        let branch_name = BranchName::new("xattrs").expect("branch name is valid");
        storage
            .create_branch(&branch_name, main.head_position())
            .expect("branch is created");

        storage
            .switch_branch(&branch_name)
            .expect("xattr branch is active");
        assert_eq!(
            storage
                .getxattr(inode_number, name)
                .expect("copied xattr is readable"),
            b"main"
        );
        storage
            .setxattr(inode_number, name, b"branch", libc::XATTR_REPLACE)
            .expect("branch xattr is replaced");
        assert_eq!(
            storage
                .getxattr(inode_number, name)
                .expect("branch xattr is readable"),
            b"branch"
        );

        storage
            .switch_branch(main.name())
            .expect("main branch is active again");
        assert_eq!(
            storage
                .getxattr(inode_number, name)
                .expect("main xattr is isolated"),
            b"main"
        );
        storage
            .removexattr(inode_number, name)
            .expect("main xattr is removed");
        assert!(matches!(
            storage.getxattr(inode_number, name),
            Err(FuseError::Errno(errno)) if errno.code() == fuser::Errno::NO_XATTR.code()
        ));
        let main_after_remove = storage
            .current_branch()
            .expect("main branch after removal is readable");
        let after_remove_branch_name =
            BranchName::new("xattrs-after-remove").expect("branch name is valid");
        storage
            .create_branch(&after_remove_branch_name, main_after_remove.head_position())
            .expect("branch is created after xattr removal");
        storage
            .switch_branch(&after_remove_branch_name)
            .expect("post-removal branch is active");
        assert!(matches!(
            storage.getxattr(inode_number, name),
            Err(FuseError::Errno(errno)) if errno.code() == fuser::Errno::NO_XATTR.code()
        ));

        let kinds = collect_all_events(&storage)
            .into_iter()
            .map(|event| event.kind())
            .collect::<Vec<_>>();
        assert!(kinds.contains(&EventKind::ExtendedAttributeSet));
        assert!(kinds.contains(&EventKind::ExtendedAttributeRemoved));
    }

    #[test]
    fn branch_creation_copies_namespace_root_without_duplicating_manifest_storage() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "root-copy");
        storage
            .write_file(inode_number, 0, b"shared-by-root")
            .expect("file is written");
        let main = storage.current_branch().expect("main branch is readable");
        let source_root = storage
            .branch_root_at_position(main.head_position())
            .expect("main branch root is readable");
        let before = RootStorageCounts::capture(&storage);

        let branch_name = BranchName::new("root-copy").expect("branch name is valid");
        let branch = storage
            .create_branch(&branch_name, main.head_position())
            .expect("branch is created from the root");
        let branch_root = storage
            .branch_root_at_position(branch.head_position())
            .expect("new branch root is readable");

        assert_eq!(branch_root.sequence, source_root.sequence);
        assert_eq!(
            branch_root.namespace_identifier,
            source_root.namespace_identifier
        );
        assert_eq!(RootStorageCounts::capture(&storage), before);

        storage
            .switch_branch(&branch_name)
            .expect("new branch is active");
        assert_eq!(
            storage
                .read_file(inode_number, 0, 32)
                .expect("file bytes are readable through copied root"),
            b"shared-by-root"
        );
    }

    #[test]
    fn extended_attributes_validate_flags_names_and_noop_writes() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "xattr-edges");
        let name = OsStr::new("user.eventfs.edge");

        storage
            .setxattr(inode_number, name, b"value", libc::XATTR_CREATE)
            .expect("xattr is created");
        let before_noop = storage
            .last_event_sequence()
            .expect("last event sequence is readable");

        storage
            .setxattr(inode_number, name, b"value", 0)
            .expect("setting the same xattr value is a no-op");

        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is reread"),
            before_noop
        );
        assert_fuse_errno(
            storage
                .setxattr(inode_number, name, b"value", libc::XATTR_CREATE)
                .expect_err("create rejects an existing xattr"),
            fuser::Errno::EEXIST,
        );
        assert_fuse_errno(
            storage
                .setxattr(
                    inode_number,
                    OsStr::new("user.eventfs.missing"),
                    b"value",
                    libc::XATTR_REPLACE,
                )
                .expect_err("replace rejects a missing xattr"),
            fuser::Errno::NO_XATTR,
        );
        assert_fuse_errno(
            storage
                .setxattr(
                    inode_number,
                    name,
                    b"value",
                    libc::XATTR_CREATE | libc::XATTR_REPLACE,
                )
                .expect_err("create and replace flags conflict"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            storage
                .setxattr(inode_number, name, b"value", 0x4000)
                .expect_err("unknown xattr flags are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            storage
                .setxattr(inode_number, OsStr::new(""), b"value", 0)
                .expect_err("empty xattr names are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            storage
                .setxattr(
                    inode_number,
                    OsStr::new("trusted.eventfs.edge"),
                    b"value",
                    0,
                )
                .expect_err("non-user xattr namespaces are rejected"),
            fuser::Errno::ENOTSUP,
        );
        assert_fuse_errno(
            storage
                .getxattr(inode_number, OsStr::from_bytes(b"user.\xff"))
                .expect_err("non-utf8 xattr names are rejected"),
            fuser::Errno::ENOTSUP,
        );
        assert_fuse_errno(
            storage
                .getxattr(inode_number, OsStr::from_bytes(&[b'a'; 256]))
                .expect_err("overlong xattr names are rejected"),
            fuser::Errno::ERANGE,
        );
        assert_fuse_errno(
            storage
                .setxattr(inode_number, name, &vec![0; XATTR_VALUE_MAX_BYTES + 1], 0)
                .expect_err("overlong xattr values are rejected"),
            fuser::Errno::ERANGE,
        );
        assert_fuse_errno(
            storage
                .removexattr(inode_number, OsStr::from_bytes(b"user\0bad"))
                .expect_err("nul-containing xattr names are rejected"),
            fuser::Errno::EINVAL,
        );
        let symlink = storage
            .create_symlink(
                INODE_ROOT,
                OsStr::new("xattr-link"),
                Path::new("xattr-edges"),
            )
            .expect("symlink is created")
            .attr
            .ino
            .into();
        assert_fuse_errno(
            storage
                .setxattr(symlink, name, b"value", 0)
                .expect_err("symlink xattrs are rejected"),
            fuser::Errno::ENOTSUP,
        );
    }

    #[test]
    fn branch_creation_after_xattr_file_deletion_replays_inode_deletes() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "deleted-xattr-file");
        let name = OsStr::new("user.eventfs.deleted");
        storage
            .setxattr(inode_number, name, b"value", libc::XATTR_CREATE)
            .expect("xattr is created");
        storage
            .unlink(INODE_ROOT, OsStr::new("deleted-xattr-file"))
            .expect("file is unlinked");
        let main = storage
            .current_branch()
            .expect("main branch after unlink is readable");
        let branch_name = BranchName::new("after-xattr-delete").expect("branch name is valid");

        storage
            .create_branch(&branch_name, main.head_position())
            .expect("branch after deleted xattr file is created");
        storage
            .switch_branch(&branch_name)
            .expect("branch after deleted xattr file is active");

        assert_fuse_errno(
            storage
                .getxattr(inode_number, name)
                .expect_err("deleted file inode is absent on materialized branch"),
            fuser::Errno::ENOENT,
        );
        assert_fuse_errno(
            storage
                .lookup(INODE_ROOT, OsStr::new("deleted-xattr-file"))
                .expect_err("deleted file path is absent on materialized branch"),
            fuser::Errno::ENOENT,
        );
    }

    #[test]
    fn write_state_rejects_invalid_inode_and_event_boundaries() {
        assert!(matches!(
            WriteState {
                next_inode_number: INODE_FIRST_ALLOCATED - 1,
                last_event_sequence: 0,
                next_branch_identifier: 0,
                active_branch_identifier: 0,
                active_branch_head_sequence: 0,
                active_branch_head_ordinal: 0,
            }
            .next_inode_number(),
            Err(FuseError::Integrity)
        ));
        assert_fuse_errno(
            WriteState {
                next_inode_number: u64::MAX,
                last_event_sequence: 0,
                next_branch_identifier: 0,
                active_branch_identifier: 0,
                active_branch_head_sequence: 0,
                active_branch_head_ordinal: 0,
            }
            .next_inode_number()
            .expect_err("exhausted inode space is reported"),
            fuser::Errno::ENOSPC,
        );
        assert!(matches!(
            WriteState {
                next_inode_number: INODE_FIRST_ALLOCATED,
                last_event_sequence: u64::MAX,
                next_branch_identifier: 0,
                active_branch_identifier: 0,
                active_branch_head_sequence: 0,
                active_branch_head_ordinal: 0,
            }
            .next_event_sequence(),
            Err(FuseError::Integrity)
        ));
    }

    #[test]
    fn readdirplus_returns_entries_with_attributes_and_honors_offsets() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let directory = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("directory"),
                CreateNodeKind::Directory,
            )
            .expect("directory is created")
            .attr
            .ino
            .into();
        let file = create_regular_file(&storage, "file");

        let mut entries = Vec::new();
        storage
            .readdirplus(INODE_ROOT, 0, |entry| {
                entries.push((entry.name, entry.inode, entry.entry.attr.kind));
                true
            })
            .expect("readdirplus succeeds");

        assert_eq!(entries[0].0, ".");
        assert_eq!(entries[0].1, INODE_ROOT);
        assert_eq!(entries[1].0, "..");
        assert_eq!(entries[1].1, INODE_ROOT);
        assert!(entries.contains(&(
            OsString::from("directory"),
            directory,
            fuser::FileType::Directory
        )));
        assert!(entries.contains(&(OsString::from("file"), file, fuser::FileType::RegularFile)));

        let mut offset_entries = Vec::new();
        storage
            .readdirplus(INODE_ROOT, 2, |entry| {
                offset_entries.push(entry.name);
                true
            })
            .expect("readdirplus with offset succeeds");

        assert_eq!(
            offset_entries,
            vec![OsString::from("directory"), OsString::from("file")]
        );

        let mut emitted = 0;
        storage
            .readdirplus(INODE_ROOT, 0, |_entry| {
                emitted += 1;
                false
            })
            .expect("readdirplus stops when the caller buffer is full");
        assert_eq!(emitted, 1);

        assert_fuse_errno(
            storage
                .readdirplus(file, 0, |_entry| true)
                .expect_err("readdirplus rejects regular files"),
            fuser::Errno::ENOTDIR,
        );
        assert_fuse_errno(
            storage
                .readdirplus(u64::MAX, 0, |_entry| true)
                .expect_err("readdirplus rejects missing inodes"),
            fuser::Errno::ENOENT,
        );
        assert_fuse_errno(
            storage
                .readdir(file, 0, |_entry| true)
                .expect_err("readdir rejects regular files"),
            fuser::Errno::ENOTDIR,
        );
    }

    #[test]
    fn payload_reads_and_index_listings_handle_empty_ranges_and_prefix_boundaries() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let first = create_regular_file(&storage, "payload-first");
        let first_create_sequence = storage
            .last_event_sequence()
            .expect("create event sequence is readable");
        let second = create_regular_file(&storage, "payload-second");
        let xattr_name = OsStr::new("user.eventfs.only-second");

        storage
            .write_file(first, 0, b"payload")
            .expect("file is written");
        let write_sequence = storage
            .last_event_sequence()
            .expect("write event sequence is readable");
        storage
            .setxattr(second, xattr_name, b"value", libc::XATTR_CREATE)
            .expect("xattr is set on second file");

        assert_eq!(
            storage
                .read_file_event_payload_range(
                    EventSequence::new(u64::MAX),
                    FileEventPayloadPart::Written,
                    0,
                    16,
                )
                .expect("missing event payload reads as empty"),
            Vec::<u8>::new()
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(
                    first_create_sequence,
                    FileEventPayloadPart::Written,
                    0,
                    16,
                )
                .expect("non-payload event payload reads as empty"),
            Vec::<u8>::new()
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(
                    write_sequence,
                    FileEventPayloadPart::Written,
                    7,
                    16,
                )
                .expect("payload range at end reads as empty"),
            Vec::<u8>::new()
        );

        let limit = EventPageLimit::new(10).expect("limit is valid");
        assert!(
            storage
                .list_file_events(FileIdentifier::new(0), None, limit)
                .expect("missing active-branch file history lists as empty")
                .records()
                .is_empty()
        );
        assert!(
            storage
                .list_branch_events(BranchIdentifier::new(0), None, limit)
                .expect("missing branch history lists as empty")
                .records()
                .is_empty()
        );
        assert!(
            storage
                .list_branch_file_events(
                    BranchIdentifier::new(0),
                    FileIdentifier::new(0),
                    None,
                    limit,
                )
                .expect("missing branch file history lists as empty")
                .records()
                .is_empty()
        );
        assert_eq!(
            storage
                .listxattr(first)
                .expect("xattr listing with only later keys succeeds")
                .bytes,
            Vec::<u8>::new()
        );
    }

    #[test]
    fn directory_readers_cover_offsets_stops_and_nested_prefix_boundaries() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let directory = storage
            .create_node(INODE_ROOT, OsStr::new("nested"), CreateNodeKind::Directory)
            .expect("directory is created")
            .attr
            .ino
            .into();
        storage
            .create_node(directory, OsStr::new("child"), CreateNodeKind::RegularFile)
            .expect("nested file is created");

        let mut names = Vec::new();
        storage
            .readdir(INODE_ROOT, 0, |entry| {
                names.push(entry.name);
                true
            })
            .expect("root directory is read");
        assert_eq!(names[0], ".");
        assert_eq!(names[1], "..");
        assert!(names.contains(&OsString::from("nested")));

        let mut emitted = 0;
        storage
            .readdir(INODE_ROOT, 0, |_entry| {
                emitted += 1;
                false
            })
            .expect("readdir stops after dot");
        assert_eq!(emitted, 1);
        emitted = 0;
        storage
            .readdir(INODE_ROOT, 1, |_entry| {
                emitted += 1;
                false
            })
            .expect("readdir stops after parent entry");
        assert_eq!(emitted, 1);
        emitted = 0;
        storage
            .readdir(INODE_ROOT, 2, |_entry| {
                emitted += 1;
                false
            })
            .expect("readdir stops after a real entry");
        assert_eq!(emitted, 1);

        let mut plus_names = Vec::new();
        storage
            .readdirplus(INODE_ROOT, 0, |entry| {
                plus_names.push(entry.name);
                true
            })
            .expect("root directory plus attributes is read");
        assert_eq!(plus_names[0], ".");
        assert_eq!(plus_names[1], "..");
        assert!(plus_names.contains(&OsString::from("nested")));

        emitted = 0;
        storage
            .readdirplus(directory, 1, |entry| {
                assert_eq!(entry.name, "..");
                emitted += 1;
                false
            })
            .expect("readdirplus stops after parent entry");
        assert_eq!(emitted, 1);
        emitted = 0;
        storage
            .readdirplus(INODE_ROOT, 2, |_entry| {
                emitted += 1;
                false
            })
            .expect("readdirplus stops after a real entry");
        assert_eq!(emitted, 1);
    }

    #[test]
    fn node_symlink_xattr_and_access_validation_cover_user_errors() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let file = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("access-file"),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created")
            .attr
            .ino
            .into();
        let attributes = storage
            .getattr(file)
            .expect("file attributes are readable")
            .attr;
        assert_eq!(attributes.perm, 0o666);
        assert_eq!(attributes.uid, 0);
        assert_eq!(attributes.gid, 0);
        assert_eq!(attributes.flags, 0);

        assert_fuse_errno(
            storage
                .create_node(
                    INODE_ROOT,
                    OsStr::new("access-file"),
                    CreateNodeKind::RegularFile,
                )
                .expect_err("duplicate file creation is rejected"),
            fuser::Errno::EEXIST,
        );

        let symlink = storage
            .create_symlink(INODE_ROOT, OsStr::new("link"), Path::new("access-file"))
            .expect("symlink is created")
            .attr
            .ino
            .into();
        assert_eq!(
            storage
                .readlink(symlink)
                .expect("symlink target is readable"),
            b"access-file"
        );
        assert_fuse_errno(
            storage
                .create_symlink(INODE_ROOT, OsStr::new("link"), Path::new("other"))
                .expect_err("duplicate symlink creation is rejected"),
            fuser::Errno::EEXIST,
        );
        assert_fuse_errno(
            storage
                .readlink(file)
                .expect_err("readlink rejects regular files"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            storage
                .open_file(INODE_ROOT, false)
                .expect_err("open_file rejects directories"),
            fuser::Errno::EISDIR,
        );
        assert_fuse_errno(
            storage
                .read_file(INODE_ROOT, 0, 1)
                .expect_err("read_file rejects directories"),
            fuser::Errno::EISDIR,
        );
        assert_fuse_errno(
            storage
                .opendir(file)
                .expect_err("opendir rejects regular files"),
            fuser::Errno::ENOTDIR,
        );
        storage
            .access(
                file,
                0,
                0,
                fuser::AccessFlags::R_OK | fuser::AccessFlags::W_OK,
            )
            .expect("access validates existing inode");
        assert_fuse_errno(
            storage
                .access(file, 1, 1, fuser::AccessFlags::X_OK)
                .expect_err("access rejects missing permission bits"),
            fuser::Errno::EACCES,
        );
        assert_fuse_errno(
            storage
                .access(u64::MAX, 0, 0, fuser::AccessFlags::R_OK)
                .expect_err("access rejects missing inodes"),
            fuser::Errno::ENOENT,
        );
        let xattr_name = OsStr::new("user.eventfs.access");
        storage
            .setxattr(file, xattr_name, b"value", libc::XATTR_CREATE)
            .expect("owner sets xattr");
        storage
            .removexattr(file, xattr_name)
            .expect("xattr removal succeeds");
        assert_fuse_errno(
            storage
                .removexattr(file, OsStr::new("user.eventfs.missing"))
                .expect_err("removexattr rejects missing attributes"),
            fuser::Errno::NO_XATTR,
        );
    }

    #[test]
    fn setattr_metadata_updates_permissions_and_ownership_with_events() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let file = storage
            .create_node_with_metadata(
                INODE_ROOT,
                OsStr::new("metadata-file"),
                CreateNodeKind::RegularFile,
                CreateNodeMetadata {
                    uid: 10,
                    gid: 20,
                    mode: 0o640,
                    umask: 0,
                },
            )
            .expect("file with metadata is created")
            .attr
            .ino
            .into();
        let created = storage
            .getattr(file)
            .expect("created metadata is readable")
            .attr;
        assert_eq!(created.uid, 10);
        assert_eq!(created.gid, 20);
        assert_eq!(created.perm, 0o640);

        let before_mode_change = storage
            .last_event_sequence()
            .expect("last event sequence is readable");
        let changed = storage
            .setattr_metadata(
                file,
                SetattrMetadata {
                    request_uid: 10,
                    request_gid: 20,
                    mode: Some(0o600),
                    uid: None,
                    gid: None,
                    size: None,
                    atime: None,
                    mtime: None,
                },
            )
            .expect("owner updates file mode")
            .attr;
        assert_eq!(changed.uid, 10);
        assert_eq!(changed.gid, 20);
        assert_eq!(changed.perm, 0o600);
        let mode_change_sequence = storage
            .last_event_sequence()
            .expect("last event sequence is reread");
        assert!(mode_change_sequence > before_mode_change);
        assert_eq!(
            storage
                .get_event(mode_change_sequence)
                .expect("metadata event is readable")
                .expect("metadata event exists")
                .kind(),
            EventKind::MetadataChanged,
        );

        storage
            .setattr_metadata(
                file,
                SetattrMetadata {
                    request_uid: 10,
                    request_gid: 20,
                    mode: Some(0o600),
                    uid: None,
                    gid: None,
                    size: None,
                    atime: None,
                    mtime: None,
                },
            )
            .expect("unchanged metadata is accepted");
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is reread"),
            mode_change_sequence
        );

        assert_fuse_errno(
            storage
                .setattr_metadata(
                    file,
                    SetattrMetadata {
                        request_uid: 10,
                        request_gid: 20,
                        mode: None,
                        uid: Some(11),
                        gid: None,
                        size: None,
                        atime: None,
                        mtime: None,
                    },
                )
                .expect_err("non-root owner cannot change uid"),
            fuser::Errno::EPERM,
        );

        let suid = storage
            .setattr_metadata(
                file,
                SetattrMetadata {
                    request_uid: 0,
                    request_gid: 0,
                    mode: Some(0o6755),
                    uid: None,
                    gid: None,
                    size: None,
                    atime: None,
                    mtime: None,
                },
            )
            .expect("root sets setuid and setgid mode bits")
            .attr;
        assert_eq!(suid.perm, 0o6755);

        let chowned = storage
            .setattr_metadata(
                file,
                SetattrMetadata {
                    request_uid: 0,
                    request_gid: 0,
                    mode: None,
                    uid: Some(100),
                    gid: Some(200),
                    size: None,
                    atime: None,
                    mtime: None,
                },
            )
            .expect("root updates ownership")
            .attr;
        assert_eq!(chowned.uid, 100);
        assert_eq!(chowned.gid, 200);
        assert_eq!(chowned.perm, 0o755);
    }

    #[test]
    fn rename_remove_and_link_edges_cover_user_errors() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");

        let parent_file = create_regular_file(&storage, "not-a-directory-parent");
        assert_fuse_errno(
            storage
                .create_node(
                    parent_file,
                    OsStr::new("child"),
                    CreateNodeKind::RegularFile,
                )
                .expect_err("create rejects non-directory parents"),
            fuser::Errno::ENOTDIR,
        );

        create_regular_file(&storage, "no-replace-source");
        create_regular_file(&storage, "no-replace-destination");
        assert_fuse_errno(
            storage
                .rename(
                    INODE_ROOT,
                    OsStr::new("no-replace-source"),
                    INODE_ROOT,
                    OsStr::new("no-replace-destination"),
                    true,
                )
                .expect_err("rename noreplace rejects existing destinations"),
            fuser::Errno::EEXIST,
        );

        let ancestor = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("ancestor"),
                CreateNodeKind::Directory,
            )
            .expect("ancestor directory is created")
            .attr
            .ino
            .into();
        let descendant = storage
            .create_node(
                ancestor,
                OsStr::new("descendant"),
                CreateNodeKind::Directory,
            )
            .expect("descendant directory is created")
            .attr
            .ino
            .into();
        assert_fuse_errno(
            storage
                .rename(
                    INODE_ROOT,
                    OsStr::new("ancestor"),
                    descendant,
                    OsStr::new("moved"),
                    false,
                )
                .expect_err("rename rejects moving a directory into itself"),
            fuser::Errno::EINVAL,
        );

        create_regular_file(&storage, "file-over-directory");
        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("directory-target"),
                CreateNodeKind::Directory,
            )
            .expect("directory target is created");
        assert_fuse_errno(
            storage
                .rename(
                    INODE_ROOT,
                    OsStr::new("file-over-directory"),
                    INODE_ROOT,
                    OsStr::new("directory-target"),
                    false,
                )
                .expect_err("rename rejects file over directory"),
            fuser::Errno::EISDIR,
        );

        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("directory-over-file"),
                CreateNodeKind::Directory,
            )
            .expect("directory source is created");
        create_regular_file(&storage, "file-target");
        assert_fuse_errno(
            storage
                .rename(
                    INODE_ROOT,
                    OsStr::new("directory-over-file"),
                    INODE_ROOT,
                    OsStr::new("file-target"),
                    false,
                )
                .expect_err("rename rejects directory over file"),
            fuser::Errno::ENOTDIR,
        );

        let nonempty_target = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("nonempty-target"),
                CreateNodeKind::Directory,
            )
            .expect("nonempty target is created")
            .attr
            .ino
            .into();
        storage
            .create_node(
                nonempty_target,
                OsStr::new("child"),
                CreateNodeKind::RegularFile,
            )
            .expect("target child is created");
        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("empty-directory-source"),
                CreateNodeKind::Directory,
            )
            .expect("empty source directory is created");
        assert_fuse_errno(
            storage
                .rename(
                    INODE_ROOT,
                    OsStr::new("empty-directory-source"),
                    INODE_ROOT,
                    OsStr::new("nonempty-target"),
                    false,
                )
                .expect_err("rename rejects replacing nonempty directories"),
            fuser::Errno::ENOTEMPTY,
        );

        let same_inode = create_regular_file(&storage, "same-inode");
        storage
            .hard_link(same_inode, INODE_ROOT, OsStr::new("same-inode-link"))
            .expect("hard link to same inode is created");
        storage
            .rename(
                INODE_ROOT,
                OsStr::new("same-inode"),
                INODE_ROOT,
                OsStr::new("same-inode-link"),
                false,
            )
            .expect("renaming over another link to the same inode succeeds");

        let linked_destination = create_regular_file(&storage, "linked-destination");
        storage
            .hard_link(
                linked_destination,
                INODE_ROOT,
                OsStr::new("linked-destination-survivor"),
            )
            .expect("destination hard link is created");
        create_regular_file(&storage, "linked-replacement-source");
        storage
            .rename(
                INODE_ROOT,
                OsStr::new("linked-replacement-source"),
                INODE_ROOT,
                OsStr::new("linked-destination"),
                false,
            )
            .expect("rename over hard-linked destination succeeds");
        storage
            .lookup(INODE_ROOT, OsStr::new("linked-destination-survivor"))
            .expect("surviving hard link remains addressable");

        assert_fuse_errno(
            storage
                .hard_link(INODE_ROOT, INODE_ROOT, OsStr::new("root-hard-link"))
                .expect_err("hard links to directories are rejected"),
            fuser::Errno::EPERM,
        );
        let hard_link_source = create_regular_file(&storage, "hard-link-source");
        create_regular_file(&storage, "hard-link-duplicate");
        assert_fuse_errno(
            storage
                .hard_link(
                    hard_link_source,
                    INODE_ROOT,
                    OsStr::new("hard-link-duplicate"),
                )
                .expect_err("hard links reject existing names"),
            fuser::Errno::EEXIST,
        );
        assert_fuse_errno(
            storage
                .write_file(INODE_ROOT, 0, b"nope")
                .expect_err("write rejects directories"),
            fuser::Errno::EISDIR,
        );

        let removable_directory = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("removable-directory"),
                CreateNodeKind::Directory,
            )
            .expect("removable directory is created")
            .attr
            .ino
            .into();
        storage
            .create_node(
                removable_directory,
                OsStr::new("child"),
                CreateNodeKind::RegularFile,
            )
            .expect("removable child is created");
        assert_fuse_errno(
            storage
                .rmdir(INODE_ROOT, OsStr::new("hard-link-source"))
                .expect_err("rmdir rejects regular files"),
            fuser::Errno::ENOTDIR,
        );
        assert_fuse_errno(
            storage
                .unlink(INODE_ROOT, OsStr::new("removable-directory"))
                .expect_err("unlink rejects directories"),
            fuser::Errno::EISDIR,
        );
        assert_fuse_errno(
            storage
                .rmdir(INODE_ROOT, OsStr::new("removable-directory"))
                .expect_err("rmdir rejects nonempty directories"),
            fuser::Errno::ENOTEMPTY,
        );
        storage
            .unlink(removable_directory, OsStr::new("child"))
            .expect("nested child is removed");
        storage
            .rmdir(INODE_ROOT, OsStr::new("removable-directory"))
            .expect("empty directory is removed");

        let scratch_name = BranchName::new("deleted-switch-target").expect("branch name is valid");
        let main = storage.current_branch().expect("main branch is readable");
        storage
            .create_branch(&scratch_name, main.head_position())
            .expect("scratch branch is created");
        storage
            .delete_branch(&scratch_name)
            .expect("scratch branch is deleted");
        assert!(matches!(
            storage.switch_branch(&scratch_name),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn allocator_branch_and_history_index_corruption_guards_fail_closed() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let first = create_regular_file(&storage, "corrupt-first");
        let original_write_state = write_state_snapshot(&storage);

        {
            let mut write_state = storage
                .write_state
                .lock()
                .expect("write state lock is available");
            write_state.next_inode_number = first;
        }
        assert!(matches!(
            storage.create_node(
                INODE_ROOT,
                OsStr::new("colliding-file"),
                CreateNodeKind::RegularFile,
            ),
            Err(FuseError::Integrity)
        ));
        {
            let mut write_state = storage
                .write_state
                .lock()
                .expect("write state lock is available");
            write_state.next_inode_number = first;
        }
        assert!(matches!(
            storage.create_symlink(
                INODE_ROOT,
                OsStr::new("colliding-link"),
                Path::new("target"),
            ),
            Err(FuseError::Integrity)
        ));
        {
            let mut write_state = storage
                .write_state
                .lock()
                .expect("write state lock is available");
            write_state.next_inode_number = original_write_state.next_inode_number;
        }

        let main = storage.current_branch().expect("main branch is readable");
        let duplicate_name = BranchName::new("duplicate").expect("branch name is valid");
        storage
            .create_branch(&duplicate_name, main.head_position())
            .expect("branch is created");
        assert!(matches!(
            storage.create_branch(&duplicate_name, main.head_position()),
            Err(FilesystemError::Integrity)
        ));
        let before_branch_corruption = write_state_snapshot(&storage);
        {
            let mut write_state = storage
                .write_state
                .lock()
                .expect("write state lock is available");
            write_state.next_branch_identifier = main.branch_identifier().get();
        }
        assert!(matches!(
            storage.create_branch(
                &BranchName::new("colliding-branch").expect("branch name is valid"),
                main.head_position(),
            ),
            Err(FilesystemError::Integrity)
        ));
        {
            let mut write_state = storage
                .write_state
                .lock()
                .expect("write state lock is available");
            write_state.next_branch_identifier = u64::MAX;
        }
        assert!(matches!(
            storage.create_branch(
                &BranchName::new("overflow-branch").expect("branch name is valid"),
                main.head_position(),
            ),
            Err(FilesystemError::Integrity)
        ));
        {
            let mut write_state = storage
                .write_state
                .lock()
                .expect("write state lock is available");
            write_state.next_branch_identifier = before_branch_corruption.next_branch_identifier;
        }

        let second = create_regular_file(&storage, "corrupt-second");
        let second_sequence = storage
            .last_event_sequence()
            .expect("second create sequence is readable");
        let second_ordinal = storage
            .get_event(second_sequence)
            .expect("second create event lookup succeeds")
            .expect("second create event exists")
            .branch_position()
            .expect("second create event has branch position")
            .ordinal();
        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .branch_file_events()
                .expect("branch file events column family exists"),
            encode_branch_file_position_key(main.branch_identifier().get(), first, second_ordinal),
            encode_u64(second_sequence.get()),
        );
        write_batch(storage.database(), batch).expect("corrupt branch file index is written");
        assert!(matches!(
            storage.list_branch_file_events(
                main.branch_identifier(),
                FileIdentifier::new(first),
                None,
                EventPageLimit::new(10).expect("limit is valid"),
            ),
            Err(FilesystemError::Integrity)
        ));

        let _ = second;
        let separate = tempfile::tempdir().expect("temporary directory is created");
        let storage = open_database_path(&separate.path().join("database")).expect("storage opens");
        create_regular_file(&storage, "branch-event-corrupt");
        let create_sequence = storage
            .last_event_sequence()
            .expect("create sequence is readable");
        let main = storage.current_branch().expect("main branch is readable");
        let bad_ordinal = main.head_position().ordinal() + 1;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .branch_events()
                .expect("branch events column family exists"),
            encode_branch_position_key(main.branch_identifier().get(), bad_ordinal),
            encode_u64(create_sequence.get()),
        );
        write_batch(storage.database(), batch).expect("corrupt branch event index is written");
        assert!(matches!(
            storage.list_branch_events(
                main.branch_identifier(),
                None,
                EventPageLimit::new(10).expect("limit is valid"),
            ),
            Err(FilesystemError::Integrity)
        ));

        let separate = tempfile::tempdir().expect("temporary directory is created");
        let storage = open_database_path(&separate.path().join("database")).expect("storage opens");
        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage.branches().expect("branches column family exists"),
            b"short",
            b"invalid",
        );
        write_batch(storage.database(), batch).expect("corrupt branch key is written");
        assert!(matches!(
            storage.create_branch(
                &BranchName::new("bad-branch-key").expect("branch name is valid"),
                storage
                    .current_branch()
                    .expect("main branch is readable")
                    .head_position(),
            ),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn branch_and_listing_cursor_edges_reject_invalid_or_exhausted_cursors() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let limit = EventPageLimit::new(2).expect("limit is valid");

        assert!(
            storage
                .list_events(Some(EventSequence::new(u64::MAX)), limit)
                .expect("exhausted event cursor lists as empty")
                .records()
                .is_empty()
        );
        assert!(
            storage
                .list_branches(
                    Some(BranchIdentifier::new(u64::MAX)),
                    BranchPageLimit::new(2).expect("limit is valid")
                )
                .expect("exhausted branch cursor lists as empty")
                .records()
                .is_empty()
        );

        let file = create_regular_file(&storage, "cursor-file");
        let main = storage.current_branch().expect("main branch is readable");
        assert!(
            storage
                .list_file_events(
                    FileIdentifier::new(file),
                    Some(EventSequence::new(u64::MAX)),
                    limit
                )
                .expect("exhausted active file cursor lists as empty")
                .records()
                .is_empty()
        );

        let wrong_branch_position =
            BranchPosition::new(BranchIdentifier::new(main.branch_identifier().get() + 1), 0);
        assert!(matches!(
            storage.list_branch_events(
                main.branch_identifier(),
                Some(wrong_branch_position),
                limit
            ),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            storage.list_branch_file_events(
                main.branch_identifier(),
                FileIdentifier::new(file),
                Some(wrong_branch_position),
                limit,
            ),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            storage.list_branch_events(
                main.branch_identifier(),
                Some(BranchPosition::new(main.branch_identifier(), u64::MAX)),
                limit,
            ),
            Err(FilesystemError::Integrity)
        ));

        assert!(matches!(
            storage.delete_branch(main.name()),
            Err(FilesystemError::Integrity)
        ));
        let deleted_name = BranchName::new("deleted-source").expect("branch name is valid");
        let deleted = storage
            .create_branch(&deleted_name, main.head_position())
            .expect("branch is created");
        storage
            .delete_branch(&deleted_name)
            .expect("inactive branch is deleted");
        assert!(matches!(
            storage.create_branch(
                &BranchName::new("from-deleted").expect("branch name is valid"),
                deleted.head_position(),
            ),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn key_metadata_and_identifier_decoders_fail_closed_on_corruption() {
        assert_eq!(decode_u64(&[1, 2, 3]), Err(FilesystemError::Integrity));
        assert_eq!(
            decode_branch_position_ordinal(&[0; 15]),
            Err(FilesystemError::Integrity)
        );
        assert_eq!(
            decode_branch_file_position_key(&[0; 23]),
            Err(FilesystemError::Integrity)
        );
        assert!(matches!(
            decode_stored_event(&[0xff; 8]),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            decode_branch(&[0xff; 8]),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            decode_branch_root(&[0xff; 8]),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            decode_snapshot_metadata(&[0xff; 8]),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            decode_event_payload_manifest(&[0xff; 8]),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            decode_content_manifest(&[0xff; 8]),
            Err(FilesystemError::Integrity)
        ));
        assert!(matches!(
            decode_namespace_node(&[0xff; 8]),
            Err(FilesystemError::Integrity)
        ));

        let empty_manifest = encode_content_manifest(&[]).expect("empty manifest encodes");
        let content_identifier = content_manifest_identifier_for_bytes(&empty_manifest);
        assert_eq!(
            ContentManifestIdentifier::from_bytes(&content_identifier)
                .expect("content manifest identifier is valid")
                .as_bytes(),
            content_identifier.as_slice()
        );
        assert_eq!(
            ContentManifestIdentifier::from_bytes(&content_identifier[1..]),
            Err(FilesystemError::Integrity)
        );
        let mut wrong_content_prefix = content_identifier.clone();
        wrong_content_prefix[0] = NAMESPACE_HASH_BLAKE3;
        assert_eq!(
            validate_content_manifest(&wrong_content_prefix, &empty_manifest),
            Err(FilesystemError::Integrity)
        );
        assert_eq!(
            validate_content_manifest(&content_identifier, b"changed"),
            Err(FilesystemError::Integrity)
        );

        let namespace_bytes = encode_namespace_node(&StoredNamespaceNode::Leaf(Vec::new()))
            .expect("namespace node encodes");
        let namespace_identifier = namespace_identifier_for_bytes(&namespace_bytes);
        assert_eq!(
            NamespaceIdentifier::from_bytes(&namespace_identifier)
                .expect("namespace identifier is valid")
                .as_bytes(),
            namespace_identifier.as_slice()
        );
        assert_eq!(
            NamespaceIdentifier::from_bytes(&namespace_identifier[1..]),
            Err(FilesystemError::Integrity)
        );
        assert_eq!(
            validate_namespace(&namespace_identifier, b"changed"),
            Err(FilesystemError::Integrity)
        );

        let invalid_branch = StoredBranch {
            identifier: 1,
            name: String::new(),
            status: BranchStatus::Open,
            head_sequence: 0,
            head_ordinal: 0,
            namespace_identifier,
            fork_branch_identifier: None,
            fork_ordinal: None,
        };
        assert!(matches!(
            branch_record_from_stored(invalid_branch),
            Err(FilesystemError::Integrity)
        ));

        assert_eq!(
            event_payload_part_byte(FileEventPayloadPart::Overwritten),
            EVENT_PAYLOAD_PART_OVERWRITTEN
        );
        assert_eq!(
            event_payload_part_byte(FileEventPayloadPart::Written),
            EVENT_PAYLOAD_PART_WRITTEN
        );
    }

    #[test]
    fn metadata_reads_reject_missing_malformed_and_non_utf8_values() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let metadata = storage
            .database()
            .cf_handle(COLUMN_FAMILY_FILESYSTEM_METADATA)
            .expect("metadata column family exists");

        let mut batch = WriteBatch::default();
        batch.delete_cf(metadata, METADATA_KEY_VOLUME_NAME);
        write_batch(storage.database(), batch).expect("volume name metadata is deleted");
        assert_eq!(
            read_metadata_string(storage.database(), METADATA_KEY_VOLUME_NAME),
            Err(FilesystemError::Integrity)
        );

        let mut batch = WriteBatch::default();
        batch.put_cf(metadata, METADATA_KEY_VOLUME_NAME, [0xff]);
        write_batch(storage.database(), batch).expect("invalid volume name metadata is written");
        assert_eq!(
            read_metadata_string(storage.database(), METADATA_KEY_VOLUME_NAME),
            Err(FilesystemError::Integrity)
        );

        let mut batch = WriteBatch::default();
        batch.put_cf(metadata, METADATA_KEY_NEXT_INODE_NUMBER, [1, 2, 3]);
        write_batch(storage.database(), batch).expect("invalid inode metadata is written");
        assert_eq!(
            read_metadata_u64(storage.database(), METADATA_KEY_NEXT_INODE_NUMBER),
            Err(FilesystemError::Integrity)
        );
        drop(storage);

        assert!(matches!(
            open_database_path(&database),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn namespace_trie_round_trips_and_splits_leaf_nodes() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let mut state = MaterializedBranchState::default();
        for inode_number in 1..=40 {
            state
                .inodes
                .insert(inode_number, test_inode(inode_number, StoredNodeKind::RegularFile));
        }
        state.directory_entries.insert(
            (INODE_ROOT, b"entry".to_vec()),
            StoredDirectoryEntry {
                inode: 2,
                kind: StoredNodeKind::RegularFile,
            },
        );
        state
            .extended_attributes
            .insert((2, b"user.eventfs.test".to_vec()), b"value".to_vec());

        let mut batch = WriteBatch::default();
        let identifier = put_namespace_state_in_database(storage.database(), &mut batch, &state)
            .expect("namespace trie is written");
        write_batch(storage.database(), batch).expect("namespace trie batch is committed");

        assert_eq!(
            namespace_state_for_identifier_in_database(storage.database(), &identifier)
                .expect("namespace trie is readable"),
            state
        );
        assert!(RootStorageCounts::capture(&storage).namespace_nodes > 4);
    }

    #[test]
    fn namespace_mutation_updates_multiple_entries_in_one_map() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let branch = storage.current_branch().expect("current branch is readable");
        let root = storage
            .branch_root_at_position(branch.head_position())
            .expect("branch root is readable");
        let mut mutation = StoredMutation::default();
        mutation.put_inode(INODE_ROOT, &test_inode(INODE_ROOT, StoredNodeKind::Directory));
        mutation.put_inode(2, &test_inode(2, StoredNodeKind::RegularFile));

        let mut batch = WriteBatch::default();
        let identifier = put_namespace_mutation_in_database(
            storage.database(),
            &mut batch,
            &root.namespace_identifier,
            &mutation,
        )
        .expect("namespace mutation is written");
        write_batch(storage.database(), batch).expect("namespace mutation batch is committed");

        let old_state =
            namespace_state_for_identifier_in_database(storage.database(), &root.namespace_identifier)
                .expect("old namespace remains readable");
        let new_state = namespace_state_for_identifier_in_database(storage.database(), &identifier)
            .expect("new namespace is readable");
        assert!(!old_state.inodes.contains_key(&2));
        assert_eq!(
            new_state
                .inodes
                .get(&2)
                .expect("new inode exists")
                .kind,
            StoredNodeKind::RegularFile
        );
    }

    #[test]
    fn namespace_trie_rejects_unsorted_entries_and_children() {
        let empty_leaf_bytes = encode_namespace_node(&StoredNamespaceNode::Leaf(Vec::new()))
            .expect("empty leaf encodes");
        let empty_leaf_identifier = namespace_identifier_for_bytes(&empty_leaf_bytes);

        assert_eq!(
            validate_namespace_entries(&[
                StoredNamespaceEntry {
                    key: b"b".to_vec(),
                    value: Vec::new(),
                },
                StoredNamespaceEntry {
                    key: b"a".to_vec(),
                    value: Vec::new(),
                },
            ]),
            Err(FilesystemError::Integrity)
        );
        assert_eq!(
            validate_namespace_children(&[
                StoredNamespaceChild {
                    nibble: 2,
                    identifier: empty_leaf_identifier.clone(),
                },
                StoredNamespaceChild {
                    nibble: 2,
                    identifier: empty_leaf_identifier,
                },
            ]),
            Err(FilesystemError::Integrity)
        );
    }

    #[test]
    fn snapshot_and_payload_manifest_corruption_paths_are_rejected() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "manifest-corruption");
        storage
            .write_file(inode_number, 0, b"abcdef")
            .expect("file is written");
        let sequence = storage
            .last_event_sequence()
            .expect("write event sequence is readable");
        let file_identifier = FileIdentifier::new(inode_number);
        let snapshot = storage
            .file_snapshot_at_or_before(file_identifier, sequence)
            .expect("snapshot lookup succeeds")
            .expect("snapshot exists");

        assert_eq!(
            storage
                .read_file_event_payload_range(sequence, FileEventPayloadPart::Written, 2, 3)
                .expect("payload slice is readable"),
            b"cde"
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(sequence, FileEventPayloadPart::Overwritten, 0, 3)
                .expect("missing payload part reads as empty"),
            Vec::<u8>::new()
        );
        assert_eq!(
            storage
                .read_file_snapshot_range(&snapshot, snapshot.file_size(), 16)
                .expect("snapshot range at EOF reads as empty"),
            Vec::<u8>::new()
        );

        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .event_payload_manifests()
                .expect("event payload manifests column family exists"),
            encode_event_payload_manifest_key(sequence.get(), EVENT_PAYLOAD_PART_WRITTEN),
            [0xff; 8],
        );
        write_batch(storage.database(), batch).expect("corrupt payload manifest is written");
        assert!(matches!(
            storage.read_file_event_payload_range(sequence, FileEventPayloadPart::Written, 0, 1,),
            Err(FilesystemError::Integrity)
        ));

        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .file_snapshot_manifests()
                .expect("file snapshot manifests column family exists"),
            encode_file_snapshot_manifest_key(
                snapshot.branch_position().branch_identifier().get(),
                snapshot.file_identifier().get(),
                snapshot.branch_position().ordinal(),
            ),
            [0xff; 8],
        );
        write_batch(storage.database(), batch).expect("corrupt snapshot manifest is written");
        assert!(matches!(
            storage.read_file_snapshot_range(&snapshot, 0, 1),
            Err(FilesystemError::Integrity)
        ));
    }

    #[test]
    fn name_xattr_statfs_and_file_type_mode_helpers_cover_edges() {
        assert_eq!(
            validate_name(OsStr::new("child")).expect("name is valid"),
            b"child"
        );
        assert_fuse_errno(
            validate_name(OsStr::new("")).expect_err("empty names are rejected"),
            fuser::Errno::ENOENT,
        );
        assert_fuse_errno(
            validate_name(OsStr::new(".")).expect_err("dot names are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            validate_name(OsStr::new("..")).expect_err("dot-dot names are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            validate_name(OsStr::new("bad/name")).expect_err("slash names are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            validate_name(OsStr::from_bytes(&[0xff])).expect_err("non-utf8 names are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            validate_name(OsStr::from_bytes(&[b'a'; FUSE_MAX_NAME_LENGTH + 1]))
                .expect_err("overlong names are rejected"),
            fuser::Errno::ENAMETOOLONG,
        );

        assert_eq!(
            validate_extended_attribute_name(OsStr::new("user.key")).expect("xattr name is valid"),
            b"user.key"
        );
        assert_fuse_errno(
            validate_extended_attribute_name(OsStr::new(""))
                .expect_err("empty xattr names are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            validate_extended_attribute_name(OsStr::from_bytes(b"user\0key"))
                .expect_err("nul-containing xattr names are rejected"),
            fuser::Errno::EINVAL,
        );
        assert_fuse_errno(
            validate_extended_attribute_name(OsStr::from_bytes(&[b'a'; FUSE_MAX_NAME_LENGTH + 1]))
                .expect_err("overlong xattr names are rejected"),
            fuser::Errno::ERANGE,
        );

        assert_eq!(
            special_kind_from_mode(libc::S_IFREG).expect("regular mode maps"),
            StoredNodeKind::RegularFile
        );
        assert_eq!(
            special_kind_from_mode(libc::S_IFIFO).expect("fifo mode maps"),
            StoredNodeKind::NamedPipe
        );
        assert_eq!(
            special_kind_from_mode(libc::S_IFCHR).expect("char mode maps"),
            StoredNodeKind::CharacterDevice
        );
        assert_eq!(
            special_kind_from_mode(libc::S_IFBLK).expect("block mode maps"),
            StoredNodeKind::BlockDevice
        );
        assert_eq!(
            special_kind_from_mode(libc::S_IFSOCK).expect("socket mode maps"),
            StoredNodeKind::Socket
        );
        assert_fuse_errno(
            special_kind_from_mode(libc::S_IFDIR).expect_err("directory special mode is rejected"),
            fuser::Errno::EINVAL,
        );
        assert_eq!(
            file_identifier_for_kind(12, StoredNodeKind::RegularFile),
            Some(FileIdentifier::new(12))
        );
        assert_eq!(
            file_identifier_for_kind(12, StoredNodeKind::Directory),
            None
        );

        assert!(matches!(
            statistics_from_backing(
                BackingFileSystemStatistics {
                    blocks: 1,
                    free_blocks: 1,
                    available_blocks: 1,
                    fragment_size: 4096,
                },
                INODE_FIRST_ALLOCATED - 1,
            ),
            Err(FuseError::Integrity)
        ));
        assert_fuse_errno(
            next_inode_number_after(u64::MAX).expect_err("inode number overflow is rejected"),
            fuser::Errno::ENOSPC,
        );
    }

    #[test]
    fn extent_write_and_bounds_helpers_cover_edge_errors() {
        let first = stored_extent_for_test(0, 4, 0, b"first");
        assert_eq!(
            slice_extents(std::slice::from_ref(&first), 1, 3, 100).expect("extent slice succeeds"),
            vec![StoredExtent {
                logical_offset: 100,
                length: 2,
                chunk_identifier: b"first".to_vec(),
                chunk_offset: 1,
            }]
        );
        assert!(
            slice_extents(std::slice::from_ref(&first), 2, 2, 0)
                .expect("empty slice succeeds")
                .is_empty()
        );

        assert!(matches!(
            validate_extent_bounds(&[stored_extent_for_test(10, 3, 0, b"bounds")], 12),
            Err(FuseError::Integrity)
        ));
        let overflowing = StoredExtent {
            logical_offset: u64::MAX,
            length: 1,
            chunk_identifier: b"overflow".to_vec(),
            chunk_offset: 0,
        };
        assert!(matches!(
            validate_extent_bounds(std::slice::from_ref(&overflowing), u64::MAX),
            Err(FuseError::Integrity)
        ));
        assert_fuse_errno(
            current_extents_after_write(
                &[],
                0,
                u64::MAX,
                u64::MAX,
                &[stored_extent_for_test(0, 1, 0, b"write")],
            )
            .expect_err("overflowing write range is rejected"),
            fuser::Errno::EFBIG,
        );
        assert!(matches!(
            stored_extents_from_pending(
                u64::MAX,
                &[PendingExtent {
                    logical_offset: 1,
                    length: 1,
                    byte_start: 0,
                    byte_end: 1,
                    chunk_identifier: b"pending".to_vec(),
                }],
            ),
            Err(FuseError::Integrity)
        ));
    }

    #[derive(Clone, Debug)]
    enum BoundedFileOperation {
        Write { offset: u64, bytes: Vec<u8> },
        Truncate { size: u64 },
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct BoundedFileExpectation {
        kind: EventKind,
        before: Vec<u8>,
        after: Vec<u8>,
        offset: Option<u64>,
        byte_length: Option<u64>,
        overwritten: Vec<u8>,
        written: Vec<u8>,
    }

    #[derive(Default)]
    struct BoundedExtentModel {
        extents: Vec<StoredExtent>,
        content_chunks: BTreeMap<Vec<u8>, Vec<u8>>,
        file_size: u64,
    }

    impl BoundedFileExpectation {
        fn created() -> Self {
            Self {
                kind: EventKind::FileCreated,
                before: Vec::new(),
                after: Vec::new(),
                offset: None,
                byte_length: None,
                overwritten: Vec::new(),
                written: Vec::new(),
            }
        }

        fn old_file_size(&self) -> Option<u64> {
            match self.kind {
                EventKind::FileWritten => Some(self.before.len() as u64),
                _ => None,
            }
        }

        fn new_file_size(&self) -> Option<u64> {
            match self.kind {
                EventKind::FileWritten => Some(self.after.len() as u64),
                _ => None,
            }
        }

        fn overwritten_byte_length(&self) -> Option<u64> {
            match self.kind {
                EventKind::FileWritten => Some(self.overwritten.len() as u64),
                _ => None,
            }
        }

        fn written_byte_length(&self) -> Option<u64> {
            match self.kind {
                EventKind::FileWritten => Some(self.written.len() as u64),
                _ => None,
            }
        }
    }

    impl BoundedExtentModel {
        fn apply(&mut self, operation: &BoundedFileOperation) -> FuseResult<()> {
            match operation {
                BoundedFileOperation::Write { offset, bytes } => self.apply_write(*offset, bytes),
                BoundedFileOperation::Truncate { size } => self.apply_truncate(*size),
            }
        }

        fn apply_write(&mut self, offset: u64, bytes: &[u8]) -> FuseResult<()> {
            let pending = chunk_bytes(bytes)?;
            let written_extents = stored_extents_from_pending(offset, &pending)?;
            for extent in &pending {
                self.content_chunks
                    .entry(extent.chunk_identifier.clone())
                    .or_insert_with(|| bytes[extent.byte_range()].to_vec());
            }

            let new_size = offset
                .checked_add(bytes.len() as u64)
                .ok_or(FuseError::Integrity)?
                .max(self.file_size);
            self.extents = current_extents_after_write(
                &self.extents,
                self.file_size,
                offset,
                new_size,
                &written_extents,
            )?;
            self.file_size = new_size;
            Ok(())
        }

        fn apply_truncate(&mut self, size: u64) -> FuseResult<()> {
            if size < self.file_size {
                self.extents =
                    slice_extents(&self.extents, 0, size, 0).map_err(to_fuse_integrity)?;
            }
            self.file_size = size;
            Ok(())
        }

        fn materialize(&self) -> Result<Vec<u8>, FilesystemError> {
            let mut bytes = vec![0; existing_byte_length_for_storage(self.file_size)?];
            for extent in &self.extents {
                let chunk = self
                    .content_chunks
                    .get(&extent.chunk_identifier)
                    .ok_or(FilesystemError::Integrity)?;
                let destination_start = existing_byte_length_for_storage(extent.logical_offset)?;
                let destination_end =
                    destination_start + existing_byte_length_for_storage(extent.length)?;
                let chunk_start = existing_byte_length_for_storage(extent.chunk_offset)?;
                let chunk_end = chunk_start + existing_byte_length_for_storage(extent.length)?;
                if chunk_end > chunk.len() || destination_end > bytes.len() {
                    return Err(FilesystemError::Integrity);
                }
                bytes[destination_start..destination_end]
                    .copy_from_slice(&chunk[chunk_start..chunk_end]);
            }
            Ok(bytes)
        }
    }

    fn bounded_extent_model_operations() -> Vec<BoundedFileOperation> {
        vec![
            BoundedFileOperation::Write {
                offset: 0,
                bytes: patterned_bytes(48 * 1024, 3),
            },
            BoundedFileOperation::Write {
                offset: 12 * 1024,
                bytes: patterned_bytes(96 * 1024, 11),
            },
            BoundedFileOperation::Write {
                offset: 170 * 1024,
                bytes: patterned_bytes(20 * 1024, 17),
            },
            BoundedFileOperation::Write {
                offset: 40 * 1024,
                bytes: patterned_bytes(120 * 1024, 29),
            },
            BoundedFileOperation::Write {
                offset: 192 * 1024,
                bytes: patterned_bytes(12 * 1024, 41),
            },
            BoundedFileOperation::Truncate { size: 96 * 1024 },
            BoundedFileOperation::Truncate { size: 180 * 1024 },
        ]
    }

    fn bounded_file_history_model_operations() -> Vec<BoundedFileOperation> {
        let dedup_bytes = patterned_bytes(96 * 1024, 7);
        vec![
            BoundedFileOperation::Write {
                offset: 0,
                bytes: dedup_bytes.clone(),
            },
            BoundedFileOperation::Write {
                offset: 0,
                bytes: dedup_bytes,
            },
            BoundedFileOperation::Write {
                offset: 24 * 1024,
                bytes: patterned_bytes(40 * 1024, 19),
            },
            BoundedFileOperation::Write {
                offset: 80 * 1024,
                bytes: patterned_bytes(20 * 1024, 37),
            },
            BoundedFileOperation::Write {
                offset: 128 * 1024,
                bytes: patterned_bytes(8 * 1024, 59),
            },
            BoundedFileOperation::Truncate { size: 72 * 1024 },
            BoundedFileOperation::Truncate { size: 160 * 1024 },
        ]
    }

    fn apply_bounded_file_operation(
        dense: &mut Vec<u8>,
        operation: &BoundedFileOperation,
    ) -> BoundedFileExpectation {
        match operation {
            BoundedFileOperation::Write { offset, bytes } => {
                let before = dense.clone();
                let offset = usize::try_from(*offset).expect("bounded write offset fits usize");
                let overwritten_end = before.len().min(offset + bytes.len());
                let overwritten = if offset < overwritten_end {
                    before[offset..overwritten_end].to_vec()
                } else {
                    Vec::new()
                };
                if dense.len() < offset {
                    dense.resize(offset, 0);
                }
                if dense.len() < offset + bytes.len() {
                    dense.resize(offset + bytes.len(), 0);
                }
                dense[offset..offset + bytes.len()].copy_from_slice(bytes);

                BoundedFileExpectation {
                    kind: EventKind::FileWritten,
                    before,
                    after: dense.clone(),
                    offset: Some(offset as u64),
                    byte_length: Some(bytes.len() as u64),
                    overwritten,
                    written: bytes.clone(),
                }
            }
            BoundedFileOperation::Truncate { size } => {
                let before = dense.clone();
                let size = usize::try_from(*size).expect("bounded truncate size fits usize");
                let overwritten = if size < before.len() {
                    before[size..].to_vec()
                } else {
                    Vec::new()
                };
                let event_offset = if size < before.len() {
                    size as u64
                } else {
                    before.len() as u64
                };
                dense.resize(size, 0);

                BoundedFileExpectation {
                    kind: EventKind::FileWritten,
                    before,
                    after: dense.clone(),
                    offset: Some(event_offset),
                    byte_length: Some(0),
                    overwritten,
                    written: Vec::new(),
                }
            }
        }
    }

    fn run_bounded_file_operation(
        storage: &Storage,
        inode_number: u64,
        operation: &BoundedFileOperation,
    ) {
        match operation {
            BoundedFileOperation::Write { offset, bytes } => {
                storage
                    .write_file(inode_number, *offset, bytes)
                    .expect("bounded write succeeds");
            }
            BoundedFileOperation::Truncate { size } => {
                storage
                    .setattr_metadata(
                        inode_number,
                        SetattrMetadata {
                            request_uid: 0,
                            request_gid: 0,
                            mode: None,
                            uid: None,
                            gid: None,
                            size: Some(*size),
                            atime: None,
                            mtime: None,
                        },
                    )
                    .expect("bounded truncate succeeds");
            }
        }
    }

    fn assert_storage_history_entry(
        storage: &Storage,
        inode_number: u64,
        file_identifier: FileIdentifier,
        sequence: EventSequence,
        expectation: &BoundedFileExpectation,
        history_bytes_by_sequence: &BTreeMap<EventSequence, Vec<u8>>,
    ) {
        let event = storage
            .get_event(sequence)
            .expect("history event lookup succeeds")
            .expect("history event exists");
        assert_eq!(event.file_identifier(), Some(file_identifier));
        assert_eq!(event.kind(), expectation.kind);
        assert_eq!(event.offset(), expectation.offset);
        assert_eq!(event.byte_length(), expectation.byte_length);
        assert_eq!(event.old_file_size(), expectation.old_file_size());
        assert_eq!(event.new_file_size(), expectation.new_file_size());
        assert_eq!(
            event.overwritten_byte_length(),
            expectation.overwritten_byte_length()
        );
        assert_eq!(
            event.written_byte_length(),
            expectation.written_byte_length()
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(
                    sequence,
                    FileEventPayloadPart::Overwritten,
                    0,
                    expectation.overwritten.len() as u64 + 64,
                )
                .expect("overwritten payload is readable"),
            expectation.overwritten
        );
        assert_eq!(
            storage
                .read_file_event_payload_range(
                    sequence,
                    FileEventPayloadPart::Written,
                    0,
                    expectation.written.len() as u64 + 64,
                )
                .expect("written payload is readable"),
            expectation.written
        );
        assert_eq!(
            storage
                .read_file(
                    inode_number,
                    0,
                    u32::try_from(expectation.after.len() + 16)
                        .expect("bounded file length fits u32"),
                )
                .expect("current file bytes are readable"),
            expectation.after
        );

        let snapshot = storage
            .file_snapshot_at_or_before(file_identifier, sequence)
            .expect("snapshot lookup succeeds")
            .expect("snapshot exists");
        let snapshot_bytes = storage
            .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
            .expect("snapshot bytes are readable");
        let expected_snapshot_bytes = history_bytes_by_sequence
            .get(&snapshot.sequence())
            .expect("snapshot sequence has expected bytes");
        assert_eq!(snapshot.file_size(), expected_snapshot_bytes.len() as u64);
        assert_eq!(snapshot_bytes, *expected_snapshot_bytes);
    }

    fn assert_extent_manifest_is_sorted_and_non_overlapping(
        extents: &[StoredExtent],
        file_size: u64,
    ) {
        for extent in extents {
            assert!(extent.length > 0);
            assert!(extent.logical_offset + extent.length <= file_size);
        }
        for pair in extents.windows(2) {
            assert!(pair[0].logical_offset + pair[0].length <= pair[1].logical_offset);
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    struct RootStorageCounts {
        content_chunks: usize,
        content_manifest_nodes: usize,
        namespace_nodes: usize,
        file_snapshot_manifests: usize,
    }

    impl RootStorageCounts {
        fn capture(storage: &Storage) -> Self {
            Self {
                content_chunks: count_column_family(
                    storage,
                    storage
                        .content_chunks()
                        .expect("content chunks column family exists"),
                ),
                content_manifest_nodes: count_column_family(
                    storage,
                    storage
                        .content_manifest_nodes()
                        .expect("content manifest nodes column family exists"),
                ),
                namespace_nodes: count_column_family(
                    storage,
                    storage
                        .namespace_nodes()
                        .expect("namespace nodes column family exists"),
                ),
                file_snapshot_manifests: count_column_family(
                    storage,
                    storage
                        .file_snapshot_manifests()
                        .expect("file snapshot manifests column family exists"),
                ),
            }
        }
    }

    fn count_content_chunks(storage: &Storage) -> usize {
        let content_chunks = storage
            .content_chunks()
            .expect("content chunks column family exists");
        count_column_family(storage, content_chunks)
    }

    fn count_column_family(storage: &Storage, column_family: &ColumnFamily) -> usize {
        let mut iterator = storage.database().raw_iterator_cf(column_family);
        let mut count = 0;

        iterator.seek_to_first();
        while iterator.valid() {
            count += 1;
            iterator.next();
        }
        iterator.status().expect("column family iterator is valid");
        count
    }

    fn collect_all_events(storage: &Storage) -> Vec<EventRecord> {
        let mut records = Vec::new();
        let mut after = None;
        let limit = EventPageLimit::new(2).expect("limit is valid");

        loop {
            let page = storage
                .list_events(after, limit)
                .expect("event page is readable");
            records.extend(page.records().iter().cloned());
            match page.next_after() {
                Some(next_after) => after = Some(next_after),
                None => return records,
            }
        }
    }

    fn collect_all_file_events(
        storage: &Storage,
        file_identifier: FileIdentifier,
    ) -> Vec<EventRecord> {
        let mut records = Vec::new();
        let mut after = None;
        let limit = EventPageLimit::new(2).expect("limit is valid");

        loop {
            let page = storage
                .list_file_events(file_identifier, after, limit)
                .expect("file event page is readable");
            records.extend(page.records().iter().cloned());
            match page.next_after() {
                Some(next_after) => after = Some(next_after),
                None => return records,
            }
        }
    }

    fn collect_all_branch_events(storage: &Storage, branch: BranchIdentifier) -> Vec<EventRecord> {
        let mut records = Vec::new();
        let mut after = None;
        let limit = EventPageLimit::new(2).expect("limit is valid");

        loop {
            let page = storage
                .list_branch_events(branch, after, limit)
                .expect("branch event page is readable");
            records.extend(page.records().iter().cloned());
            match page.next_after() {
                Some(next_after) => after = Some(next_after),
                None => return records,
            }
        }
    }

    fn collect_all_branch_file_events(
        storage: &Storage,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
    ) -> Vec<EventRecord> {
        let mut records = Vec::new();
        let mut after = None;
        let limit = EventPageLimit::new(2).expect("limit is valid");

        loop {
            let page = storage
                .list_branch_file_events(branch, file_identifier, after, limit)
                .expect("branch file event page is readable");
            records.extend(page.records().iter().cloned());
            match page.next_after() {
                Some(next_after) => after = Some(next_after),
                None => return records,
            }
        }
    }

    fn reconstruct_file_bytes_at_sequence(
        storage: &Storage,
        file_identifier: FileIdentifier,
        target: EventSequence,
    ) -> Vec<u8> {
        let snapshot = storage
            .file_snapshot_at_or_before(file_identifier, target)
            .expect("snapshot lookup succeeds")
            .expect("snapshot exists");
        let mut bytes = storage
            .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
            .expect("snapshot bytes are readable");
        let mut after = Some(snapshot.sequence());

        loop {
            let page = storage
                .list_file_events(
                    file_identifier,
                    after,
                    EventPageLimit::new(2).expect("limit is valid"),
                )
                .expect("file event page is readable");
            for event in page.records() {
                if event.sequence() > target {
                    return bytes;
                }
                apply_listed_file_event(storage, &mut bytes, event);
            }
            match page.next_after() {
                Some(next_after) if next_after <= target => after = Some(next_after),
                Some(_) | None => return bytes,
            }
        }
    }

    fn apply_listed_file_event(storage: &Storage, bytes: &mut Vec<u8>, event: &EventRecord) {
        match event.kind() {
            EventKind::FileCreated => bytes.clear(),
            EventKind::FileWritten => {
                let offset = usize::try_from(event.offset().expect("write offset exists"))
                    .expect("write offset fits usize");
                let written = storage
                    .read_file_event_payload_range(
                        event.sequence(),
                        FileEventPayloadPart::Written,
                        0,
                        event.written_byte_length().expect("written length exists"),
                    )
                    .expect("written payload is readable");
                let end = offset + written.len();
                if bytes.len() < offset {
                    bytes.resize(offset, 0);
                }
                if bytes.len() < end {
                    bytes.resize(end, 0);
                }
                bytes[offset..end].copy_from_slice(&written);
                bytes.resize(
                    usize::try_from(event.new_file_size().expect("new file size exists"))
                        .expect("new file size fits usize"),
                    0,
                );
            }
            _ => {}
        }
    }

    fn write_state_snapshot(storage: &Storage) -> WriteState {
        *storage
            .write_state
            .lock()
            .expect("write state lock is available")
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ActiveBranchStateSnapshot {
        branch: StoredBranch,
        namespace_root: StoredNamespaceRoot,
        namespace: MaterializedBranchState,
    }

    fn active_branch_state_snapshot(storage: &Storage) -> ActiveBranchStateSnapshot {
        let active = storage
            .active_branch_state
            .lock()
            .expect("active branch state lock is available");
        ActiveBranchStateSnapshot {
            branch: active.branch.clone(),
            namespace_root: active.namespace_root.clone(),
            namespace: active.namespace.clone(),
        }
    }

    fn test_inode(inode_number: u64, kind: StoredNodeKind) -> StoredInode {
        let time = StoredTime {
            seconds: 0,
            nanoseconds: 0,
        };
        StoredInode {
            kind,
            size: 0,
            nlink: if kind == StoredNodeKind::Directory {
                2
            } else {
                1
            },
            uid: 0,
            gid: 0,
            mode: if kind == StoredNodeKind::Directory {
                INODE_MODE_ROOT
            } else {
                INODE_MODE_DEFAULT_FILE
            },
            rdev: 0,
            atime: time,
            mtime: time,
            ctime: time,
            parent: INODE_ROOT,
            name: if inode_number == INODE_ROOT {
                Vec::new()
            } else {
                format!("inode-{inode_number}").into_bytes()
            },
            symlink_target: None,
            content_manifest_identifier: None,
        }
    }

    fn create_regular_file(storage: &Storage, name: &str) -> u64 {
        storage
            .create_node(INODE_ROOT, OsStr::new(name), CreateNodeKind::RegularFile)
            .expect("regular file is created")
            .attr
            .ino
            .into()
    }

    fn stored_extent_for_test(
        logical_offset: u64,
        length: u64,
        chunk_offset: u64,
        chunk_identifier: &[u8],
    ) -> StoredExtent {
        StoredExtent {
            logical_offset,
            length,
            chunk_identifier: chunk_identifier.to_vec(),
            chunk_offset,
        }
    }

    fn assert_fuse_errno(error: FuseError, expected: fuser::Errno) {
        match error {
            FuseError::Errno(actual) => assert_eq!(actual.code(), expected.code()),
            FuseError::Database | FuseError::Integrity => {
                panic!("expected errno {}, got {error:?}", expected.code())
            }
        }
    }

    fn patterned_bytes(length: usize, salt: usize) -> Vec<u8> {
        (0..length)
            .map(|index| ((index * 31 + salt) % 251) as u8)
            .collect()
    }
}
