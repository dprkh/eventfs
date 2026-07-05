#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::mem::MaybeUninit;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB,
    DataBlockIndexType, Direction, IteratorMode, Options, SliceTransform, WriteBatch, WriteOptions,
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
pub(crate) struct SetAttributes {
    pub(crate) mode: Option<u32>,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) size: Option<u64>,
    pub(crate) atime: Option<fuser::TimeOrNow>,
    pub(crate) mtime: Option<fuser::TimeOrNow>,
    pub(crate) ctime: Option<SystemTime>,
    pub(crate) crtime: Option<SystemTime>,
    pub(crate) bkuptime: Option<SystemTime>,
    pub(crate) flags: Option<u32>,
}

#[derive(Clone, Debug)]
pub(crate) struct FuseWrite {
    pub(crate) written: u32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FuseCredentials {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct ExtendedAttributeList {
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExtendedTimes {
    pub(crate) bkuptime: SystemTime,
    pub(crate) crtime: SystemTime,
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

pub(crate) fn open_database(
    configuration: &FilesystemConfiguration,
) -> Result<Storage, FilesystemError> {
    open_database_path(configuration.database_directory())
}

pub(crate) fn open_database_path(path: &Path) -> Result<Storage, FilesystemError> {
    let is_new_database = is_new_database_directory(path)?;
    let block_cache = Cache::new_lru_cache(ROCKSDB_BLOCK_CACHE_CAPACITY);
    let options = database_options(is_new_database, &block_cache);

    if is_new_database {
        let descriptors = column_family_descriptors(required_column_family_names(), &block_cache);
        let database = DB::open_cf_descriptors(&options, path, descriptors)
            .map_err(|_| FilesystemError::Database)?;
        initialize_new_database(&database)?;
        let write_state = load_write_state(&database)?;
        Ok(storage(database, write_state))
    } else {
        let existing_column_families = existing_column_family_names(&options, path)?;
        validate_required_column_families(&existing_column_families)?;
        let descriptors = column_family_descriptors(existing_column_families, &block_cache);
        let database = DB::open_cf_descriptors(&options, path, descriptors)
            .map_err(|_| FilesystemError::Database)?;
        validate_existing_database(&database)?;
        let write_state = load_write_state(&database)?;
        Ok(storage(database, write_state))
    }
}

fn storage(database: DB, write_state: WriteState) -> Storage {
    let active_branch_identifier = write_state.active_branch_identifier;
    Storage {
        database,
        write_state: Mutex::new(write_state),
        active_branch_identifier: AtomicU64::new(active_branch_identifier),
    }
}

type FuseResult<T> = Result<T, FuseError>;

const STORAGE_SCHEMA_VERSION_CURRENT: u64 = 7;

const EVENT_SEQUENCE_INITIAL: EventSequence = EventSequence::new(0);
const BRANCH_IDENTIFIER_INITIAL: BranchIdentifier = BranchIdentifier::new(1);
const BRANCH_NAME_INITIAL: &str = "main";

const INODE_ROOT: u64 = 1;
const INODE_FIRST_ALLOCATED: u64 = 2;
const INODE_LOGICAL_CAPACITY: u64 = u64::MAX - 1;

const CONTENT_CHUNK_MIN_SIZE: usize = 16 * 1024;
const CONTENT_CHUNK_TARGET_SIZE: usize = 64 * 1024;
const CONTENT_CHUNK_MAX_SIZE: usize = 256 * 1024;
const CONTENT_CHUNK_FASTCDC_SEED: u64 = 0;

const FUSE_ATTRIBUTE_TTL: Duration = Duration::from_secs(1);
const FUSE_STATFS_BLOCK_SIZE: u32 = 4096;
const FUSE_MAX_NAME_LENGTH: usize = 255;

const ROCKSDB_BLOCK_CACHE_CAPACITY: usize = 128 * 1024 * 1024;
const ROCKSDB_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;
const ROCKSDB_BYTES_PER_SYNC: u64 = 1024 * 1024;
const ROCKSDB_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
const ROCKSDB_MAX_WRITE_BUFFER_NUMBER: i32 = 4;
const ROCKSDB_BLOB_MIN_SIZE: u64 = 1;

const COLUMN_FAMILY_EVENTS: &str = "events";
const COLUMN_FAMILY_INODES: &str = "inodes";
const COLUMN_FAMILY_DIRECTORY_ENTRIES: &str = "directory_entries";
const COLUMN_FAMILY_EXTENDED_ATTRIBUTES: &str = "extended_attributes";
const COLUMN_FAMILY_FILESYSTEM_METADATA: &str = "filesystem_metadata";
const COLUMN_FAMILY_FILE_EVENTS: &str = "file_events";
const COLUMN_FAMILY_BRANCHES: &str = "branches";
const COLUMN_FAMILY_BRANCH_NAMES: &str = "branch_names";
const COLUMN_FAMILY_BRANCH_EVENTS: &str = "branch_events";
const COLUMN_FAMILY_BRANCH_FILE_EVENTS: &str = "branch_file_events";
const COLUMN_FAMILY_CONTENT_CHUNKS: &str = "content_chunks";
const COLUMN_FAMILY_CURRENT_FILE_EXTENTS: &str = "current_file_extents";
const COLUMN_FAMILY_FILE_SNAPSHOT_EXTENTS: &str = "file_snapshot_extents";
const COLUMN_FAMILY_EVENT_PAYLOAD_EXTENTS: &str = "event_payload_extents";
const COLUMN_FAMILY_REQUIRED: &[&str] = &[
    COLUMN_FAMILY_EVENTS,
    COLUMN_FAMILY_INODES,
    COLUMN_FAMILY_DIRECTORY_ENTRIES,
    COLUMN_FAMILY_EXTENDED_ATTRIBUTES,
    COLUMN_FAMILY_FILESYSTEM_METADATA,
    COLUMN_FAMILY_FILE_EVENTS,
    COLUMN_FAMILY_BRANCHES,
    COLUMN_FAMILY_BRANCH_NAMES,
    COLUMN_FAMILY_BRANCH_EVENTS,
    COLUMN_FAMILY_BRANCH_FILE_EVENTS,
    COLUMN_FAMILY_CONTENT_CHUNKS,
    COLUMN_FAMILY_CURRENT_FILE_EXTENTS,
    COLUMN_FAMILY_FILE_SNAPSHOT_EXTENTS,
    COLUMN_FAMILY_EVENT_PAYLOAD_EXTENTS,
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
const EVENT_PAYLOAD_PART_REMOVED: u8 = b'r';
const CONTENT_CHUNK_HASH_BLAKE3: u8 = 1;

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
    mode: u16,
    uid: u32,
    gid: u32,
    size: u64,
    nlink: u32,
    rdev: u32,
    atime: StoredTime,
    mtime: StoredTime,
    ctime: StoredTime,
    crtime: StoredTime,
    bkuptime: StoredTime,
    flags: u32,
    parent: u64,
    name: Vec<u8>,
    symlink_target: Option<Vec<u8>>,
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
    fork_branch_identifier: Option<u64>,
    fork_ordinal: Option<u64>,
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
struct StoredSnapshotMetadata {
    file_size: u64,
    sequence: u64,
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
    FileTruncate {
        removed_extents: Vec<StoredExtent>,
    },
}

#[derive(Clone, Debug, Default)]
struct MaterializedBranchState {
    inodes: BTreeMap<u64, StoredInode>,
    directory_entries: BTreeMap<(u64, Vec<u8>), StoredDirectoryEntry>,
    extended_attributes: BTreeMap<(u64, Vec<u8>), Vec<u8>>,
}

#[derive(Clone, Copy, Debug)]
struct DirectoryEntryLocation<'a> {
    parent: u64,
    name: &'a [u8],
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
        let branch_identifier = self.active_branch_identifier();
        self.branch_record(branch_identifier)
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
        if self.branch_name_exists(&name)? {
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
        self.copy_branch_state_at_position(&mut batch, from, branch_identifier, fork_sequence)?;
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
        let record = self.branch_record(branch_identifier)?;
        if record.status() != BranchStatus::Open {
            return Err(FilesystemError::Integrity);
        }
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

    pub(crate) fn create_node(
        &self,
        parent: u64,
        name: &OsStr,
        requested_mode: u32,
        umask: u32,
        credentials: FuseCredentials,
        kind: CreateNodeKind,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut parent_inode = self.require_directory_with_access(
            parent,
            credentials.uid,
            credentials.gid,
            write_directory_access_mask(),
        )?;
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
        let path = child_event_path(
            self.active_branch_identifier(),
            parent,
            &name_bytes,
            &self.database,
        )?;
        let (stored_kind, mode, rdev, size, symlink_target, event_kind) = match kind {
            CreateNodeKind::Directory => (
                StoredNodeKind::Directory,
                permissions_from_mode(requested_mode, umask),
                0,
                0,
                None,
                EventKind::DirectoryCreated,
            ),
            CreateNodeKind::RegularFile => (
                StoredNodeKind::RegularFile,
                permissions_from_mode(requested_mode, umask),
                0,
                0,
                None,
                EventKind::FileCreated,
            ),
            CreateNodeKind::Special { mode, rdev } => (
                special_kind_from_mode(mode)?,
                permissions_from_mode(mode, umask),
                rdev,
                0,
                None,
                EventKind::NodeCreated,
            ),
        };
        let inode = StoredInode {
            kind: stored_kind,
            mode,
            uid: credentials.uid,
            gid: credentials.gid,
            size,
            nlink: match stored_kind {
                StoredNodeKind::Directory => 2,
                _ => 1,
            },
            rdev,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            bkuptime: now,
            flags: 0,
            parent,
            name: name_bytes.clone(),
            symlink_target,
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

        let mut batch = WriteBatch::default();
        self.put_inode(&mut batch, inode_number, &inode)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let entry = StoredDirectoryEntry {
            inode: inode_number,
            kind: stored_kind,
        };
        self.put_directory_entry(&mut batch, parent, &name_bytes, entry.clone())?;
        mutation.put_directory_entry(parent, &name_bytes, &entry);
        if stored_kind == StoredNodeKind::Directory {
            parent_inode.nlink = parent_inode.nlink.saturating_add(1);
        }
        self.touch_loaded_directory(&mut batch, parent, &mut parent_inode, now)?;
        mutation.put_inode(parent, &parent_inode);
        self.commit_event(&mut batch, event_sequence, &event, mutation)?;
        self.put_metadata_u64(
            &mut batch,
            METADATA_KEY_NEXT_INODE_NUMBER,
            next_inode_number,
        )?;
        self.commit_mutation(
            &mut write_state,
            batch,
            event_sequence,
            Some(next_inode_number),
        )?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    pub(crate) fn create_symlink(
        &self,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        uid: u32,
        gid: u32,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut parent_inode =
            self.require_directory_with_access(parent, uid, gid, write_directory_access_mask())?;
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
        let path = child_event_path(
            self.active_branch_identifier(),
            parent,
            &name_bytes,
            &self.database,
        )?;
        let target_bytes = target.as_os_str().as_bytes().to_vec();
        let inode = StoredInode {
            kind: StoredNodeKind::Symlink,
            mode: 0o777,
            uid,
            gid,
            size: target_bytes.len() as u64,
            nlink: 1,
            rdev: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            bkuptime: now,
            flags: 0,
            parent,
            name: name_bytes.clone(),
            symlink_target: Some(target_bytes),
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

        let mut batch = WriteBatch::default();
        self.put_inode(&mut batch, inode_number, &inode)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let entry = StoredDirectoryEntry {
            inode: inode_number,
            kind: StoredNodeKind::Symlink,
        };
        self.put_directory_entry(&mut batch, parent, &name_bytes, entry.clone())?;
        mutation.put_directory_entry(parent, &name_bytes, &entry);
        self.touch_loaded_directory(&mut batch, parent, &mut parent_inode, now)?;
        mutation.put_inode(parent, &parent_inode);
        self.commit_event(&mut batch, event_sequence, &event, mutation)?;
        self.put_metadata_u64(
            &mut batch,
            METADATA_KEY_NEXT_INODE_NUMBER,
            next_inode_number,
        )?;
        self.commit_mutation(
            &mut write_state,
            batch,
            event_sequence,
            Some(next_inode_number),
        )?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    pub(crate) fn setattr(
        &self,
        inode_number: u64,
        attributes: SetAttributes,
        uid: u32,
        gid: u32,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if uid != 0 && uid != inode.uid {
            return Err(FuseError::Errno(fuser::Errno::EACCES));
        }
        if attributes.size.is_some()
            && uid != 0
            && !access_allowed(&inode, uid, gid, fuser::AccessFlags::W_OK)
        {
            return Err(FuseError::Errno(fuser::Errno::EACCES));
        }
        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        let mut event_kind = EventKind::MetadataChanged;
        let mut offset = None;
        let mut byte_length = None;
        let mut payload = EventPayload::None;
        let mut payload_extents = EventPayloadExtents::None;
        let mut snapshot_extents = None;

        if let Some(mode) = attributes.mode {
            inode.mode = permission_bits(mode);
        }
        if let Some(uid) = attributes.uid {
            inode.uid = uid;
        }
        if let Some(gid) = attributes.gid {
            inode.gid = gid;
        }
        if let Some(atime) = attributes.atime {
            inode.atime = stored_time_from_time_or_now(atime);
        }
        if let Some(mtime) = attributes.mtime {
            inode.mtime = stored_time_from_time_or_now(mtime);
        }
        if let Some(ctime) = attributes.ctime {
            inode.ctime = StoredTime::from_system_time(ctime);
        }
        if let Some(crtime) = attributes.crtime {
            inode.crtime = StoredTime::from_system_time(crtime);
        }
        if let Some(bkuptime) = attributes.bkuptime {
            inode.bkuptime = StoredTime::from_system_time(bkuptime);
        }
        if let Some(flags) = attributes.flags {
            inode.flags = flags;
        }

        let mut batch = WriteBatch::default();
        if let Some(size) = attributes.size {
            if inode.kind != StoredNodeKind::RegularFile {
                return Err(FuseError::Errno(fuser::Errno::EINVAL));
            }
            let old_size = inode.size;
            let existing_extents = self
                .current_file_extent_manifest(
                    self.active_branch_identifier(),
                    FileIdentifier::new(inode_number),
                )
                .map_err(to_fuse_integrity)?;
            self.validate_stored_extents(&existing_extents)?;
            let final_extents = current_extents_after_truncate(&existing_extents, size)?;
            self.replace_current_file_extents(&mut batch, inode_number, &final_extents)?;
            if size < old_size {
                payload_extents = EventPayloadExtents::FileTruncate {
                    removed_extents: slice_extents(&existing_extents, size, old_size, 0)
                        .map_err(to_fuse_integrity)?,
                };
            }
            snapshot_extents = Some(final_extents);
            inode.size = size;
            inode.mtime = now;
            event_kind = EventKind::FileTruncated;
            offset = Some(old_size.min(size));
            byte_length = Some(old_size.abs_diff(size));
            payload = EventPayload::FileTruncate {
                old_file_size: old_size,
                new_file_size: size,
                removed_byte_length: old_size.saturating_sub(size),
            };
        }
        inode.ctime = now;
        self.put_inode(&mut batch, inode_number, &inode)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);

        let event = EventRecord::new(
            event_sequence,
            event_kind,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, inode.kind),
            inode_event_path(
                self.active_branch_identifier(),
                inode_number,
                &inode,
                &self.database,
            )
            .ok(),
            offset,
            byte_length,
        )
        .with_payload(payload);
        self.commit_event_with_payloads(
            &mut batch,
            event_sequence,
            &event,
            payload_extents,
            snapshot_extents.as_deref(),
            None,
            mutation,
        )?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    pub(crate) fn unlink(&self, parent: u64, name: &OsStr, uid: u32, gid: u32) -> FuseResult<()> {
        self.remove_directory_entry(parent, name, false, uid, gid)
    }

    pub(crate) fn rmdir(&self, parent: u64, name: &OsStr, uid: u32, gid: u32) -> FuseResult<()> {
        self.remove_directory_entry(parent, name, true, uid, gid)
    }

    pub(crate) fn rename(
        &self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        no_replace: bool,
        credentials: FuseCredentials,
    ) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let name_bytes = validate_name(name)?;
        let new_name_bytes = validate_name(new_name)?;
        let mut old_parent_inode = self.require_directory_with_access(
            parent,
            credentials.uid,
            credentials.gid,
            write_directory_access_mask(),
        )?;
        let mut new_parent_inode = if new_parent == parent {
            None
        } else {
            Some(self.require_directory_with_access(
                new_parent,
                credentials.uid,
                credentials.gid,
                write_directory_access_mask(),
            )?)
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
        let new_path = child_event_path(
            self.active_branch_identifier(),
            new_parent,
            &new_name_bytes,
            &self.database,
        )?;
        let mut batch = WriteBatch::default();
        let mut removed_destination = None;

        if let Some(destination) = existing_destination {
            if destination.inode == entry.inode {
                self.delete_directory_entry(&mut batch, parent, &name_bytes)?;
                self.put_directory_entry(&mut batch, new_parent, &new_name_bytes, entry.clone())?;
            } else {
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
                self.remove_entry_in_batch(
                    &mut batch,
                    DirectoryEntryLocation {
                        parent: new_parent,
                        name: &new_name_bytes,
                    },
                    destination,
                    destination_inode,
                    destination_parent_inode,
                    now,
                )?;
                self.delete_directory_entry(&mut batch, parent, &name_bytes)?;
                self.put_directory_entry(&mut batch, new_parent, &new_name_bytes, entry.clone())?;
            }
        } else {
            self.delete_directory_entry(&mut batch, parent, &name_bytes)?;
            self.put_directory_entry(&mut batch, new_parent, &new_name_bytes, entry.clone())?;
        }

        if entry.kind == StoredNodeKind::Directory && parent != new_parent {
            old_parent_inode.nlink = old_parent_inode.nlink.saturating_sub(1);
            let new_parent_inode = new_parent_inode.as_mut().ok_or(FuseError::Integrity)?;
            new_parent_inode.nlink = new_parent_inode.nlink.saturating_add(1);
        }
        inode.parent = new_parent;
        inode.name = new_name_bytes.clone();
        inode.ctime = now;
        self.put_inode(&mut batch, entry.inode, &inode)?;
        self.touch_loaded_directory(&mut batch, parent, &mut old_parent_inode, now)?;
        if let Some(new_parent_inode) = &mut new_parent_inode {
            self.touch_loaded_directory(&mut batch, new_parent, new_parent_inode, now)?;
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
        self.commit_event(&mut batch, event_sequence, &event, mutation)?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)
    }

    pub(crate) fn hard_link(
        &self,
        inode_number: u64,
        new_parent: u64,
        new_name: &OsStr,
        uid: u32,
        gid: u32,
    ) -> FuseResult<EntrySnapshot> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind == StoredNodeKind::Directory {
            return Err(FuseError::Errno(fuser::Errno::EPERM));
        }
        let mut parent_inode = self.require_directory_with_access(
            new_parent,
            uid,
            gid,
            write_directory_access_mask(),
        )?;
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
        let path = child_event_path(
            self.active_branch_identifier(),
            new_parent,
            &name_bytes,
            &self.database,
        )?;
        let mut batch = WriteBatch::default();
        self.put_inode(&mut batch, inode_number, &inode)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let entry = StoredDirectoryEntry {
            inode: inode_number,
            kind: inode.kind,
        };
        self.put_directory_entry(&mut batch, new_parent, &name_bytes, entry.clone())?;
        mutation.put_directory_entry(new_parent, &name_bytes, &entry);
        self.touch_loaded_directory(&mut batch, new_parent, &mut parent_inode, now)?;
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
        self.commit_event(&mut batch, event_sequence, &event, mutation)?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)?;

        Ok(EntrySnapshot {
            ttl: FUSE_ATTRIBUTE_TTL,
            attr: inode_attr(inode_number, &inode),
        })
    }

    pub(crate) fn open_file(
        &self,
        inode_number: u64,
        uid: u32,
        gid: u32,
        flags: fuser::OpenFlags,
    ) -> FuseResult<()> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        self.check_inode_access(&inode, uid, gid, access_mask_for_open_flags(flags))?;
        Ok(())
    }

    pub(crate) fn read_file(
        &self,
        inode_number: u64,
        offset: u64,
        size: u32,
        uid: u32,
        gid: u32,
    ) -> FuseResult<Vec<u8>> {
        let snapshot = self.database.snapshot();
        let inode = self
            .inode_from_snapshot(&snapshot, inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        self.check_inode_access(&inode, uid, gid, fuser::AccessFlags::R_OK)?;
        if offset >= inode.size || size == 0 {
            return Ok(Vec::new());
        }
        let readable = u64::from(size).min(inode.size - offset) as usize;
        drop(snapshot);
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

    pub(crate) fn write_file(
        &self,
        inode_number: u64,
        offset: u64,
        data: &[u8],
        uid: u32,
        gid: u32,
    ) -> FuseResult<FuseWrite> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        self.check_inode_access(&inode, uid, gid, fuser::AccessFlags::W_OK)?;
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or(FuseError::Errno(fuser::Errno::EFBIG))?;
        let new_size = inode.size.max(end);
        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        let old_size = inode.size;
        let existing_extents = self
            .current_file_extent_manifest(
                self.active_branch_identifier(),
                FileIdentifier::new(inode_number),
            )
            .map_err(to_fuse_integrity)?;
        self.validate_stored_extents(&existing_extents)?;
        let overwritten_end = end.min(old_size);
        let overwritten_extents = if offset < overwritten_end && !data.is_empty() {
            slice_extents(&existing_extents, offset, overwritten_end, 0)
                .map_err(to_fuse_integrity)?
        } else {
            Vec::new()
        };
        let written_chunks = chunk_bytes(data)?;
        let written_current_extents = stored_extents_from_pending(offset, &written_chunks)?;
        let written_payload_extents = stored_extents_from_pending(0, &written_chunks)?;
        let final_extents = current_extents_after_write(
            &existing_extents,
            old_size,
            offset,
            new_size,
            &written_current_extents,
        )?;
        let mut batch = WriteBatch::default();
        self.delete_current_file_extents(&mut batch, inode_number)?;
        self.put_existing_extents(
            &mut batch,
            ExtentSet::Current {
                branch: self.active_branch_identifier(),
                file_identifier: FileIdentifier::new(inode_number),
            },
            &slice_extents(&existing_extents, 0, offset.min(old_size), 0)
                .map_err(to_fuse_integrity)?,
        )?;
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
        if end < old_size {
            self.put_existing_extents(
                &mut batch,
                ExtentSet::Current {
                    branch: self.active_branch_identifier(),
                    file_identifier: FileIdentifier::new(inode_number),
                },
                &slice_extents(&existing_extents, end, old_size, end).map_err(to_fuse_integrity)?,
            )?;
        }
        inode.size = new_size;
        inode.mtime = now;
        inode.ctime = now;
        self.put_inode(&mut batch, inode_number, &inode)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);

        let event = EventRecord::new(
            event_sequence,
            EventKind::FileWritten,
            UtcDateTime::now(),
            Some(FileIdentifier::new(inode_number)),
            inode_event_path(
                self.active_branch_identifier(),
                inode_number,
                &inode,
                &self.database,
            )
            .ok(),
            Some(offset),
            Some(data.len() as u64),
        )
        .with_payload(EventPayload::FileWrite {
            old_file_size: old_size,
            new_file_size: new_size,
            overwritten_byte_length: overwritten_end.saturating_sub(offset),
            written_byte_length: data.len() as u64,
        });
        self.commit_event_with_payloads(
            &mut batch,
            event_sequence,
            &event,
            EventPayloadExtents::FileWrite {
                overwritten_extents,
                written_extents: written_payload_extents,
            },
            Some(&final_extents),
            None,
            mutation,
        )?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)?;

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
        let snapshot = self.database.snapshot();
        let inode = self
            .inode_from_snapshot(&snapshot, inode_number)
            .map_err(to_fuse_integrity)?
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

        let entries_cf = self.directory_entries().map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        let prefix = encode_branch_parent_prefix(branch.get(), inode_number);
        let mut entry_offset = 3;
        for item in
            snapshot.iterator_cf(entries_cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item.map_err(|_| FuseError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let name = key[16..].to_vec();
            if entry_offset > offset {
                let entry = decode_directory_entry(&value).map_err(to_fuse_integrity)?;
                if !emit(DirectoryEntrySnapshot {
                    inode: entry.inode,
                    offset: entry_offset + 1,
                    kind: fuser_file_type(entry.kind),
                    name: OsString::from_vec(name),
                }) {
                    return Ok(());
                }
            }
            entry_offset += 1;
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
        let snapshot = self.database.snapshot();
        let inode = self
            .inode_from_snapshot(&snapshot, inode_number)
            .map_err(to_fuse_integrity)?
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
            let parent_inode = self
                .inode_from_snapshot(&snapshot, parent_number)
                .map_err(to_fuse_integrity)?
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

        let entries_cf = self.directory_entries().map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        let prefix = encode_branch_parent_prefix(branch.get(), inode_number);
        let mut entry_offset = 3;
        for item in
            snapshot.iterator_cf(entries_cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item.map_err(|_| FuseError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let name = key[16..].to_vec();
            if entry_offset > offset {
                let entry = decode_directory_entry(&value).map_err(to_fuse_integrity)?;
                let entry_inode = self
                    .inode_from_snapshot(&snapshot, entry.inode)
                    .map_err(to_fuse_integrity)?
                    .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
                if !emit(DirectoryEntryPlusSnapshot {
                    inode: entry.inode,
                    offset: entry_offset + 1,
                    name: OsString::from_vec(name),
                    entry: EntrySnapshot {
                        ttl: FUSE_ATTRIBUTE_TTL,
                        attr: inode_attr(entry.inode, &entry_inode),
                    },
                }) {
                    return Ok(());
                }
            }
            entry_offset += 1;
        }
        Ok(())
    }

    pub(crate) fn access(
        &self,
        inode_number: u64,
        uid: u32,
        gid: u32,
        mask: fuser::AccessFlags,
    ) -> FuseResult<()> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if uid == 0 || access_allowed(&inode, uid, gid, mask) {
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

    pub(crate) fn setxattr(
        &self,
        inode_number: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        uid: u32,
        gid: u32,
    ) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if uid != 0 && uid != inode.uid {
            self.check_inode_access(&inode, uid, gid, fuser::AccessFlags::W_OK)?;
        }
        let name_bytes = validate_extended_attribute_name(name)?;
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
        self.put_inode(&mut batch, inode_number, &inode)?;
        self.put_extended_attribute(&mut batch, inode_number, &name_bytes, value)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        mutation.put_extended_attribute(inode_number, &name_bytes, value);
        let event = EventRecord::new(
            event_sequence,
            EventKind::ExtendedAttributeSet,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, inode.kind),
            inode_event_path(
                self.active_branch_identifier(),
                inode_number,
                &inode,
                &self.database,
            )
            .ok(),
            None,
            Some(value.len() as u64),
        );
        self.commit_event(&mut batch, event_sequence, &event, mutation)?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)
    }

    pub(crate) fn getxattr(&self, inode_number: u64, name: &OsStr) -> FuseResult<Vec<u8>> {
        self.inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let name_bytes = validate_extended_attribute_name(name)?;
        self.extended_attribute(inode_number, &name_bytes)?
            .ok_or(FuseError::Errno(fuser::Errno::NO_XATTR))
    }

    pub(crate) fn listxattr(&self, inode_number: u64) -> FuseResult<ExtendedAttributeList> {
        self.inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let extended_attributes = self.extended_attributes().map_err(to_fuse_integrity)?;
        let branch = self.active_branch_identifier();
        let prefix = encode_branch_file_prefix(branch.get(), inode_number);
        let mut bytes = Vec::new();
        for item in self.database.iterator_cf(
            extended_attributes,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, _value) = item.map_err(|_| FuseError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let name = key.get(16..).ok_or(FuseError::Integrity)?;
            bytes.extend_from_slice(name);
            bytes.push(0);
        }
        Ok(ExtendedAttributeList { bytes })
    }

    pub(crate) fn removexattr(&self, inode_number: u64, name: &OsStr, uid: u32) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if uid != 0 && uid != inode.uid {
            return Err(FuseError::Errno(fuser::Errno::EACCES));
        }
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
        self.put_inode(&mut batch, inode_number, &inode)?;
        self.delete_extended_attribute(&mut batch, inode_number, &name_bytes)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        mutation.delete_extended_attribute(inode_number, &name_bytes);
        let event = EventRecord::new(
            event_sequence,
            EventKind::ExtendedAttributeRemoved,
            UtcDateTime::now(),
            file_identifier_for_kind(inode_number, inode.kind),
            inode_event_path(
                self.active_branch_identifier(),
                inode_number,
                &inode,
                &self.database,
            )
            .ok(),
            None,
            None,
        );
        self.commit_event(&mut batch, event_sequence, &event, mutation)?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)
    }

    pub(crate) fn volume_name(&self) -> Result<String, FilesystemError> {
        read_metadata_string(&self.database, METADATA_KEY_VOLUME_NAME)
    }

    pub(crate) fn bmap(&self, inode_number: u64) -> FuseResult<()> {
        self.inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        Ok(())
    }

    pub(crate) fn poll(
        &self,
        inode_number: u64,
        uid: u32,
        gid: u32,
        requested: fuser::PollEvents,
    ) -> FuseResult<fuser::PollEvents> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let mut ready = fuser::PollEvents::empty();
        if requested.intersects(read_poll_events())
            && (uid == 0 || access_allowed(&inode, uid, gid, fuser::AccessFlags::R_OK))
        {
            ready |= requested & read_poll_events();
        }
        if requested.intersects(write_poll_events())
            && (uid == 0 || access_allowed(&inode, uid, gid, fuser::AccessFlags::W_OK))
        {
            ready |= requested & write_poll_events();
        }
        Ok(ready)
    }

    pub(crate) fn fallocate(
        &self,
        inode_number: u64,
        offset: u64,
        length: u64,
        mode: i32,
        uid: u32,
        gid: u32,
    ) -> FuseResult<()> {
        if length == 0 {
            self.open_file(inode_number, uid, gid, fuser::OpenFlags(libc::O_WRONLY))?;
            return Ok(());
        }
        let supported = fallocate_keep_size() | fallocate_punch_hole() | fallocate_zero_range();
        if mode & !supported != 0
            || mode & fallocate_collapse_range() != 0
            || mode & fallocate_insert_range() != 0
            || mode & fallocate_unshare_range() != 0
        {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        if mode & fallocate_punch_hole() != 0 {
            if mode & fallocate_zero_range() != 0 {
                return Err(FuseError::Errno(fuser::Errno::EINVAL));
            }
            return self.zero_file_range(inode_number, offset, length, true, uid, gid);
        }
        if mode & fallocate_zero_range() != 0 {
            return self.zero_file_range(
                inode_number,
                offset,
                length,
                mode & fallocate_keep_size() != 0,
                uid,
                gid,
            );
        }
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        self.check_inode_access(&inode, uid, gid, fuser::AccessFlags::W_OK)?;
        let end = offset
            .checked_add(length)
            .ok_or(FuseError::Errno(fuser::Errno::EFBIG))?;
        if mode & fallocate_keep_size() != 0 || end <= inode.size {
            return Ok(());
        }
        self.setattr(
            inode_number,
            SetAttributes {
                mode: None,
                uid: None,
                gid: None,
                size: Some(end),
                atime: None,
                mtime: None,
                ctime: None,
                crtime: None,
                bkuptime: None,
                flags: None,
            },
            uid,
            gid,
        )
        .map(|_| ())
    }

    pub(crate) fn lseek(&self, inode_number: u64, offset: i64, whence: i32) -> FuseResult<i64> {
        if offset < 0 {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        let offset = offset as u64;
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        if offset >= inode.size {
            return Err(FuseError::Errno(fuser::Errno::ENXIO));
        }
        let extents = self
            .current_file_extent_manifest(
                self.active_branch_identifier(),
                FileIdentifier::new(inode_number),
            )
            .map_err(to_fuse_integrity)?;
        self.validate_stored_extents(&extents)?;
        let target = match whence {
            value if value == libc::SEEK_DATA => seek_data(&extents, offset, inode.size)?,
            value if value == libc::SEEK_HOLE => seek_hole(&extents, offset, inode.size)?,
            _ => return Err(FuseError::Errno(fuser::Errno::EINVAL)),
        };
        i64::try_from(target).map_err(|_| FuseError::Errno(fuser::Errno::EOVERFLOW))
    }

    pub(crate) fn copy_file_range(
        &self,
        source_inode_number: u64,
        source_offset: u64,
        destination_inode_number: u64,
        destination_offset: u64,
        length: u64,
        uid: u32,
        gid: u32,
    ) -> FuseResult<FuseWrite> {
        if length == 0 {
            return Ok(FuseWrite { written: 0 });
        }
        let source_inode = self
            .inode(source_inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if source_inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        self.check_inode_access(&source_inode, uid, gid, fuser::AccessFlags::R_OK)?;
        if source_offset >= source_inode.size {
            return Ok(FuseWrite { written: 0 });
        }
        let copy_length = length
            .min(source_inode.size - source_offset)
            .min(u64::from(u32::MAX));
        let bytes = self
            .read_extent_range(
                ExtentSet::Current {
                    branch: self.active_branch_identifier(),
                    file_identifier: FileIdentifier::new(source_inode_number),
                },
                source_offset,
                copy_length,
            )
            .map_err(to_fuse_integrity)?;
        if bytes.is_empty() {
            return Ok(FuseWrite { written: 0 });
        }
        self.write_file(
            destination_inode_number,
            destination_offset,
            &bytes,
            uid,
            gid,
        )
    }

    pub(crate) fn set_volume_name(&self, name: &OsStr) -> FuseResult<()> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > FUSE_MAX_NAME_LENGTH {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        let name =
            std::str::from_utf8(bytes).map_err(|_| FuseError::Errno(fuser::Errno::EINVAL))?;
        if self.volume_name().map_err(to_fuse_integrity)? == name {
            return Ok(());
        }
        let mut write_state = self.lock_write_state()?;
        let event_sequence = write_state.next_event_sequence()?;
        let mut batch = WriteBatch::default();
        let metadata = self.metadata().map_err(to_fuse_integrity)?;
        batch.put_cf(metadata, METADATA_KEY_VOLUME_NAME, name.as_bytes());
        let event = EventRecord::new(
            event_sequence,
            EventKind::VolumeRenamed,
            UtcDateTime::now(),
            None,
            Some("/".to_owned()),
            None,
            None,
        );
        self.commit_event(
            &mut batch,
            event_sequence,
            &event,
            StoredMutation::default(),
        )?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)
    }

    pub(crate) fn getxtimes(&self, inode_number: u64) -> FuseResult<ExtendedTimes> {
        let inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        Ok(ExtendedTimes {
            bkuptime: inode.bkuptime.to_system_time(),
            crtime: inode.crtime.to_system_time(),
        })
    }

    pub(crate) fn exchange(
        &self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        uid: u32,
        gid: u32,
    ) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let name_bytes = validate_name(name)?;
        let new_name_bytes = validate_name(new_name)?;
        let entry = self
            .directory_entry(parent, &name_bytes)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let new_entry = self
            .directory_entry(new_parent, &new_name_bytes)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if entry.inode == new_entry.inode {
            return Ok(());
        }
        let mut inode = self
            .inode(entry.inode)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let mut new_inode = self
            .inode(new_entry.inode)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile
            || new_inode.kind != StoredNodeKind::RegularFile
        {
            return Err(FuseError::Errno(fuser::Errno::EINVAL));
        }
        self.check_inode_access(&inode, uid, gid, fuser::AccessFlags::W_OK)?;
        self.check_inode_access(&new_inode, uid, gid, fuser::AccessFlags::W_OK)?;
        let path = child_event_path(
            self.active_branch_identifier(),
            parent,
            &name_bytes,
            &self.database,
        )?;
        let new_path = child_event_path(
            self.active_branch_identifier(),
            new_parent,
            &new_name_bytes,
            &self.database,
        )?;
        let extents = self
            .current_file_extent_manifest(
                self.active_branch_identifier(),
                FileIdentifier::new(entry.inode),
            )
            .map_err(to_fuse_integrity)?;
        let new_extents = self
            .current_file_extent_manifest(
                self.active_branch_identifier(),
                FileIdentifier::new(new_entry.inode),
            )
            .map_err(to_fuse_integrity)?;
        self.validate_stored_extents(&extents)?;
        self.validate_stored_extents(&new_extents)?;

        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        let old_size = inode.size;
        let new_old_size = new_inode.size;
        inode.size = new_old_size;
        inode.mtime = now;
        inode.ctime = now;
        new_inode.size = old_size;
        new_inode.mtime = now;
        new_inode.ctime = now;

        let mut batch = WriteBatch::default();
        self.replace_current_file_extents(&mut batch, entry.inode, &new_extents)?;
        self.replace_current_file_extents(&mut batch, new_entry.inode, &extents)?;
        self.put_inode(&mut batch, entry.inode, &inode)?;
        self.put_inode(&mut batch, new_entry.inode, &new_inode)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(entry.inode, &inode);
        mutation.put_inode(new_entry.inode, &new_inode);
        let event = EventRecord::new(
            event_sequence,
            EventKind::FileContentsExchanged,
            UtcDateTime::now(),
            Some(FileIdentifier::new(entry.inode)),
            Some(path),
            None,
            None,
        )
        .with_secondary_file(FileIdentifier::new(new_entry.inode), Some(new_path))
        .with_payload(EventPayload::FileExchange {
            primary_file_size: inode.size,
            secondary_file_size: new_inode.size,
        });
        self.commit_event_with_payloads(
            &mut batch,
            event_sequence,
            &event,
            EventPayloadExtents::None,
            Some(&new_extents),
            Some(&extents),
            mutation,
        )?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)
    }

    fn lock_write_state(&self) -> FuseResult<MutexGuard<'_, WriteState>> {
        self.write_state.lock().map_err(|_| FuseError::Database)
    }

    fn zero_file_range(
        &self,
        inode_number: u64,
        offset: u64,
        length: u64,
        keep_size: bool,
        uid: u32,
        gid: u32,
    ) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let mut inode = self
            .inode(inode_number)
            .map_err(to_fuse_integrity)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        if inode.kind != StoredNodeKind::RegularFile {
            return Err(FuseError::Errno(fuser::Errno::EISDIR));
        }
        self.check_inode_access(&inode, uid, gid, fuser::AccessFlags::W_OK)?;
        let requested_end = offset
            .checked_add(length)
            .ok_or(FuseError::Errno(fuser::Errno::EFBIG))?;
        let old_size = inode.size;
        let zero_end = if keep_size {
            requested_end.min(old_size)
        } else {
            requested_end
        };
        let new_size = if keep_size {
            old_size
        } else {
            old_size.max(requested_end)
        };
        let existing_extents = self
            .current_file_extent_manifest(
                self.active_branch_identifier(),
                FileIdentifier::new(inode_number),
            )
            .map_err(to_fuse_integrity)?;
        self.validate_stored_extents(&existing_extents)?;
        let final_extents =
            current_extents_after_zero(&existing_extents, old_size, offset, zero_end)?;
        if new_size == old_size && final_extents == existing_extents {
            return Ok(());
        }

        let event_sequence = write_state.next_event_sequence()?;
        let now = StoredTime::now();
        let mut batch = WriteBatch::default();
        self.replace_current_file_extents(&mut batch, inode_number, &final_extents)?;
        inode.size = new_size;
        inode.mtime = now;
        inode.ctime = now;
        self.put_inode(&mut batch, inode_number, &inode)?;
        let mut mutation = StoredMutation::default();
        mutation.put_inode(inode_number, &inode);
        let event = EventRecord::new(
            event_sequence,
            EventKind::FileRangeZeroed,
            UtcDateTime::now(),
            Some(FileIdentifier::new(inode_number)),
            inode_event_path(
                self.active_branch_identifier(),
                inode_number,
                &inode,
                &self.database,
            )
            .ok(),
            Some(offset),
            Some(length),
        )
        .with_payload(EventPayload::FileSizeChange {
            old_file_size: old_size,
            new_file_size: new_size,
        });
        self.commit_event_with_payloads(
            &mut batch,
            event_sequence,
            &event,
            EventPayloadExtents::None,
            Some(&final_extents),
            None,
            mutation,
        )?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)
    }

    fn commit_mutation(
        &self,
        write_state: &mut WriteState,
        batch: WriteBatch,
        event_sequence: EventSequence,
        next_inode_number: Option<u64>,
    ) -> FuseResult<()> {
        write_batch(&self.database, batch).map_err(|_| FuseError::Database)?;
        write_state.record_committed_event(event_sequence);
        if let Some(next_inode_number) = next_inode_number {
            write_state.record_committed_inode_number(next_inode_number);
        }
        Ok(())
    }

    fn remove_directory_entry(
        &self,
        parent: u64,
        name: &OsStr,
        directory: bool,
        uid: u32,
        gid: u32,
    ) -> FuseResult<()> {
        let mut write_state = self.lock_write_state()?;
        let name_bytes = validate_name(name)?;
        let mut parent_inode =
            self.require_directory_with_access(parent, uid, gid, write_directory_access_mask())?;
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
        let path = child_event_path(
            self.active_branch_identifier(),
            parent,
            &name_bytes,
            &self.database,
        )?;
        let mut batch = WriteBatch::default();
        self.remove_entry_in_batch(
            &mut batch,
            DirectoryEntryLocation {
                parent,
                name: &name_bytes,
            },
            entry.clone(),
            inode,
            &mut parent_inode,
            now,
        )?;
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
        self.commit_event(&mut batch, event_sequence, &event, mutation)?;
        self.commit_mutation(&mut write_state, batch, event_sequence, None)
    }

    fn remove_entry_in_batch(
        &self,
        batch: &mut WriteBatch,
        location: DirectoryEntryLocation<'_>,
        entry: StoredDirectoryEntry,
        mut inode: StoredInode,
        parent_inode: &mut StoredInode,
        now: StoredTime,
    ) -> FuseResult<()> {
        self.delete_directory_entry(batch, location.parent, location.name)?;

        if inode.kind == StoredNodeKind::Directory {
            self.delete_inode(batch, entry.inode)?;
            self.delete_current_file_extents(batch, entry.inode)?;
            parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
            self.touch_loaded_directory(batch, location.parent, parent_inode, now)?;
        } else {
            inode.nlink = inode.nlink.saturating_sub(1);
            inode.ctime = now;
            if inode.nlink == 0 {
                self.delete_inode(batch, entry.inode)?;
                self.delete_current_file_extents(batch, entry.inode)?;
            } else {
                self.put_inode(batch, entry.inode, &inode)?;
            }
            self.touch_loaded_directory(batch, location.parent, parent_inode, now)?;
        }
        Ok(())
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

    fn require_directory_with_access(
        &self,
        inode_number: u64,
        uid: u32,
        gid: u32,
        mask: fuser::AccessFlags,
    ) -> FuseResult<StoredInode> {
        let inode = self.require_directory(inode_number)?;
        self.check_inode_access(&inode, uid, gid, mask)?;
        Ok(inode)
    }

    fn check_inode_access(
        &self,
        inode: &StoredInode,
        uid: u32,
        gid: u32,
        mask: fuser::AccessFlags,
    ) -> FuseResult<()> {
        if uid == 0 || access_allowed(inode, uid, gid, mask) {
            Ok(())
        } else {
            Err(FuseError::Errno(fuser::Errno::EACCES))
        }
    }

    fn directory_is_empty(&self, inode_number: u64) -> FuseResult<bool> {
        let entries = self.directory_entries().map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        let prefix = encode_branch_parent_prefix(branch.get(), inode_number);
        let mut iterator = self
            .database
            .iterator_cf(entries, IteratorMode::From(&prefix, Direction::Forward));
        match iterator.next() {
            Some(Ok((key, _value))) if key.starts_with(&prefix) => Ok(false),
            Some(Ok(_)) | None => Ok(true),
            Some(Err(_)) => Err(FuseError::Database),
        }
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
        let inodes = self.inodes()?;
        let branch = self.active_branch_identifier();
        self.database
            .get_pinned_cf(inodes, encode_branch_inode_key(branch.get(), inode_number))
            .map_err(|_| FilesystemError::Database)?
            .map(|value| decode_inode(&value))
            .transpose()
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
        let snapshots = self.file_snapshot_extents()?;
        let prefix = encode_snapshot_file_prefix(branch.get(), file_identifier.get());
        let mut iterator = self.database.raw_iterator_cf(snapshots);
        iterator.seek_for_prev(encode_snapshot_extent_key(
            branch.get(),
            file_identifier.get(),
            position.ordinal(),
            u64::MAX,
        ));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let (ordinal, offset) = decode_snapshot_ordinal_and_offset(key)?;
            if offset == u64::MAX {
                let metadata =
                    decode_snapshot_metadata(iterator.value().ok_or(FilesystemError::Database)?)?;
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
        let snapshots = self.file_snapshot_extents()?;
        let prefix = encode_snapshot_file_prefix(branch.get(), file_identifier.get());
        let mut iterator = self.database.raw_iterator_cf(snapshots);
        iterator.seek_for_prev(encode_snapshot_extent_key(
            branch.get(),
            file_identifier.get(),
            u64::MAX,
            u64::MAX,
        ));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let (ordinal, offset) = decode_snapshot_ordinal_and_offset(key)?;
            if offset == u64::MAX {
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
            }
            iterator.prev();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;
        Ok(None)
    }

    fn inode_from_snapshot(
        &self,
        snapshot: &rocksdb::Snapshot<'_>,
        inode_number: u64,
    ) -> Result<Option<StoredInode>, FilesystemError> {
        let inodes = self.inodes()?;
        let branch = self.active_branch_identifier();
        snapshot
            .get_pinned_cf(inodes, encode_branch_inode_key(branch.get(), inode_number))
            .map_err(|_| FilesystemError::Database)?
            .map(|value| decode_inode(&value))
            .transpose()
    }

    fn directory_entry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<StoredDirectoryEntry>, FilesystemError> {
        let entries = self.directory_entries()?;
        let branch = self.active_branch_identifier();
        self.database
            .get_pinned_cf(
                entries,
                encode_directory_entry_key(branch.get(), parent, name),
            )
            .map_err(|_| FilesystemError::Database)?
            .map(|value| decode_directory_entry(&value))
            .transpose()
    }

    fn extended_attribute(&self, inode_number: u64, name: &[u8]) -> FuseResult<Option<Vec<u8>>> {
        let extended_attributes = self.extended_attributes().map_err(to_fuse_integrity)?;
        let branch = self.active_branch_identifier();
        self.database
            .get_pinned_cf(
                extended_attributes,
                encode_extended_attribute_key(branch.get(), inode_number, name),
            )
            .map_err(|_| FuseError::Database)
            .map(|value| value.map(|value| value.to_vec()))
    }

    fn put_inode(
        &self,
        batch: &mut WriteBatch,
        inode_number: u64,
        inode: &StoredInode,
    ) -> FuseResult<()> {
        let inodes = self.inodes().map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        batch.put_cf(
            inodes,
            encode_branch_inode_key(branch.get(), inode_number),
            encode_inode(inode).map_err(|_| FuseError::Database)?,
        );
        Ok(())
    }

    fn delete_inode(&self, batch: &mut WriteBatch, inode_number: u64) -> FuseResult<()> {
        let inodes = self.inodes().map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        batch.delete_cf(inodes, encode_branch_inode_key(branch.get(), inode_number));
        self.delete_extended_attributes_for_inode(batch, inode_number)?;
        Ok(())
    }

    fn delete_extended_attributes_for_inode(
        &self,
        batch: &mut WriteBatch,
        inode_number: u64,
    ) -> FuseResult<()> {
        let extended_attributes = self
            .extended_attributes()
            .map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        let prefix = encode_branch_file_prefix(branch.get(), inode_number);
        for item in self.database.iterator_cf(
            extended_attributes,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, _value) = item.map_err(|_| FuseError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete_cf(extended_attributes, key);
        }
        Ok(())
    }

    fn put_directory_entry(
        &self,
        batch: &mut WriteBatch,
        parent: u64,
        name: &[u8],
        entry: StoredDirectoryEntry,
    ) -> FuseResult<()> {
        let entries = self.directory_entries().map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        batch.put_cf(
            entries,
            encode_directory_entry_key(branch.get(), parent, name),
            encode_directory_entry(&entry).map_err(|_| FuseError::Database)?,
        );
        Ok(())
    }

    fn delete_directory_entry(
        &self,
        batch: &mut WriteBatch,
        parent: u64,
        name: &[u8],
    ) -> FuseResult<()> {
        let entries = self.directory_entries().map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        batch.delete_cf(
            entries,
            encode_directory_entry_key(branch.get(), parent, name),
        );
        Ok(())
    }

    fn put_extended_attribute(
        &self,
        batch: &mut WriteBatch,
        inode_number: u64,
        name: &[u8],
        value: &[u8],
    ) -> FuseResult<()> {
        let extended_attributes = self.extended_attributes().map_err(to_fuse_integrity)?;
        let branch = self.active_branch_identifier();
        batch.put_cf(
            extended_attributes,
            encode_extended_attribute_key(branch.get(), inode_number, name),
            value,
        );
        Ok(())
    }

    fn delete_extended_attribute(
        &self,
        batch: &mut WriteBatch,
        inode_number: u64,
        name: &[u8],
    ) -> FuseResult<()> {
        let extended_attributes = self.extended_attributes().map_err(to_fuse_integrity)?;
        let branch = self.active_branch_identifier();
        batch.delete_cf(
            extended_attributes,
            encode_extended_attribute_key(branch.get(), inode_number, name),
        );
        Ok(())
    }

    fn touch_loaded_directory(
        &self,
        batch: &mut WriteBatch,
        inode_number: u64,
        inode: &mut StoredInode,
        timestamp: StoredTime,
    ) -> FuseResult<()> {
        inode.mtime = timestamp;
        inode.ctime = timestamp;
        self.put_inode(batch, inode_number, inode)
    }

    fn delete_current_file_extents(
        &self,
        batch: &mut WriteBatch,
        inode_number: u64,
    ) -> FuseResult<()> {
        let extents = self
            .current_file_extents()
            .map_err(|_| FuseError::Integrity)?;
        let branch = self.active_branch_identifier();
        let prefix = encode_current_file_prefix(branch.get(), inode_number);
        for item in self
            .database
            .iterator_cf(extents, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _value) = item.map_err(|_| FuseError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete_cf(extents, key);
        }
        Ok(())
    }

    fn replace_current_file_extents(
        &self,
        batch: &mut WriteBatch,
        inode_number: u64,
        extents: &[StoredExtent],
    ) -> FuseResult<()> {
        self.delete_current_file_extents(batch, inode_number)?;
        self.put_existing_extents(
            batch,
            ExtentSet::Current {
                branch: self.active_branch_identifier(),
                file_identifier: FileIdentifier::new(inode_number),
            },
            extents,
        )
    }

    fn current_file_extent_manifest(
        &self,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
    ) -> Result<Vec<StoredExtent>, FilesystemError> {
        let extents = self.current_file_extents()?;
        let prefix = encode_current_file_prefix(branch.get(), file_identifier.get());
        let mut manifest = Vec::new();
        for item in self
            .database
            .iterator_cf(extents, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item.map_err(|_| FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let extent = decode_extent(&value)?;
            if decode_extent_offset_from_current_key(&key)? != extent.logical_offset {
                return Err(FilesystemError::Integrity);
            }
            manifest.push(extent);
        }
        Ok(manifest)
    }

    fn materialize_branch_state_at_position(
        &self,
        position: BranchPosition,
    ) -> Result<MaterializedBranchState, FilesystemError> {
        let mut visited = BTreeSet::new();
        self.materialize_branch_state_at_position_inner(position, &mut visited)
    }

    fn materialize_branch_state_at_position_inner(
        &self,
        position: BranchPosition,
        visited: &mut BTreeSet<u64>,
    ) -> Result<MaterializedBranchState, FilesystemError> {
        let branch_identifier = position.branch_identifier();
        if !visited.insert(branch_identifier.get()) {
            return Err(FilesystemError::Integrity);
        }
        let branch = self.stored_branch(branch_identifier)?;
        if position.ordinal() > branch.head_ordinal {
            return Err(FilesystemError::Integrity);
        }

        let (mut state, start_ordinal) = match (branch.fork_branch_identifier, branch.fork_ordinal)
        {
            (Some(fork_branch), Some(fork_ordinal)) => {
                if position.ordinal() < fork_ordinal {
                    return Err(FilesystemError::Integrity);
                }
                let fork_position =
                    BranchPosition::new(BranchIdentifier::new(fork_branch), fork_ordinal);
                (
                    self.materialize_branch_state_at_position_inner(fork_position, visited)?,
                    fork_ordinal
                        .checked_add(1)
                        .ok_or(FilesystemError::Integrity)?,
                )
            }
            (None, None) => (MaterializedBranchState::default(), 0),
            _ => return Err(FilesystemError::Integrity),
        };
        self.apply_branch_events_to_state(
            &mut state,
            branch_identifier,
            start_ordinal,
            position.ordinal(),
        )?;
        visited.remove(&branch_identifier.get());
        Ok(state)
    }

    fn apply_branch_events_to_state(
        &self,
        state: &mut MaterializedBranchState,
        branch: BranchIdentifier,
        start_ordinal: u64,
        end_ordinal: u64,
    ) -> Result<(), FilesystemError> {
        if start_ordinal > end_ordinal {
            return Ok(());
        }
        let branch_events = self.branch_events()?;
        let prefix = encode_u64(branch.get());
        let mut iterator = self.database.raw_iterator_cf(branch_events);
        iterator.seek(encode_branch_position_key(branch.get(), start_ordinal));
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if key.len() != 16 {
                return Err(FilesystemError::Integrity);
            }
            let ordinal = decode_u64(&key[8..])?;
            if ordinal > end_ordinal {
                break;
            }
            let sequence = decode_u64(iterator.value().ok_or(FilesystemError::Database)?)
                .map(EventSequence::new)?;
            let event = self
                .stored_event(sequence)?
                .ok_or(FilesystemError::Integrity)?;
            validate_branch_event_record(&event.record, branch, ordinal)?;
            state.apply(&event.mutation);
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)
    }

    fn copy_branch_state_at_position(
        &self,
        batch: &mut WriteBatch,
        source_position: BranchPosition,
        target: BranchIdentifier,
        fork_sequence: EventSequence,
    ) -> Result<(), FilesystemError> {
        let source = source_position.branch_identifier();
        let inodes = self.inodes()?;
        let directory_entries = self.directory_entries()?;
        let state = self.materialize_branch_state_at_position(source_position)?;
        for ((parent, name), entry) in &state.directory_entries {
            batch.put_cf(
                directory_entries,
                encode_directory_entry_key(target.get(), *parent, name),
                encode_directory_entry(entry)?,
            );
        }
        let extended_attributes = self.extended_attributes()?;
        for ((inode_number, name), value) in &state.extended_attributes {
            batch.put_cf(
                extended_attributes,
                encode_extended_attribute_key(target.get(), *inode_number, name),
                value,
            );
        }
        for (inode_number, inode) in &state.inodes {
            let mut inode = inode.clone();
            if inode.kind == StoredNodeKind::RegularFile {
                let file_identifier = FileIdentifier::new(*inode_number);
                if let Some(snapshot) = self.file_snapshot_metadata_at_or_before(
                    source,
                    file_identifier,
                    source_position,
                )? {
                    inode.size = snapshot.file_size();
                    self.copy_snapshot_extents_to_branch(
                        batch,
                        snapshot.branch_position(),
                        target,
                        file_identifier,
                        source_position.ordinal(),
                    )?;
                    self.put_branch_file_snapshot_metadata(
                        batch,
                        target,
                        file_identifier,
                        source_position.ordinal(),
                        snapshot.file_size(),
                        fork_sequence,
                    )?;
                } else {
                    return Err(FilesystemError::Integrity);
                }
            }
            batch.put_cf(
                inodes,
                encode_branch_inode_key(target.get(), *inode_number),
                encode_inode(&inode)?,
            );
        }
        Ok(())
    }

    fn copy_snapshot_extents_to_branch(
        &self,
        batch: &mut WriteBatch,
        source_position: BranchPosition,
        target: BranchIdentifier,
        file_identifier: FileIdentifier,
        target_ordinal: u64,
    ) -> Result<(), FilesystemError> {
        let snapshots = self.file_snapshot_extents()?;
        let current_extents = self.current_file_extents()?;
        let prefix = encode_snapshot_extent_prefix(
            source_position.branch_identifier().get(),
            file_identifier.get(),
            source_position.ordinal(),
        );
        for item in self
            .database
            .iterator_cf(snapshots, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item.map_err(|_| FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let (_ordinal, offset) = decode_snapshot_ordinal_and_offset(&key)?;
            if offset == u64::MAX {
                break;
            }
            batch.put_cf(
                current_extents,
                encode_current_file_extent_key(target.get(), file_identifier.get(), offset),
                value.as_ref(),
            );
            batch.put_cf(
                snapshots,
                encode_snapshot_extent_key(
                    target.get(),
                    file_identifier.get(),
                    target_ordinal,
                    offset,
                ),
                value.as_ref(),
            );
        }
        Ok(())
    }

    fn put_branch_file_snapshot_metadata(
        &self,
        batch: &mut WriteBatch,
        branch: BranchIdentifier,
        file_identifier: FileIdentifier,
        ordinal: u64,
        file_size: u64,
        sequence: EventSequence,
    ) -> Result<(), FilesystemError> {
        let metadata = StoredSnapshotMetadata {
            file_size,
            sequence: sequence.get(),
        };
        batch.put_cf(
            self.file_snapshot_extents()?,
            encode_snapshot_extent_key(branch.get(), file_identifier.get(), ordinal, u64::MAX),
            encode_snapshot_metadata(&metadata)?,
        );
        Ok(())
    }

    fn commit_event(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        event: &EventRecord,
        mutation: StoredMutation,
    ) -> FuseResult<()> {
        self.commit_event_with_payloads(
            batch,
            event_sequence,
            event,
            EventPayloadExtents::None,
            None,
            None,
            mutation,
        )
    }

    fn commit_event_with_payloads(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        event: &EventRecord,
        payload_extents: EventPayloadExtents,
        snapshot_extents: Option<&[StoredExtent]>,
        secondary_snapshot_extents: Option<&[StoredExtent]>,
        mutation: StoredMutation,
    ) -> FuseResult<()> {
        let events = self.events().map_err(|_| FuseError::Integrity)?;
        let metadata = self.metadata().map_err(|_| FuseError::Integrity)?;
        let branches = self.branches().map_err(|_| FuseError::Integrity)?;
        let branch_identifier = self.active_branch_identifier();
        let mut branch = self
            .stored_branch(branch_identifier)
            .map_err(|_| FuseError::Integrity)?;
        let first_parent = EventSequence::new(branch.head_sequence);
        let branch_position = BranchPosition::new(branch_identifier, branch.head_ordinal + 1);
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
        batch.put_cf(
            events,
            event_key,
            encode_stored_event(&StoredEvent {
                record: event.clone(),
                mutation,
            })
            .map_err(|_| FuseError::Database)?,
        );
        batch.put_cf(
            branches,
            encode_u64(branch_identifier.get()),
            encode_branch(&branch).map_err(|_| FuseError::Database)?,
        );
        self.put_event_payloads(batch, event_sequence, payload_extents)?;
        self.put_file_event_index(batch, event_sequence, &event)?;
        self.put_branch_event_index(batch, event_sequence, branch_position)?;
        self.put_branch_file_event_index(batch, event_sequence, &event)?;
        self.put_file_snapshot_for_event(
            batch,
            event_sequence,
            &event,
            snapshot_extents,
            secondary_snapshot_extents,
        )?;
        batch.put_cf(
            metadata,
            METADATA_KEY_LAST_COMMITTED_EVENT_SEQUENCE,
            encode_u64(event_sequence.get()),
        );
        Ok(())
    }

    fn put_event_payloads(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        payload_extents: EventPayloadExtents,
    ) -> FuseResult<()> {
        match payload_extents {
            EventPayloadExtents::None => Ok(()),
            EventPayloadExtents::FileWrite {
                overwritten_extents,
                written_extents,
            } => {
                self.put_existing_extents(
                    batch,
                    ExtentSet::EventPayload {
                        sequence: event_sequence,
                        part: EVENT_PAYLOAD_PART_OVERWRITTEN,
                    },
                    &overwritten_extents,
                )?;
                self.put_existing_extents(
                    batch,
                    ExtentSet::EventPayload {
                        sequence: event_sequence,
                        part: EVENT_PAYLOAD_PART_WRITTEN,
                    },
                    &written_extents,
                )?;
                Ok(())
            }
            EventPayloadExtents::FileTruncate { removed_extents } => {
                self.put_existing_extents(
                    batch,
                    ExtentSet::EventPayload {
                        sequence: event_sequence,
                        part: EVENT_PAYLOAD_PART_REMOVED,
                    },
                    &removed_extents,
                )?;
                Ok(())
            }
        }
    }

    fn put_pending_extents(
        &self,
        batch: &mut WriteBatch,
        extent_set: ExtentSet,
        bytes: &[u8],
        extents: &[StoredExtent],
        pending: &[PendingExtent],
    ) -> FuseResult<()> {
        if extents.len() != pending.len() {
            return Err(FuseError::Integrity);
        }
        for (stored, pending) in extents.iter().zip(pending) {
            self.put_content_chunk(
                batch,
                &stored.chunk_identifier,
                &bytes[pending.byte_range()],
            )?;
            self.put_stored_extent(batch, extent_set, stored)?;
        }
        Ok(())
    }

    fn put_existing_extents(
        &self,
        batch: &mut WriteBatch,
        extent_set: ExtentSet,
        extents: &[StoredExtent],
    ) -> FuseResult<()> {
        for extent in extents {
            self.put_stored_extent(batch, extent_set, extent)?;
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

    fn put_content_chunk(
        &self,
        batch: &mut WriteBatch,
        chunk_identifier: &[u8],
        bytes: &[u8],
    ) -> FuseResult<()> {
        validate_content_chunk(chunk_identifier, bytes).map_err(to_fuse_integrity)?;
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

    fn put_stored_extent(
        &self,
        batch: &mut WriteBatch,
        extent_set: ExtentSet,
        stored: &StoredExtent,
    ) -> FuseResult<()> {
        match extent_set {
            ExtentSet::Current {
                branch,
                file_identifier,
            } => batch.put_cf(
                self.current_file_extents()
                    .map_err(|_| FuseError::Integrity)?,
                encode_current_file_extent_key(
                    branch.get(),
                    file_identifier.get(),
                    stored.logical_offset,
                ),
                encode_extent(stored).map_err(|_| FuseError::Database)?,
            ),
            ExtentSet::Snapshot {
                branch,
                file_identifier,
                ordinal,
            } => batch.put_cf(
                self.file_snapshot_extents()
                    .map_err(|_| FuseError::Integrity)?,
                encode_snapshot_extent_key(
                    branch.get(),
                    file_identifier.get(),
                    ordinal,
                    stored.logical_offset,
                ),
                encode_extent(stored).map_err(|_| FuseError::Database)?,
            ),
            ExtentSet::EventPayload { sequence, part } => batch.put_cf(
                self.event_payload_extents()
                    .map_err(|_| FuseError::Integrity)?,
                encode_event_payload_extent_key(sequence.get(), part, stored.logical_offset),
                encode_extent(stored).map_err(|_| FuseError::Database)?,
            ),
        }
        Ok(())
    }

    fn read_extent_range(
        &self,
        extent_set: ExtentSet,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let length = existing_byte_length_for_storage(length)?;
        let mut bytes = vec![0; length];
        let end = offset
            .checked_add(length as u64)
            .ok_or(FilesystemError::Integrity)?;
        let (cf, prefix, start_key) = self.extent_scan(extent_set, offset)?;
        let snapshot = self.database.snapshot();
        let mut iterator = snapshot.raw_iterator_cf(cf);

        iterator.seek_for_prev(&start_key);
        if iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if key.starts_with(&prefix) && !is_snapshot_metadata_key(key)? {
                let extent = decode_extent(iterator.value().ok_or(FilesystemError::Database)?)?;
                let extent_end = extent
                    .logical_offset
                    .checked_add(extent.length)
                    .ok_or(FilesystemError::Integrity)?;
                if extent_end <= offset {
                    iterator.next();
                }
            } else {
                iterator.seek(start_key);
            }
        } else {
            iterator.seek(start_key);
        }
        while iterator.valid() {
            let key = iterator.key().ok_or(FilesystemError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if is_snapshot_metadata_key(key)? {
                break;
            }
            let value = iterator.value().ok_or(FilesystemError::Database)?;
            let extent = decode_extent(value)?;
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
            iterator.next();
        }
        iterator.status().map_err(|_| FilesystemError::Database)?;
        Ok(bytes)
    }

    fn extent_scan(
        &self,
        extent_set: ExtentSet,
        offset: u64,
    ) -> Result<(&ColumnFamily, Vec<u8>, Vec<u8>), FilesystemError> {
        match extent_set {
            ExtentSet::Current {
                branch,
                file_identifier,
            } => {
                let prefix = encode_current_file_prefix(branch.get(), file_identifier.get());
                let start_key =
                    encode_current_file_extent_key(branch.get(), file_identifier.get(), offset)
                        .to_vec();
                Ok((self.current_file_extents()?, prefix, start_key))
            }
            ExtentSet::Snapshot {
                branch,
                file_identifier,
                ordinal,
            } => {
                let prefix =
                    encode_snapshot_extent_prefix(branch.get(), file_identifier.get(), ordinal);
                let start_key = encode_snapshot_extent_key(
                    branch.get(),
                    file_identifier.get(),
                    ordinal,
                    offset,
                )
                .to_vec();
                Ok((self.file_snapshot_extents()?, prefix, start_key))
            }
            ExtentSet::EventPayload { sequence, part } => {
                let prefix = encode_event_payload_extent_prefix(sequence.get(), part);
                let start_key =
                    encode_event_payload_extent_key(sequence.get(), part, offset).to_vec();
                Ok((self.event_payload_extents()?, prefix, start_key))
            }
        }
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
        snapshot_extents: Option<&[StoredExtent]>,
        secondary_snapshot_extents: Option<&[StoredExtent]>,
    ) -> FuseResult<()> {
        let Some(branch_position) = event.branch_position() else {
            return Err(FuseError::Integrity);
        };
        if !matches!(
            event.kind(),
            EventKind::NodeCreated
                | EventKind::FileCreated
                | EventKind::FileWritten
                | EventKind::FileTruncated
                | EventKind::FileRangeZeroed
                | EventKind::FileContentsExchanged
        ) {
            return Ok(());
        };
        if let Some(file_identifier) = event.file_identifier() {
            self.put_file_snapshot_for_identifier(
                batch,
                event_sequence,
                event,
                file_identifier,
                branch_position,
                snapshot_extents,
            )?;
        }
        if let Some(file_identifier) = event.secondary_file_identifier() {
            self.put_file_snapshot_for_identifier(
                batch,
                event_sequence,
                event,
                file_identifier,
                branch_position,
                secondary_snapshot_extents,
            )?;
        }
        Ok(())
    }

    fn put_file_snapshot_for_identifier(
        &self,
        batch: &mut WriteBatch,
        event_sequence: EventSequence,
        event: &EventRecord,
        file_identifier: FileIdentifier,
        branch_position: BranchPosition,
        snapshot_extents: Option<&[StoredExtent]>,
    ) -> FuseResult<()> {
        let file_size = event_snapshot_file_size(event, file_identifier).unwrap_or(0);
        if let Some(extents) = snapshot_extents {
            self.put_existing_extents(
                batch,
                ExtentSet::Snapshot {
                    branch: branch_position.branch_identifier(),
                    file_identifier,
                    ordinal: branch_position.ordinal(),
                },
                extents,
            )?;
        } else {
            self.copy_current_extents_to_snapshot(batch, file_identifier, branch_position)?;
        }
        let metadata = StoredSnapshotMetadata {
            file_size,
            sequence: event_sequence.get(),
        };
        batch.put_cf(
            self.file_snapshot_extents()
                .map_err(|_| FuseError::Integrity)?,
            encode_snapshot_extent_key(
                branch_position.branch_identifier().get(),
                file_identifier.get(),
                branch_position.ordinal(),
                u64::MAX,
            ),
            encode_snapshot_metadata(&metadata).map_err(|_| FuseError::Database)?,
        );
        Ok(())
    }

    fn copy_current_extents_to_snapshot(
        &self,
        batch: &mut WriteBatch,
        file_identifier: FileIdentifier,
        branch_position: BranchPosition,
    ) -> FuseResult<()> {
        let current_extents = self
            .current_file_extents()
            .map_err(|_| FuseError::Integrity)?;
        let snapshot_extents = self
            .file_snapshot_extents()
            .map_err(|_| FuseError::Integrity)?;
        let prefix = encode_current_file_prefix(
            branch_position.branch_identifier().get(),
            file_identifier.get(),
        );
        for item in self.database.iterator_cf(
            current_extents,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, value) = item.map_err(|_| FuseError::Database)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let offset = decode_extent_offset_from_current_key(&key).map_err(to_fuse_integrity)?;
            batch.put_cf(
                snapshot_extents,
                encode_snapshot_extent_key(
                    branch_position.branch_identifier().get(),
                    file_identifier.get(),
                    branch_position.ordinal(),
                    offset,
                ),
                value,
            );
        }
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

    fn inodes(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_INODES)
            .ok_or(FilesystemError::Integrity)
    }

    fn directory_entries(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_DIRECTORY_ENTRIES)
            .ok_or(FilesystemError::Integrity)
    }

    fn extended_attributes(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_EXTENDED_ATTRIBUTES)
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

    fn current_file_extents(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_CURRENT_FILE_EXTENTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn file_snapshot_extents(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_FILE_SNAPSHOT_EXTENTS)
            .ok_or(FilesystemError::Integrity)
    }

    fn event_payload_extents(&self) -> Result<&ColumnFamily, FilesystemError> {
        self.database
            .cf_handle(COLUMN_FAMILY_EVENT_PAYLOAD_EXTENTS)
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
        COLUMN_FAMILY_INODES
            | COLUMN_FAMILY_DIRECTORY_ENTRIES
            | COLUMN_FAMILY_EXTENDED_ATTRIBUTES
            | COLUMN_FAMILY_FILE_EVENTS
            | COLUMN_FAMILY_BRANCH_EVENTS
            | COLUMN_FAMILY_BRANCH_FILE_EVENTS
            | COLUMN_FAMILY_CURRENT_FILE_EXTENTS
            | COLUMN_FAMILY_FILE_SNAPSHOT_EXTENTS
            | COLUMN_FAMILY_EVENT_PAYLOAD_EXTENTS
    );
    apply_rocksdb_performance_options(&mut options, block_cache, uses_fixed_prefix);
    match name {
        COLUMN_FAMILY_INODES | COLUMN_FAMILY_FILE_EVENTS | COLUMN_FAMILY_BRANCH_EVENTS => {
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
        }
        COLUMN_FAMILY_DIRECTORY_ENTRIES
        | COLUMN_FAMILY_EXTENDED_ATTRIBUTES
        | COLUMN_FAMILY_BRANCH_FILE_EVENTS
        | COLUMN_FAMILY_CURRENT_FILE_EXTENTS
        | COLUMN_FAMILY_FILE_SNAPSHOT_EXTENTS => {
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(16));
        }
        COLUMN_FAMILY_EVENT_PAYLOAD_EXTENTS => {
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
    let inodes = database
        .cf_handle(COLUMN_FAMILY_INODES)
        .ok_or(FilesystemError::Integrity)?;
    let _extended_attributes = database
        .cf_handle(COLUMN_FAMILY_EXTENDED_ATTRIBUTES)
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
    let main_branch = StoredBranch {
        identifier: BRANCH_IDENTIFIER_INITIAL.get(),
        name: BRANCH_NAME_INITIAL.to_owned(),
        status: BranchStatus::Open,
        head_sequence: EVENT_SEQUENCE_INITIAL.get(),
        head_ordinal: 0,
        fork_branch_identifier: None,
        fork_ordinal: None,
    };
    let now = StoredTime::now();
    let root = StoredInode {
        kind: StoredNodeKind::Directory,
        mode: 0o755,
        uid: current_uid(),
        gid: current_gid(),
        size: 0,
        nlink: 2,
        rdev: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        bkuptime: now,
        flags: 0,
        parent: INODE_ROOT,
        name: Vec::new(),
        symlink_target: None,
    };

    let mut batch = WriteBatch::default();
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
        inodes,
        encode_branch_inode_key(BRANCH_IDENTIFIER_INITIAL.get(), INODE_ROOT),
        encode_inode(&root)?,
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

fn validate_required_column_families(
    existing_column_families: &BTreeSet<String>,
) -> Result<(), FilesystemError> {
    for name in COLUMN_FAMILY_REQUIRED {
        if !existing_column_families.contains(*name) {
            return Err(FilesystemError::Integrity);
        }
    }
    Ok(())
}

fn validate_existing_database(database: &DB) -> Result<(), FilesystemError> {
    let schema_version = read_metadata_u64(database, METADATA_KEY_STORAGE_SCHEMA_VERSION)?;
    if schema_version > STORAGE_SCHEMA_VERSION_CURRENT {
        return Err(FilesystemError::Integrity);
    }
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
    let inodes = database
        .cf_handle(COLUMN_FAMILY_INODES)
        .ok_or(FilesystemError::Integrity)?;
    let root = database
        .get_pinned_cf(
            inodes,
            encode_branch_inode_key(active_branch_identifier, INODE_ROOT),
        )
        .map_err(|_| FilesystemError::Database)?
        .ok_or(FilesystemError::Integrity)?;
    let root = decode_inode(&root)?;
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
    let inodes = database
        .cf_handle(COLUMN_FAMILY_INODES)
        .ok_or(FilesystemError::Integrity)?;
    let mut maximum = INODE_ROOT;
    let mut iterator = database.raw_iterator_cf(inodes);
    iterator.seek_to_first();
    while iterator.valid() {
        let key = iterator.key().ok_or(FilesystemError::Database)?;
        if key.len() != 16 {
            return Err(FilesystemError::Integrity);
        }
        maximum = maximum.max(decode_u64(&key[8..])?);
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
    let mut extents =
        slice_extents(existing, 0, offset.min(old_size), 0).map_err(to_fuse_integrity)?;
    extents.extend_from_slice(written_extents);
    if end < old_size {
        extents.extend(slice_extents(existing, end, old_size, end).map_err(to_fuse_integrity)?);
    }
    extents.retain(|extent| extent.logical_offset < new_size && extent.length != 0);
    Ok(extents)
}

fn current_extents_after_truncate(
    existing: &[StoredExtent],
    new_size: u64,
) -> FuseResult<Vec<StoredExtent>> {
    slice_extents(existing, 0, new_size, 0).map_err(to_fuse_integrity)
}

fn current_extents_after_zero(
    existing: &[StoredExtent],
    old_size: u64,
    offset: u64,
    zero_end: u64,
) -> FuseResult<Vec<StoredExtent>> {
    if offset >= zero_end {
        return Ok(existing.to_vec());
    }
    let mut extents =
        slice_extents(existing, 0, offset.min(old_size), 0).map_err(to_fuse_integrity)?;
    if zero_end < old_size {
        extents.extend(
            slice_extents(existing, zero_end, old_size, zero_end).map_err(to_fuse_integrity)?,
        );
    }
    Ok(extents)
}

fn seek_data(extents: &[StoredExtent], offset: u64, file_size: u64) -> FuseResult<u64> {
    for extent in extents {
        let extent_end = extent
            .logical_offset
            .checked_add(extent.length)
            .ok_or(FuseError::Integrity)?;
        if offset < extent_end && extent.logical_offset < file_size {
            return Ok(offset.max(extent.logical_offset));
        }
    }
    Err(FuseError::Errno(fuser::Errno::ENXIO))
}

fn seek_hole(extents: &[StoredExtent], offset: u64, file_size: u64) -> FuseResult<u64> {
    let mut current = offset;
    for extent in extents {
        if current < extent.logical_offset {
            return Ok(current);
        }
        let extent_end = extent
            .logical_offset
            .checked_add(extent.length)
            .ok_or(FuseError::Integrity)?;
        if current < extent_end {
            current = extent_end.min(file_size);
        }
    }
    Ok(current.min(file_size))
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

fn child_event_path(
    branch: BranchIdentifier,
    parent: u64,
    name: &[u8],
    database: &DB,
) -> FuseResult<String> {
    let mut bytes = inode_path_bytes(branch, parent, database)?;
    if bytes.len() > 1 {
        bytes.push(b'/');
    }
    bytes.extend_from_slice(name);
    String::from_utf8(bytes).map_err(|_| FuseError::Errno(fuser::Errno::EINVAL))
}

fn inode_event_path(
    branch: BranchIdentifier,
    inode_number: u64,
    inode: &StoredInode,
    database: &DB,
) -> FuseResult<String> {
    if inode_number == INODE_ROOT {
        return Ok("/".to_owned());
    }
    child_event_path(branch, inode.parent, &inode.name, database)
}

fn inode_path_bytes(
    branch: BranchIdentifier,
    inode_number: u64,
    database: &DB,
) -> FuseResult<Vec<u8>> {
    if inode_number == INODE_ROOT {
        return Ok(vec![b'/']);
    }
    let inodes = database
        .cf_handle(COLUMN_FAMILY_INODES)
        .ok_or(FuseError::Integrity)?;
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
        let inode_value = database
            .get_pinned_cf(inodes, encode_branch_inode_key(branch.get(), current))
            .map_err(|_| FuseError::Database)?
            .ok_or(FuseError::Errno(fuser::Errno::ENOENT))?;
        let inode = decode_inode(&inode_value).map_err(to_fuse_integrity)?;
        components.push(inode.name);
        current = inode.parent;
    }
    Err(FuseError::Integrity)
}

fn access_allowed(inode: &StoredInode, uid: u32, gid: u32, mask: fuser::AccessFlags) -> bool {
    if mask.is_empty() {
        return true;
    }
    let shift = if uid == inode.uid {
        6
    } else if gid == inode.gid {
        3
    } else {
        0
    };
    let permissions = (inode.mode >> shift) & 0o7;
    (!mask.contains(fuser::AccessFlags::R_OK) || permissions & 0o4 != 0)
        && (!mask.contains(fuser::AccessFlags::W_OK) || permissions & 0o2 != 0)
        && (!mask.contains(fuser::AccessFlags::X_OK) || permissions & 0o1 != 0)
}

fn write_directory_access_mask() -> fuser::AccessFlags {
    fuser::AccessFlags::W_OK | fuser::AccessFlags::X_OK
}

fn access_mask_for_open_flags(flags: fuser::OpenFlags) -> fuser::AccessFlags {
    match flags.acc_mode() {
        fuser::OpenAccMode::O_RDONLY => fuser::AccessFlags::R_OK,
        fuser::OpenAccMode::O_WRONLY => fuser::AccessFlags::W_OK,
        fuser::OpenAccMode::O_RDWR => fuser::AccessFlags::R_OK | fuser::AccessFlags::W_OK,
    }
}

fn read_poll_events() -> fuser::PollEvents {
    fuser::PollEvents::POLLIN | fuser::PollEvents::POLLRDNORM | fuser::PollEvents::POLLRDBAND
}

fn write_poll_events() -> fuser::PollEvents {
    fuser::PollEvents::POLLOUT | fuser::PollEvents::POLLWRNORM | fuser::PollEvents::POLLWRBAND
}

fn fallocate_keep_size() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::FALLOC_FL_KEEP_SIZE
    }
    #[cfg(not(target_os = "linux"))]
    {
        0x01
    }
}

fn fallocate_punch_hole() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::FALLOC_FL_PUNCH_HOLE
    }
    #[cfg(not(target_os = "linux"))]
    {
        0x02
    }
}

fn fallocate_zero_range() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::FALLOC_FL_ZERO_RANGE
    }
    #[cfg(not(target_os = "linux"))]
    {
        0x10
    }
}

fn fallocate_collapse_range() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::FALLOC_FL_COLLAPSE_RANGE
    }
    #[cfg(not(target_os = "linux"))]
    {
        0x08
    }
}

fn fallocate_insert_range() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::FALLOC_FL_INSERT_RANGE
    }
    #[cfg(not(target_os = "linux"))]
    {
        0x20
    }
}

fn fallocate_unshare_range() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::FALLOC_FL_UNSHARE_RANGE
    }
    #[cfg(not(target_os = "linux"))]
    {
        0x40
    }
}

fn inode_attr(inode_number: u64, inode: &StoredInode) -> fuser::FileAttr {
    fuser::FileAttr {
        ino: fuser::INodeNo(inode_number),
        size: inode.size,
        blocks: inode.size.div_ceil(512),
        atime: inode.atime.to_system_time(),
        mtime: inode.mtime.to_system_time(),
        ctime: inode.ctime.to_system_time(),
        crtime: inode.crtime.to_system_time(),
        kind: fuser_file_type(inode.kind),
        perm: inode.mode,
        nlink: inode.nlink,
        uid: inode.uid,
        gid: inode.gid,
        rdev: inode.rdev,
        blksize: FUSE_STATFS_BLOCK_SIZE,
        flags: inode.flags,
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

fn special_kind_from_mode(mode: u32) -> FuseResult<StoredNodeKind> {
    match mode & u32::from(libc::S_IFMT) {
        value if value == u32::from(libc::S_IFREG) => Ok(StoredNodeKind::RegularFile),
        value if value == u32::from(libc::S_IFIFO) => Ok(StoredNodeKind::NamedPipe),
        value if value == u32::from(libc::S_IFCHR) => Ok(StoredNodeKind::CharacterDevice),
        value if value == u32::from(libc::S_IFBLK) => Ok(StoredNodeKind::BlockDevice),
        value if value == u32::from(libc::S_IFSOCK) => Ok(StoredNodeKind::Socket),
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

fn permissions_from_mode(mode: u32, umask: u32) -> u16 {
    permission_bits(mode & !umask)
}

fn permission_bits(mode: u32) -> u16 {
    (mode & 0o7777) as u16
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
    if bytes.len() > FUSE_MAX_NAME_LENGTH {
        return Err(FuseError::Errno(fuser::Errno::ERANGE));
    }
    if bytes.contains(&0) {
        return Err(FuseError::Errno(fuser::Errno::EINVAL));
    }
    Ok(bytes.to_vec())
}

fn stored_time_from_time_or_now(time: fuser::TimeOrNow) -> StoredTime {
    match time {
        fuser::TimeOrNow::SpecificTime(time) => StoredTime::from_system_time(time),
        fuser::TimeOrNow::Now => StoredTime::now(),
    }
}

fn current_uid() -> u32 {
    unsafe { libc::getuid() }
}

fn current_gid() -> u32 {
    unsafe { libc::getgid() }
}

fn backing_filesystem_statistics(path: &Path) -> FuseResult<BackingFileSystemStatistics> {
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| FuseError::Database)?;
    let mut statistics = MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), statistics.as_mut_ptr()) };
    if result != 0 {
        return Err(FuseError::Database);
    }
    let statistics = unsafe { statistics.assume_init() };
    Ok(BackingFileSystemStatistics {
        blocks: statistics.f_blocks as u64,
        free_blocks: statistics.f_bfree as u64,
        available_blocks: statistics.f_bavail as u64,
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
    #[cfg(test)]
    if take_write_batch_fault(WriteBatchFault::BeforeWrite) {
        return Err(FilesystemError::Database);
    }

    let mut write_options = WriteOptions::default();
    write_options.set_sync(true);
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

fn encode_inode(inode: &StoredInode) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(inode).map_err(|_| FilesystemError::Database)
}

fn decode_inode(value: &[u8]) -> Result<StoredInode, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_directory_entry(entry: &StoredDirectoryEntry) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(entry).map_err(|_| FilesystemError::Database)
}

fn decode_directory_entry(value: &[u8]) -> Result<StoredDirectoryEntry, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_branch(branch: &StoredBranch) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(branch).map_err(|_| FilesystemError::Database)
}

fn decode_branch(value: &[u8]) -> Result<StoredBranch, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_extent(extent: &StoredExtent) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(extent).map_err(|_| FilesystemError::Database)
}

fn decode_extent(value: &[u8]) -> Result<StoredExtent, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
}

fn encode_snapshot_metadata(metadata: &StoredSnapshotMetadata) -> Result<Vec<u8>, FilesystemError> {
    postcard::to_allocvec(metadata).map_err(|_| FilesystemError::Database)
}

fn decode_snapshot_metadata(value: &[u8]) -> Result<StoredSnapshotMetadata, FilesystemError> {
    postcard::from_bytes(value).map_err(|_| FilesystemError::Integrity)
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

fn encode_branch_inode_key(branch_identifier: u64, inode_number: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&branch_identifier.to_be_bytes());
    key[8..].copy_from_slice(&inode_number.to_be_bytes());
    key
}

fn encode_branch_parent_prefix(branch_identifier: u64, parent: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&encode_u64(branch_identifier));
    key.extend_from_slice(&encode_u64(parent));
    key
}

fn encode_directory_entry_key(branch_identifier: u64, parent: u64, name: &[u8]) -> Vec<u8> {
    let mut key = encode_branch_parent_prefix(branch_identifier, parent);
    key.extend_from_slice(name);
    key
}

fn encode_extended_attribute_key(
    branch_identifier: u64,
    inode_number: u64,
    name: &[u8],
) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + name.len());
    key.extend_from_slice(&encode_u64(branch_identifier));
    key.extend_from_slice(&encode_u64(inode_number));
    key.extend_from_slice(name);
    key
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

fn encode_current_file_prefix(branch_identifier: u64, file_identifier: u64) -> Vec<u8> {
    encode_branch_file_prefix(branch_identifier, file_identifier)
}

fn encode_current_file_extent_key(
    branch_identifier: u64,
    file_identifier: u64,
    logical_offset: u64,
) -> [u8; 24] {
    encode_branch_file_position_key(branch_identifier, file_identifier, logical_offset)
}

fn encode_snapshot_file_prefix(branch_identifier: u64, file_identifier: u64) -> Vec<u8> {
    encode_branch_file_prefix(branch_identifier, file_identifier)
}

fn encode_snapshot_extent_prefix(
    branch_identifier: u64,
    file_identifier: u64,
    ordinal: u64,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&encode_u64(branch_identifier));
    key.extend_from_slice(&encode_u64(file_identifier));
    key.extend_from_slice(&encode_u64(ordinal));
    key
}

fn encode_snapshot_extent_key(
    branch_identifier: u64,
    file_identifier: u64,
    ordinal: u64,
    logical_offset: u64,
) -> [u8; 32] {
    let mut key = [0; 32];
    key[..8].copy_from_slice(&branch_identifier.to_be_bytes());
    key[8..16].copy_from_slice(&file_identifier.to_be_bytes());
    key[16..24].copy_from_slice(&ordinal.to_be_bytes());
    key[24..].copy_from_slice(&logical_offset.to_be_bytes());
    key
}

fn encode_event_payload_extent_prefix(event_sequence: u64, part: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.extend_from_slice(&encode_u64(event_sequence));
    key.push(part);
    key
}

fn encode_event_payload_extent_key(event_sequence: u64, part: u8, logical_offset: u64) -> [u8; 17] {
    let mut key = [0; 17];
    key[..8].copy_from_slice(&event_sequence.to_be_bytes());
    key[8] = part;
    key[9..].copy_from_slice(&logical_offset.to_be_bytes());
    key
}

fn decode_extent_offset_from_current_key(key: &[u8]) -> Result<u64, FilesystemError> {
    if key.len() != 24 {
        return Err(FilesystemError::Integrity);
    }
    decode_u64(&key[16..])
}

fn decode_snapshot_ordinal_and_offset(key: &[u8]) -> Result<(u64, u64), FilesystemError> {
    if key.len() != 32 {
        return Err(FilesystemError::Integrity);
    }
    Ok((decode_u64(&key[16..24])?, decode_u64(&key[24..])?))
}

fn is_snapshot_metadata_key(key: &[u8]) -> Result<bool, FilesystemError> {
    Ok(key.len() == 32 && decode_u64(&key[24..])? == u64::MAX)
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
        FileEventPayloadPart::Removed => EVENT_PAYLOAD_PART_REMOVED,
    }
}

fn event_payload_part_length(event: &EventRecord, part: FileEventPayloadPart) -> Option<u64> {
    match part {
        FileEventPayloadPart::Overwritten => event.overwritten_byte_length(),
        FileEventPayloadPart::Written => event.written_byte_length(),
        FileEventPayloadPart::Removed => event.removed_byte_length(),
    }
}

fn event_file_identifiers(event: &EventRecord) -> Vec<FileIdentifier> {
    let mut identifiers = Vec::new();
    if let Some(file_identifier) = event.file_identifier() {
        identifiers.push(file_identifier);
    }
    if let Some(file_identifier) = event.secondary_file_identifier()
        && !identifiers.contains(&file_identifier)
    {
        identifiers.push(file_identifier);
    }
    identifiers
}

fn event_snapshot_file_size(event: &EventRecord, file_identifier: FileIdentifier) -> Option<u64> {
    match event.payload() {
        EventPayload::FileWrite { new_file_size, .. }
        | EventPayload::FileTruncate { new_file_size, .. }
        | EventPayload::FileSizeChange { new_file_size, .. }
            if event.file_identifier() == Some(file_identifier) =>
        {
            Some(*new_file_size)
        }
        EventPayload::FileExchange {
            primary_file_size,
            secondary_file_size,
        } => {
            if event.file_identifier() == Some(file_identifier) {
                Some(*primary_file_size)
            } else if event.secondary_file_identifier() == Some(file_identifier) {
                Some(*secondary_file_size)
            } else {
                None
            }
        }
        EventPayload::None
        | EventPayload::FileWrite { .. }
        | EventPayload::FileTruncate { .. }
        | EventPayload::FileSizeChange { .. } => None,
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
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();

        storage
            .write_file(inode_number, 0, b"older", current_uid(), current_gid())
            .expect("older bytes are written");
        storage
            .write_file(inode_number, 0, b"newer", current_uid(), current_gid())
            .expect("newer bytes are written");
        let bytes = storage
            .read_file(inode_number, 0, 5, current_uid(), current_gid())
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
                0o644,
                0,
                current_credentials(),
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
                0o644,
                0,
                current_credentials(),
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
            .write_file(inode_number, 0, b"contents", current_uid(), current_gid())
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
            0,
            0,
            current_credentials(),
            CreateNodeKind::Special { mode: 0, rdev: 0 },
        );
        assert!(invalid_special.is_err());
        assert_eq!(write_state_snapshot(&storage), initial);

        let invalid_truncate = storage.setattr(
            INODE_ROOT,
            SetAttributes {
                mode: None,
                uid: None,
                gid: None,
                size: Some(0),
                atime: None,
                mtime: None,
                ctime: None,
                crtime: None,
                bkuptime: None,
                flags: None,
            },
            current_uid(),
            current_gid(),
        );
        assert!(invalid_truncate.is_err());
        assert_eq!(write_state_snapshot(&storage), initial);
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            EventSequence::new(initial.last_event_sequence)
        );
    }

    #[test]
    fn write_batch_fault_before_write_preserves_durable_and_cached_state() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let initial = write_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::BeforeWrite);
        assert!(matches!(
            storage.create_node(
                INODE_ROOT,
                OsStr::new("before-write-fault"),
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            ),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), initial);
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
    fn write_batch_fault_after_write_recovers_cached_state_on_reopen() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let initial = write_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::AfterWrite);
        assert!(matches!(
            storage.create_node(
                INODE_ROOT,
                OsStr::new("after-write-fault"),
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            ),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), initial);
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
            .write_file(inode_number, 0, b"abcdef", current_uid(), current_gid())
            .expect("initial bytes are written");
        let before_fault = write_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::BeforeWrite);
        assert!(matches!(
            storage.write_file(inode_number, 2, b"XYZ", current_uid(), current_gid()),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), before_fault);
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            EventSequence::new(before_fault.last_event_sequence)
        );
        assert_eq!(
            storage
                .read_file(inode_number, 0, 6, current_uid(), current_gid())
                .expect("file remains readable after pre-write fault"),
            b"abcdef"
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens after fault");
        assert_eq!(
            storage
                .read_file(inode_number, 0, 6, current_uid(), current_gid())
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
            .write_file(inode_number, 0, b"abcdef", current_uid(), current_gid())
            .expect("initial bytes are written");
        let before_fault = write_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::AfterWrite);
        assert!(matches!(
            storage.write_file(inode_number, 2, b"XYZ", current_uid(), current_gid()),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), before_fault);
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
                .read_file(inode_number, 0, 6, current_uid(), current_gid())
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
    fn write_batch_fault_after_truncate_recovers_removed_payload_and_snapshot_state_on_reopen() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        let inode_number = create_regular_file(&storage, "after-truncate-fault");
        storage
            .write_file(inode_number, 0, b"abcdef", current_uid(), current_gid())
            .expect("initial bytes are written");
        let before_fault = write_state_snapshot(&storage);

        set_write_batch_fault(WriteBatchFault::AfterWrite);
        assert!(matches!(
            storage.setattr(
                inode_number,
                SetAttributes {
                    mode: None,
                    uid: None,
                    gid: None,
                    size: Some(3),
                    atime: None,
                    mtime: None,
                    ctime: None,
                    crtime: None,
                    bkuptime: None,
                    flags: None,
                },
                current_uid(),
                current_gid(),
            ),
            Err(FuseError::Database)
        ));

        assert_eq!(write_state_snapshot(&storage), before_fault);
        let committed = EventSequence::new(before_fault.last_event_sequence + 1);
        assert_eq!(
            storage
                .last_event_sequence()
                .expect("last event sequence is readable"),
            committed
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens after truncate fault");
        assert_eq!(
            storage
                .read_file(inode_number, 0, 6, current_uid(), current_gid())
                .expect("truncated file is readable after reopen"),
            b"abc"
        );
        let event = storage
            .get_event(committed)
            .expect("committed truncate event is readable")
            .expect("committed truncate event exists");
        assert_eq!(event.kind(), EventKind::FileTruncated);
        assert_eq!(event.removed_byte_length(), Some(3));
        assert_eq!(
            storage
                .read_file_event_payload_range(committed, FileEventPayloadPart::Removed, 0, 3)
                .expect("removed payload is readable"),
            b"def"
        );
        let snapshot = storage
            .file_snapshot_at_or_before(FileIdentifier::new(inode_number), committed)
            .expect("snapshot lookup succeeds")
            .expect("snapshot exists");
        assert_eq!(
            storage
                .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
                .expect("snapshot bytes are readable"),
            b"abc"
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
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");

        let duplicate_sequence = EventSequence::new(EVENT_SEQUENCE_INITIAL.get() + 1);
        let duplicate_event = EventRecord::new(
            duplicate_sequence,
            EventKind::MetadataChanged,
            UtcDateTime::now(),
            None,
            None,
            None,
            None,
        );
        let mut batch = WriteBatch::default();

        assert!(matches!(
            storage.commit_event(
                &mut batch,
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
            EventKind::MetadataChanged,
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
                0o644,
                0,
                current_credentials(),
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
    fn corrupt_branch_event_indexes_are_rejected_during_branch_materialization() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        storage
            .create_node(
                INODE_ROOT,
                OsStr::new("materialized"),
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let main = storage.current_branch().expect("main branch is returned");

        let mut batch = WriteBatch::default();
        batch.put_cf(
            storage
                .branch_events()
                .expect("branch events column family exists"),
            encode_branch_position_key(
                main.branch_identifier().get(),
                main.head_position().ordinal(),
            ),
            encode_u64(EVENT_SEQUENCE_INITIAL.get()),
        );
        write_batch(storage.database(), batch).expect("corrupt branch event index is written");

        assert!(matches!(
            storage.create_branch(
                &BranchName::new("corrupt-materialization").expect("branch name is valid"),
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
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();
        storage
            .write_file(
                inode_number,
                0,
                b"content bytes",
                current_uid(),
                current_gid(),
            )
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
            storage.read_file(inode_number, 0, 100, current_uid(), current_gid()),
            Err(FuseError::Integrity)
        ));
        assert!(matches!(
            storage.write_file(inode_number, 0, b"new", current_uid(), current_gid()),
            Err(FuseError::Integrity)
        ));
        assert!(matches!(
            storage.setattr(
                inode_number,
                SetAttributes {
                    mode: None,
                    uid: None,
                    gid: None,
                    size: Some(3),
                    atime: None,
                    mtime: None,
                    ctime: None,
                    crtime: None,
                    bkuptime: None,
                    flags: None,
                },
                current_uid(),
                current_gid(),
            ),
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
    fn large_snapshot_and_payload_ranges_cross_content_chunks() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let created = storage
            .create_node(
                INODE_ROOT,
                OsStr::new("large"),
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();
        let file_identifier = FileIdentifier::new(inode_number);
        let overwrite_offset = 96 * 1024;
        let truncate_size = 220 * 1024;
        let original = patterned_bytes(300 * 1024, 11);
        let replacement = patterned_bytes(70 * 1024, 91);
        let mut expected_after_overwrite = original.clone();
        expected_after_overwrite[overwrite_offset..overwrite_offset + replacement.len()]
            .copy_from_slice(&replacement);
        let expected_after_truncate = expected_after_overwrite[..truncate_size].to_vec();
        let removed_after_truncate = expected_after_overwrite[truncate_size..].to_vec();

        storage
            .write_file(inode_number, 0, &original, current_uid(), current_gid())
            .expect("large file is written");
        let initial_write = storage
            .last_event_sequence()
            .expect("last event sequence is readable");
        storage
            .write_file(
                inode_number,
                overwrite_offset as u64,
                &replacement,
                current_uid(),
                current_gid(),
            )
            .expect("large file is overwritten");
        let overwrite = storage
            .last_event_sequence()
            .expect("last event sequence is readable");
        storage
            .setattr(
                inode_number,
                SetAttributes {
                    mode: None,
                    uid: None,
                    gid: None,
                    size: Some(truncate_size as u64),
                    atime: None,
                    mtime: None,
                    ctime: None,
                    crtime: None,
                    bkuptime: None,
                    flags: None,
                },
                current_uid(),
                current_gid(),
            )
            .expect("large file truncates");
        let truncate = storage
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
        assert_eq!(
            storage
                .read_file_event_payload_range(
                    truncate,
                    FileEventPayloadPart::Removed,
                    0,
                    removed_after_truncate.len() as u64,
                )
                .expect("large removed payload is read"),
            removed_after_truncate
        );

        let snapshot = storage
            .file_snapshot_at_or_before(file_identifier, truncate)
            .expect("large snapshot lookup succeeds")
            .expect("large snapshot exists");
        assert_eq!(
            storage
                .read_file_snapshot_range(&snapshot, 0, snapshot.file_size())
                .expect("large snapshot is read"),
            expected_after_truncate
        );
        assert_eq!(
            storage
                .read_file_snapshot_range(&snapshot, 200 * 1024, 64 * 1024)
                .expect("large snapshot range is read"),
            expected_after_truncate[200 * 1024..]
        );
    }

    #[test]
    fn bounded_extent_write_and_truncate_model_matches_dense_bytes() {
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
                0o644,
                0,
                current_credentials(),
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
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            )
            .expect("file is created");
        let inode_number = created.attr.ino.into();
        let after_create = storage
            .statfs(&database)
            .expect("filesystem statistics are returned after create");
        assert_eq!(after_create.free_files, initial_statistics.free_files - 1);

        storage
            .write_file(inode_number, 0, b"contents", current_uid(), current_gid())
            .expect("file is written");
        let after_write = storage
            .statfs(&database)
            .expect("filesystem statistics are returned after write");
        assert_eq!(after_write.free_files, after_create.free_files);

        storage
            .hard_link(
                inode_number,
                INODE_ROOT,
                OsStr::new("hard-link"),
                current_uid(),
                current_gid(),
            )
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
                current_credentials(),
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
            .setxattr(
                inode_number,
                name,
                b"main",
                libc::XATTR_CREATE,
                current_uid(),
                current_gid(),
            )
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
            .setxattr(
                inode_number,
                name,
                b"branch",
                libc::XATTR_REPLACE,
                current_uid(),
                current_gid(),
            )
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
            .removexattr(inode_number, name, current_uid())
            .expect("main xattr is removed");
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
    fn sparse_zero_range_and_lseek_use_extent_holes() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let inode_number = create_regular_file(&storage, "sparse-zero");
        storage
            .write_file(inode_number, 0, b"abcdef", current_uid(), current_gid())
            .expect("file is written");

        storage
            .fallocate(
                inode_number,
                2,
                2,
                fallocate_zero_range() | fallocate_keep_size(),
                current_uid(),
                current_gid(),
            )
            .expect("range is zeroed");

        assert_eq!(
            storage
                .read_file(inode_number, 0, 6, current_uid(), current_gid())
                .expect("zeroed file is readable"),
            b"ab\0\0ef"
        );
        assert_eq!(
            storage
                .lseek(inode_number, 0, libc::SEEK_DATA)
                .expect("first data is found"),
            0
        );
        assert_eq!(
            storage
                .lseek(inode_number, 0, libc::SEEK_HOLE)
                .expect("first hole is found"),
            2
        );
        assert_eq!(
            storage
                .lseek(inode_number, 2, libc::SEEK_DATA)
                .expect("next data is found"),
            4
        );
        assert_eq!(
            storage
                .get_event(
                    storage
                        .last_event_sequence()
                        .expect("last event is readable")
                )
                .expect("event lookup succeeds")
                .expect("event exists")
                .kind(),
            EventKind::FileRangeZeroed
        );
    }

    #[test]
    fn copy_file_range_writes_destination_once() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let source = create_regular_file(&storage, "copy-source");
        let destination = create_regular_file(&storage, "copy-destination");
        storage
            .write_file(source, 0, b"copy source", current_uid(), current_gid())
            .expect("source is written");
        storage
            .write_file(destination, 0, b"destination", current_uid(), current_gid())
            .expect("destination is written");
        let before = storage
            .last_event_sequence()
            .expect("event sequence is readable");

        let copied = storage
            .copy_file_range(source, 0, destination, 0, 4, current_uid(), current_gid())
            .expect("range is copied");

        assert_eq!(copied.written, 4);
        assert_eq!(
            storage
                .read_file(destination, 0, 11, current_uid(), current_gid())
                .expect("destination is readable"),
            b"copyination"
        );
        let after = storage
            .last_event_sequence()
            .expect("event sequence is readable");
        assert_eq!(after.get(), before.get() + 1);
        assert_eq!(
            storage
                .get_event(after)
                .expect("event lookup succeeds")
                .expect("event exists")
                .kind(),
            EventKind::FileWritten
        );
    }

    #[test]
    fn exchange_swaps_contents_and_indexes_both_file_histories() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let storage =
            open_database_path(&temporary.path().join("database")).expect("storage opens");
        let left = create_regular_file(&storage, "left");
        let right = create_regular_file(&storage, "right");
        storage
            .write_file(left, 0, b"left", current_uid(), current_gid())
            .expect("left is written");
        storage
            .write_file(right, 0, b"right", current_uid(), current_gid())
            .expect("right is written");

        storage
            .exchange(
                INODE_ROOT,
                OsStr::new("left"),
                INODE_ROOT,
                OsStr::new("right"),
                current_uid(),
                current_gid(),
            )
            .expect("contents are exchanged");

        assert_eq!(
            storage
                .read_file(left, 0, 8, current_uid(), current_gid())
                .expect("left is readable"),
            b"right"
        );
        assert_eq!(
            storage
                .read_file(right, 0, 8, current_uid(), current_gid())
                .expect("right is readable"),
            b"left"
        );
        let event = storage
            .get_event(
                storage
                    .last_event_sequence()
                    .expect("last event is readable"),
            )
            .expect("event lookup succeeds")
            .expect("event exists");
        assert_eq!(event.kind(), EventKind::FileContentsExchanged);
        assert_eq!(event.file_identifier(), Some(FileIdentifier::new(left)));
        assert_eq!(
            event.secondary_file_identifier(),
            Some(FileIdentifier::new(right))
        );
        assert_eq!(
            collect_all_file_events(&storage, FileIdentifier::new(left))
                .last()
                .map(EventRecord::kind),
            Some(EventKind::FileContentsExchanged)
        );
        assert_eq!(
            collect_all_file_events(&storage, FileIdentifier::new(right))
                .last()
                .map(EventRecord::kind),
            Some(EventKind::FileContentsExchanged)
        );
    }

    #[test]
    fn volume_name_persists_and_appends_rename_events() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let database = temporary.path().join("database");
        let storage = open_database_path(&database).expect("storage opens");
        assert_eq!(
            storage.volume_name().expect("volume name is readable"),
            METADATA_VOLUME_NAME_DEFAULT
        );

        storage
            .set_volume_name(OsStr::new("eventfs-renamed"))
            .expect("volume name is changed");
        let sequence = storage
            .last_event_sequence()
            .expect("last event is readable");
        assert_eq!(
            storage
                .get_event(sequence)
                .expect("event lookup succeeds")
                .expect("event exists")
                .kind(),
            EventKind::VolumeRenamed
        );
        drop(storage);

        let storage = open_database_path(&database).expect("storage reopens");
        assert_eq!(
            storage.volume_name().expect("volume name persists"),
            "eventfs-renamed"
        );
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
        removed: Vec<u8>,
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
                removed: Vec::new(),
            }
        }

        fn old_file_size(&self) -> Option<u64> {
            match self.kind {
                EventKind::FileWritten | EventKind::FileTruncated => Some(self.before.len() as u64),
                _ => None,
            }
        }

        fn new_file_size(&self) -> Option<u64> {
            match self.kind {
                EventKind::FileWritten | EventKind::FileTruncated => Some(self.after.len() as u64),
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

        fn removed_byte_length(&self) -> Option<u64> {
            match self.kind {
                EventKind::FileTruncated => Some(self.removed.len() as u64),
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
            self.extents = current_extents_after_truncate(&self.extents, size)?;
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
            BoundedFileOperation::Truncate { size: 80 * 1024 },
            BoundedFileOperation::Write {
                offset: 40 * 1024,
                bytes: patterned_bytes(120 * 1024, 29),
            },
            BoundedFileOperation::Truncate { size: 210 * 1024 },
            BoundedFileOperation::Write {
                offset: 192 * 1024,
                bytes: patterned_bytes(12 * 1024, 41),
            },
            BoundedFileOperation::Truncate { size: 32 * 1024 },
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
            BoundedFileOperation::Truncate { size: 50 * 1024 },
            BoundedFileOperation::Write {
                offset: 80 * 1024,
                bytes: patterned_bytes(20 * 1024, 37),
            },
            BoundedFileOperation::Truncate { size: 140 * 1024 },
            BoundedFileOperation::Write {
                offset: 128 * 1024,
                bytes: patterned_bytes(8 * 1024, 59),
            },
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
                    removed: Vec::new(),
                }
            }
            BoundedFileOperation::Truncate { size } => {
                let before = dense.clone();
                let size = usize::try_from(*size).expect("bounded truncate size fits usize");
                let offset = before.len().min(size) as u64;
                let byte_length = before.len().abs_diff(size) as u64;
                let removed = if size < before.len() {
                    before[size..].to_vec()
                } else {
                    Vec::new()
                };
                dense.resize(size, 0);

                BoundedFileExpectation {
                    kind: EventKind::FileTruncated,
                    before,
                    after: dense.clone(),
                    offset: Some(offset),
                    byte_length: Some(byte_length),
                    overwritten: Vec::new(),
                    written: Vec::new(),
                    removed,
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
                    .write_file(inode_number, *offset, bytes, current_uid(), current_gid())
                    .expect("bounded write succeeds");
            }
            BoundedFileOperation::Truncate { size } => {
                storage
                    .setattr(
                        inode_number,
                        SetAttributes {
                            mode: None,
                            uid: None,
                            gid: None,
                            size: Some(*size),
                            atime: None,
                            mtime: None,
                            ctime: None,
                            crtime: None,
                            bkuptime: None,
                            flags: None,
                        },
                        current_uid(),
                        current_gid(),
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
            event.removed_byte_length(),
            expectation.removed_byte_length()
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
                .read_file_event_payload_range(
                    sequence,
                    FileEventPayloadPart::Removed,
                    0,
                    expectation.removed.len() as u64 + 64,
                )
                .expect("removed payload is readable"),
            expectation.removed
        );
        assert_eq!(
            storage
                .read_file(
                    inode_number,
                    0,
                    u32::try_from(expectation.after.len() + 16)
                        .expect("bounded file length fits u32"),
                    current_uid(),
                    current_gid(),
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

    fn count_content_chunks(storage: &Storage) -> usize {
        let content_chunks = storage
            .content_chunks()
            .expect("content chunks column family exists");
        let mut iterator = storage.database().raw_iterator_cf(content_chunks);
        let mut count = 0;

        iterator.seek_to_first();
        while iterator.valid() {
            count += 1;
            iterator.next();
        }
        iterator.status().expect("content chunk iterator is valid");
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
            EventKind::FileTruncated => bytes.resize(
                usize::try_from(event.new_file_size().expect("new file size exists"))
                    .expect("new file size fits usize"),
                0,
            ),
            _ => {}
        }
    }

    fn write_state_snapshot(storage: &Storage) -> WriteState {
        *storage
            .write_state
            .lock()
            .expect("write state lock is available")
    }

    fn create_regular_file(storage: &Storage, name: &str) -> u64 {
        storage
            .create_node(
                INODE_ROOT,
                OsStr::new(name),
                0o644,
                0,
                current_credentials(),
                CreateNodeKind::RegularFile,
            )
            .expect("regular file is created")
            .attr
            .ino
            .into()
    }

    fn current_credentials() -> FuseCredentials {
        FuseCredentials {
            uid: current_uid(),
            gid: current_gid(),
        }
    }

    fn patterned_bytes(length: usize, salt: usize) -> Vec<u8> {
        (0..length)
            .map(|index| ((index * 31 + salt) % 251) as u8)
            .collect()
    }
}
