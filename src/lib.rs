//! Event-sourced filesystem library mounted through FUSE and persisted in RocksDB.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

#[cfg(target_os = "linux")]
pub use backup::{BackupDirectory, BackupIdentifier, BackupReceipt, ImportReceipt};
#[cfg(target_os = "linux")]
pub use filesystem::{
    BranchEventPage, BranchIdentifier, BranchName, BranchPage, BranchPageLimit, BranchPosition,
    BranchRecord, BranchStatus, ConfigurationError, EventKind, EventPage, EventPageLimit,
    EventRecord, EventSequence, FileEventPayloadPart, FileIdentifier, FileSnapshot, Filesystem,
    FilesystemConfiguration, FilesystemError, FuseOperationError, MountOption,
};
#[cfg(target_os = "linux")]
pub use mount::MountedFilesystem;

#[cfg(target_os = "linux")]
mod backup;
#[cfg(target_os = "linux")]
mod filesystem;
#[cfg(target_os = "linux")]
mod mount;
#[cfg(target_os = "linux")]
mod storage;

#[cfg(not(target_os = "linux"))]
compile_error!("eventfs supports only Linux targets");
