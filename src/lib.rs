//! Event-sourced filesystem library mounted through FUSE and persisted in RocksDB.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("eventfs supports only Linux and macOS targets");

pub use backup::{BackupDirectory, BackupIdentifier, BackupReceipt, ImportReceipt};
pub use filesystem::{
    BranchEventPage, BranchIdentifier, BranchName, BranchPage, BranchPageLimit, BranchPosition,
    BranchRecord, BranchStatus, ConfigurationError, EventKind, EventPage, EventPageLimit,
    EventRecord, EventSequence, FileEventPayloadPart, FileIdentifier, FileSnapshot, Filesystem,
    FilesystemConfiguration, FilesystemConfigurationBuilder, FilesystemError, Ready,
    WantsDatabaseDirectory, WantsMountPoint,
};
pub use mount::MountedFilesystem;

mod backup;
mod filesystem;
mod mount;
mod storage;
