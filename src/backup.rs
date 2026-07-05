#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeSet;
use std::fs;
use std::num::NonZeroU32;
use std::path::{Component, Path, PathBuf};

use rocksdb::Env;
use rocksdb::backup::{BackupEngine, BackupEngineInfo, BackupEngineOptions, RestoreOptions};

use crate::filesystem::{ConfigurationError, EventSequence, Filesystem, FilesystemError};
use crate::storage;

/// Local RocksDB BackupEngine repository directory.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackupDirectory(PathBuf);

impl BackupDirectory {
    /// Creates a backup directory value.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError::EmptyValue`] when the supplied path is empty.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, ConfigurationError> {
        new_backup_directory(path.into())
    }

    /// Returns the configured backup directory path.
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

/// Non-zero RocksDB BackupEngine backup identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackupIdentifier(NonZeroU32);

impl BackupIdentifier {
    /// Creates a backup identifier value.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError::ZeroValue`] when the identifier is zero.
    pub fn new(value: u32) -> Result<Self, ConfigurationError> {
        new_backup_identifier(value)
    }

    /// Returns the numeric backup identifier.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Receipt returned after creating a local backup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupReceipt {
    backup_identifier: BackupIdentifier,
    source_event_sequence: EventSequence,
}

impl BackupReceipt {
    /// Returns the created backup identifier.
    pub fn backup_identifier(&self) -> BackupIdentifier {
        self.backup_identifier
    }

    /// Returns the source filesystem event sequence included in the backup.
    pub fn source_event_sequence(&self) -> EventSequence {
        self.source_event_sequence
    }
}

/// Receipt returned after importing a local backup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportReceipt {
    backup_identifier: BackupIdentifier,
    imported_event_sequence: EventSequence,
}

impl ImportReceipt {
    /// Returns the imported backup identifier.
    pub fn backup_identifier(&self) -> BackupIdentifier {
        self.backup_identifier
    }

    /// Returns the restored filesystem event sequence.
    pub fn imported_event_sequence(&self) -> EventSequence {
        self.imported_event_sequence
    }
}

impl Filesystem {
    /// Creates a verified incremental backup in a local backup directory.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Backup`] when the backup repository cannot be opened or written,
    /// [`FilesystemError::Integrity`] when the created backup fails verification, and
    /// [`FilesystemError::Database`] when source metadata cannot be read.
    pub fn create_backup(
        &self,
        backup_directory: BackupDirectory,
    ) -> Result<BackupReceipt, FilesystemError> {
        create_backup_internal(self, backup_directory)
    }

    /// Imports a verified local backup into a database directory.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemError::Integrity`] when the backup repository or identifier is invalid,
    /// and [`FilesystemError::Import`] when restore, validation, or target replacement fails.
    pub fn import_backup(
        database_directory: impl Into<PathBuf>,
        backup_directory: BackupDirectory,
        backup_identifier: BackupIdentifier,
    ) -> Result<ImportReceipt, FilesystemError> {
        import_backup_internal(
            database_directory.into(),
            backup_directory,
            backup_identifier,
        )
    }
}

fn create_backup_internal(
    filesystem: &Filesystem,
    backup_directory: BackupDirectory,
) -> Result<BackupReceipt, FilesystemError> {
    if paths_overlap(
        filesystem.database_directory(),
        &normalized_path(backup_directory.as_path(), FilesystemError::Backup)?,
    ) {
        return Err(FilesystemError::Backup);
    }
    fs::create_dir_all(backup_directory.as_path()).map_err(|_| FilesystemError::Backup)?;

    let mut backup_engine =
        open_backup_engine(backup_directory.as_path(), FilesystemError::Backup)?;
    let existing_backup_identifiers = backup_identifiers(&backup_engine);
    filesystem.storage().with_commit_lock(|| {
        backup_engine
            .create_new_backup_flush(filesystem.storage().database(), true)
            .map_err(|_| FilesystemError::Backup)?;
        #[cfg(test)]
        if take_backup_fault(BackupFault::AfterCreateBeforeVerify) {
            return Err(FilesystemError::Backup);
        }

        let backup_identifier = created_backup_identifier(
            &existing_backup_identifiers,
            &backup_engine.get_backup_info(),
        )?;
        backup_engine
            .verify_backup(backup_identifier.get())
            .map_err(|_| FilesystemError::Integrity)?;

        Ok(BackupReceipt {
            backup_identifier,
            source_event_sequence: filesystem.storage().last_event_sequence()?,
        })
    })
}

fn import_backup_internal(
    database_directory: PathBuf,
    backup_directory: BackupDirectory,
    backup_identifier: BackupIdentifier,
) -> Result<ImportReceipt, FilesystemError> {
    validate_existing_backup_directory(backup_directory.as_path())?;
    validate_import_database_directory(&database_directory, backup_directory.as_path())?;
    let mut backup_engine =
        open_backup_engine(backup_directory.as_path(), FilesystemError::Integrity)?;
    backup_engine
        .verify_backup(backup_identifier.get())
        .map_err(|_| FilesystemError::Integrity)?;

    let target_parent = database_directory.parent().ok_or(FilesystemError::Import)?;
    let temporary_directory = tempfile::Builder::new()
        .prefix(".eventfs-import-restore-")
        .tempdir_in(target_parent)
        .map_err(|_| FilesystemError::Import)?;
    let restored_database_directory = temporary_directory.path().join("database");
    let restore_options = RestoreOptions::default();
    backup_engine
        .restore_from_backup(
            &restored_database_directory,
            &restored_database_directory,
            &restore_options,
            backup_identifier.get(),
        )
        .map_err(|_| FilesystemError::Import)?;

    let restored_storage = storage::open_database_path(&restored_database_directory)
        .map_err(|_| FilesystemError::Import)?;
    let imported_event_sequence = restored_storage
        .last_event_sequence()
        .map_err(|_| FilesystemError::Import)?;
    drop(restored_storage);

    validate_import_database_directory(&database_directory, backup_directory.as_path())?;
    #[cfg(test)]
    if take_backup_fault(BackupFault::AfterRestoreBeforeReplace) {
        return Err(FilesystemError::Import);
    }
    replace_path(&database_directory, &restored_database_directory)?;

    Ok(ImportReceipt {
        backup_identifier,
        imported_event_sequence,
    })
}

fn validate_import_database_directory(
    path: &Path,
    backup_directory: &Path,
) -> Result<(), FilesystemError> {
    if path.as_os_str().is_empty() {
        return Err(FilesystemError::Import);
    }
    let backup = normalized_path(backup_directory, FilesystemError::Integrity)?;
    let target = normalized_path(path, FilesystemError::Import)?;
    if paths_overlap(&target, &backup) {
        return Err(FilesystemError::Import);
    }
    Ok(())
}

fn open_backup_engine(
    backup_directory: &Path,
    error: FilesystemError,
) -> Result<BackupEngine, FilesystemError> {
    let backup_options = BackupEngineOptions::new(backup_directory).map_err(|_| error)?;
    let environment = Env::new().map_err(|_| error)?;
    BackupEngine::open(&backup_options, &environment).map_err(|_| error)
}

fn backup_identifiers(backup_engine: &BackupEngine) -> BTreeSet<u32> {
    backup_engine
        .get_backup_info()
        .into_iter()
        .map(|backup| backup.backup_id)
        .collect()
}

fn created_backup_identifier(
    existing_backup_identifiers: &BTreeSet<u32>,
    backup_info: &[BackupEngineInfo],
) -> Result<BackupIdentifier, FilesystemError> {
    let created_identifier = backup_info
        .iter()
        .map(|backup| backup.backup_id)
        .filter(|backup_identifier| !existing_backup_identifiers.contains(backup_identifier))
        .max()
        .ok_or(FilesystemError::Backup)?;
    BackupIdentifier::new(created_identifier).map_err(|_| FilesystemError::Backup)
}

fn validate_existing_backup_directory(path: &Path) -> Result<(), FilesystemError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(FilesystemError::Integrity),
        Err(_) => Err(FilesystemError::Integrity),
    }
}

fn replace_path(target: &Path, source: &Path) -> Result<(), FilesystemError> {
    let parent = target.parent().ok_or(FilesystemError::Import)?;
    fs::create_dir_all(parent).map_err(|_| FilesystemError::Import)?;
    let old_slot = tempfile::Builder::new()
        .prefix(".eventfs-import-old-")
        .tempdir_in(parent)
        .map_err(|_| FilesystemError::Import)?;
    let old_path = old_slot.path().to_path_buf();
    old_slot.close().map_err(|_| FilesystemError::Import)?;
    if target.exists() {
        fs::rename(target, &old_path).map_err(|_| FilesystemError::Import)?;
    }
    #[cfg(test)]
    if take_backup_fault(BackupFault::DuringReplaceAfterOldMoved) {
        if old_path.exists() {
            let _ = fs::rename(&old_path, target);
        }
        return Err(FilesystemError::Import);
    }
    if fs::rename(source, target).is_err() {
        if old_path.exists() {
            let _ = fs::rename(&old_path, target);
        }
        return Err(FilesystemError::Import);
    }
    remove_path_if_exists(&old_path).map_err(|_| FilesystemError::Import)
}

fn paths_overlap(first: &Path, second: &Path) -> bool {
    first == second || first.starts_with(second) || second.starts_with(first)
}

fn normalized_path(path: &Path, error: FilesystemError) -> Result<PathBuf, FilesystemError> {
    if path.as_os_str().is_empty() {
        return Err(error);
    }
    if let Ok(canonical) = fs::canonicalize(path) {
        return Ok(canonical);
    }

    let mut normalized = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().map_err(|_| error)?
    };
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            Component::Normal(name) => {
                let candidate = normalized.join(name);
                normalized = fs::canonicalize(&candidate).unwrap_or(candidate);
            }
        }
    }
    Ok(normalized)
}

fn remove_path_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn new_backup_directory(path: PathBuf) -> Result<BackupDirectory, ConfigurationError> {
    if path.as_os_str().is_empty() {
        return Err(ConfigurationError::EmptyValue);
    }
    Ok(BackupDirectory(path))
}

fn new_backup_identifier(value: u32) -> Result<BackupIdentifier, ConfigurationError> {
    NonZeroU32::new(value)
        .map(BackupIdentifier)
        .ok_or(ConfigurationError::ZeroValue)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackupFault {
    AfterCreateBeforeVerify,
    AfterRestoreBeforeReplace,
    DuringReplaceAfterOldMoved,
}

#[cfg(test)]
thread_local! {
    static BACKUP_FAULT: Cell<Option<BackupFault>> = const { Cell::new(None) };
}

#[cfg(test)]
fn set_backup_fault(fault: BackupFault) {
    BACKUP_FAULT.with(|slot| slot.set(Some(fault)));
}

#[cfg(test)]
fn take_backup_fault(fault: BackupFault) -> bool {
    BACKUP_FAULT.with(|slot| {
        if slot.get() == Some(fault) {
            slot.set(None);
            true
        } else {
            false
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::filesystem::{BranchName, FilesystemConfiguration};

    #[test]
    fn backup_fault_after_creation_leaves_repository_usable() {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let filesystem = filesystem_at(
            temporary.path().join("source-database"),
            temporary.path().join("source-mount"),
        );
        let backup_directory =
            BackupDirectory::new(temporary.path().join("backups")).expect("backup path is valid");

        set_backup_fault(BackupFault::AfterCreateBeforeVerify);
        assert_eq!(
            filesystem.create_backup(backup_directory.clone()),
            Err(FilesystemError::Backup)
        );

        let retry = filesystem
            .create_backup(backup_directory)
            .expect("backup retry succeeds");
        assert!(retry.backup_identifier().get() > 0);
    }

    #[test]
    fn import_fault_after_restore_validation_preserves_target_database() {
        assert_import_fault_preserves_target(BackupFault::AfterRestoreBeforeReplace);
    }

    #[test]
    fn import_fault_during_replacement_after_old_target_move_rolls_back_target_database() {
        assert_import_fault_preserves_target(BackupFault::DuringReplaceAfterOldMoved);
    }

    fn assert_import_fault_preserves_target(fault: BackupFault) {
        let temporary = tempfile::tempdir().expect("temporary directory is created");
        let source = filesystem_at(
            temporary.path().join("source-database"),
            temporary.path().join("source-mount"),
        );
        let backup_directory =
            BackupDirectory::new(temporary.path().join("backups")).expect("backup path is valid");
        let backup = source
            .create_backup(backup_directory.clone())
            .expect("backup succeeds");

        let target_database = temporary.path().join("target-database");
        let target = filesystem_at(
            target_database.clone(),
            temporary.path().join("target-mount"),
        );
        let target_branch = BranchName::new("target-only").expect("branch name is valid");
        let main = target
            .current_branch()
            .expect("target main branch is returned");
        target
            .create_branch(&target_branch, main.head_position())
            .expect("target-only branch is created");
        drop(target);

        set_backup_fault(fault);
        assert_eq!(
            Filesystem::import_backup(
                target_database.clone(),
                backup_directory,
                backup.backup_identifier(),
            ),
            Err(FilesystemError::Import)
        );

        let target = filesystem_at(target_database, temporary.path().join("target-remount"));
        target
            .switch_branch(&target_branch)
            .expect("target-only branch remains after failed import");
    }

    fn filesystem_at(database_directory: PathBuf, mount_point: PathBuf) -> Filesystem {
        fs::create_dir_all(&mount_point).expect("mount point is created");
        Filesystem::open(
            FilesystemConfiguration::new(database_directory, mount_point)
                .expect("configuration is valid"),
        )
        .expect("filesystem opens")
    }
}
