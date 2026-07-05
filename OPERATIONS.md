# eventfs Operations

## Data Location

- `FilesystemConfiguration.database_directory` is the durable RocksDB data directory.
- `FilesystemConfiguration.mount_point` is only the FUSE presentation mount point.
- Operators MUST preserve the database directory for durable filesystem state. The mount point can be recreated as an empty directory before mounting.
- The database directory and mount point MUST NOT intentionally overlap.

## Backup Rotation

- `BackupDirectory` is a local RocksDB BackupEngine repository.
- Repeated `Filesystem::create_backup` calls against the same `BackupDirectory` add incremental backups to that repository and preserve existing repository contents.
- eventfs does not expose an API to delete individual BackupEngine backups.
- Rotate backups by retaining, archiving, or replacing whole `BackupDirectory` repositories outside active `create_backup` and `import_backup` calls.

## Recovery

- Restore with `Filesystem::import_backup(database_directory, backup_directory, backup_identifier)`.
- The import path verifies the requested backup, restores it to a temporary directory, opens the restored RocksDB database, then replaces the target database directory.
- A failed import returns `FilesystemError::Import` or `FilesystemError::Integrity` and must not be treated as a successful recovery.
- After import succeeds, open the restored database with `Filesystem::open` and mount it with `Filesystem::spawn_mount` or `Filesystem::mount`.

## Upgrade And Downgrade

- eventfs is unreleased; current pre-release APIs and storage schemas are not compatibility baselines.
- A database created by a newer storage schema is rejected before mutation by older code.
- Downgrade only by importing a backup whose storage schema is compatible with the older eventfs version.
- Before upgrading or downgrading deployed data, create and retain a complete `BackupDirectory` repository that can be imported by the target version.
