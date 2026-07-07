#![cfg(target_os = "linux")]

mod support;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use support::{TestDirectories, mount, open_test_filesystem, statvfs};

#[test]
fn mounted_client_style_workflows_use_filesystem_successfully() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let root = directories.root_path();
    let mount = directories.mount_point_path();
    let first_local = root.join("first-local.txt");
    let second_local = root.join("second-local.txt");
    let preserved_local = root.join("preserved-local.txt");
    let downloaded = root.join("downloaded.txt");

    fs::write(&first_local, b"fresh upload\n").expect("first local file is written");
    fs::write(&second_local, b"xy\n").expect("second local file is written");
    fs::write(&preserved_local, b"preserved\n").expect("preserved local file is written");
    fs::set_permissions(&preserved_local, fs::Permissions::from_mode(0o640))
        .expect("preserved local mode is set");
    let preserved_mtime = UNIX_EPOCH + Duration::from_secs(1_700_000_101);
    set_file_times(
        &preserved_local,
        UNIX_EPOCH + Duration::from_secs(1_700_000_001),
        preserved_mtime,
    )
    .expect("preserved local timestamps are set");

    upload_file(
        &first_local,
        &mount.join("fresh.txt"),
        UploadOptions::default(),
    )
    .expect("fresh upload succeeds");
    download_file(&mount.join("fresh.txt"), &downloaded).expect("download succeeds");
    assert_eq!(
        fs::read(mount.join("fresh.txt")).expect("fresh upload is readable"),
        b"fresh upload\n"
    );
    assert_eq!(
        fs::read(&downloaded).expect("downloaded file is readable"),
        b"fresh upload\n"
    );
    assert_filesystem_statistics_are_readable(mount);

    upload_file(
        &second_local,
        &mount.join("fresh.txt"),
        UploadOptions::default(),
    )
    .expect("overwrite upload succeeds");
    assert_eq!(
        fs::read(mount.join("fresh.txt")).expect("overwritten upload is readable"),
        b"xy\n"
    );

    upload_file(
        &preserved_local,
        &mount.join("preserved.txt"),
        UploadOptions {
            preserve_metadata: true,
            synchronize: false,
        },
    )
    .expect("metadata-preserving upload succeeds");
    assert_eq!(
        fs::read(mount.join("preserved.txt")).expect("metadata-preserved upload is readable"),
        b"preserved\n"
    );
    let preserved_metadata =
        fs::metadata(mount.join("preserved.txt")).expect("metadata-preserved upload has metadata");
    assert_eq!(preserved_metadata.permissions().mode() & 0o777, 0o640);
    assert_eq!(
        system_time_seconds(
            preserved_metadata
                .modified()
                .expect("preserved modified time exists"),
        ),
        system_time_seconds(preserved_mtime),
    );

    let directory = mount.join("dir");
    let uploaded = directory.join("uploaded.txt");
    let hard_link = directory.join("hard.txt");
    let symlink_path = directory.join("symlink.txt");
    let renamed = directory.join("renamed.txt");
    fs::create_dir(&directory).expect("directory is created");
    upload_file(
        &first_local,
        &uploaded,
        UploadOptions {
            preserve_metadata: false,
            synchronize: true,
        },
    )
    .expect("synchronized upload succeeds");
    fs::hard_link(&uploaded, &hard_link).expect("hard link is created");
    symlink(&uploaded, &symlink_path).expect("symbolic link is created");
    assert_eq!(
        fs::read_link(&symlink_path).expect("symbolic link target is read"),
        uploaded
    );
    fs::rename(&uploaded, &renamed).expect("uploaded file is renamed");
    assert_eq!(
        fs::read(&hard_link).expect("hard link remains readable"),
        b"fresh upload\n"
    );
    fs::remove_file(&renamed).expect("renamed file is removed");
    fs::remove_file(&hard_link).expect("hard link is removed");
    fs::remove_file(&symlink_path).expect("symbolic link is removed");
    fs::remove_dir(&directory).expect("directory is removed");
    assert!(
        !directory.exists(),
        "client-style directory workflow cleans up the directory"
    );
    assert_filesystem_statistics_are_readable(mount);
}

#[derive(Clone, Copy, Default)]
struct UploadOptions {
    preserve_metadata: bool,
    synchronize: bool,
}

fn upload_file(source: &Path, destination: &Path, options: UploadOptions) -> std::io::Result<()> {
    let bytes = fs::read(source)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(destination)?;
    file.write_all(&bytes)?;
    if options.synchronize {
        file.sync_all()?;
    }
    drop(file);

    if options.preserve_metadata {
        let metadata = fs::metadata(source)?;
        fs::set_permissions(
            destination,
            fs::Permissions::from_mode(metadata.permissions().mode() & 0o777),
        )?;
        set_file_times(destination, metadata.accessed()?, metadata.modified()?)?;
    }
    Ok(())
}

fn download_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    let bytes = fs::read(source)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(destination)?;
    file.write_all(&bytes)
}

fn assert_filesystem_statistics_are_readable(path: &Path) {
    let statistics = statvfs(path);
    assert_ne!(statistics.f_bsize, 0);
    assert_eq!(statistics.f_namemax, 255);
}

fn set_file_times(path: &Path, atime: SystemTime, mtime: SystemTime) -> std::io::Result<()> {
    let path = c_path(path);
    let times = [
        timespec_from_system_time(atime),
        timespec_from_system_time(mtime),
    ];
    let result = unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(last_os_error())
    }
}

fn timespec_from_system_time(time: SystemTime) -> libc::timespec {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch");
    libc::timespec {
        tv_sec: duration.as_secs() as libc::time_t,
        tv_nsec: duration.subsec_nanos() as libc::c_long,
    }
}

fn system_time_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch")
        .as_secs()
}

fn c_path(path: &Path) -> std::ffi::CString {
    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
