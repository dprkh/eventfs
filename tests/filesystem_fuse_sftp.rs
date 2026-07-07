#[cfg(target_os = "linux")]
mod support;

#[cfg(target_os = "linux")]
use std::env;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::process::{Command, Output};
#[cfg(target_os = "linux")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "linux")]
use support::{TestDirectories, mount, open_test_filesystem};

#[cfg(target_os = "linux")]
#[test]
fn openssh_sftp_workflows_use_mounted_filesystem_successfully() {
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

    run_sftp_batch(
        mount,
        root.join("fresh.batch"),
        &[
            format!("put {} fresh.txt", first_local.display()),
            format!("get fresh.txt {}", downloaded.display()),
            "df".to_owned(),
            "bye".to_owned(),
        ],
    );
    assert_eq!(
        fs::read(mount.join("fresh.txt")).expect("fresh upload is readable"),
        b"fresh upload\n"
    );
    assert_eq!(
        fs::read(&downloaded).expect("downloaded file is readable"),
        b"fresh upload\n"
    );

    run_sftp_batch(
        mount,
        root.join("overwrite.batch"),
        &[
            format!("put {} fresh.txt", second_local.display()),
            "bye".to_owned(),
        ],
    );
    assert_eq!(
        fs::read(mount.join("fresh.txt")).expect("overwritten upload is readable"),
        b"xy\n"
    );

    run_sftp_batch(
        mount,
        root.join("metadata.batch"),
        &[
            format!("put -p {} preserved.txt", preserved_local.display()),
            "mkdir dir".to_owned(),
            format!("put -f {} dir/uploaded.txt", first_local.display()),
            "ln dir/uploaded.txt dir/hard.txt".to_owned(),
            "ln -s dir/uploaded.txt dir/symlink.txt".to_owned(),
            "rename dir/uploaded.txt dir/renamed.txt".to_owned(),
            "rm dir/renamed.txt".to_owned(),
            "rm dir/hard.txt".to_owned(),
            "rm dir/symlink.txt".to_owned(),
            "rmdir dir".to_owned(),
            "df".to_owned(),
            "bye".to_owned(),
        ],
    );
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
    assert!(
        !mount.join("dir").exists(),
        "SFTP directory workflow cleans up the directory"
    );
}

#[cfg(target_os = "linux")]
fn run_sftp_batch(mount: &Path, batch: PathBuf, commands: &[String]) {
    fs::write(&batch, commands.join("\n") + "\n").expect("SFTP batch file is written");
    let output = Command::new(sftp_client())
        .arg("-q")
        .arg("-D")
        .arg(format!(
            "{} -d {}",
            sftp_server().display(),
            mount.display()
        ))
        .arg("-b")
        .arg(&batch)
        .arg("dummy")
        .output()
        .expect("SFTP client runs");
    assert_success(output);
}

#[cfg(target_os = "linux")]
fn sftp_client() -> PathBuf {
    executable(
        "sftp",
        &[PathBuf::from("/usr/bin/sftp"), PathBuf::from("/bin/sftp")],
    )
}

#[cfg(target_os = "linux")]
fn sftp_server() -> PathBuf {
    executable(
        "sftp-server",
        &[
            PathBuf::from("/usr/lib/openssh/sftp-server"),
            PathBuf::from("/usr/libexec/openssh/sftp-server"),
            PathBuf::from("/usr/libexec/sftp-server"),
            PathBuf::from("/usr/lib/ssh/sftp-server"),
        ],
    )
}

#[cfg(target_os = "linux")]
fn executable(name: &str, candidates: &[PathBuf]) -> PathBuf {
    candidates
        .iter()
        .find(|path| path.is_file())
        .cloned()
        .or_else(|| executable_from_path(name))
        .unwrap_or_else(|| panic!("required executable `{name}` was not found"))
}

#[cfg(target_os = "linux")]
fn executable_from_path(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|path| path.join(name))
            .find(|path| path.is_file())
    })
}

#[cfg(target_os = "linux")]
fn assert_success(output: Output) {
    assert!(
        output.status.success(),
        "SFTP command failed\nstdout:\n{}\nstderr:\n{}",
        output_text(output.stdout),
        output_text(output.stderr)
    );
}

#[cfg(target_os = "linux")]
fn output_text(bytes: Vec<u8>) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
fn timespec_from_system_time(time: SystemTime) -> libc::timespec {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch");
    libc::timespec {
        tv_sec: duration.as_secs() as libc::time_t,
        tv_nsec: duration.subsec_nanos() as libc::c_long,
    }
}

#[cfg(target_os = "linux")]
fn system_time_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .expect("timestamp is after the unix epoch")
        .as_secs()
}

#[cfg(target_os = "linux")]
fn c_path(path: &Path) -> std::ffi::CString {
    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
}

#[cfg(target_os = "linux")]
fn last_os_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
