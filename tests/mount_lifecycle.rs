mod support;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::thread;
use std::time::{Duration, Instant};

use eventfs::{Filesystem, FilesystemError};

use support::{TestDirectories, configuration_for, list_all_events, open_test_filesystem};

#[test]
fn blocking_mount_returns_after_external_unmount_and_releases_mount_state() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mut blocking_mount = BlockingMountSession::spawn(filesystem.clone(), mount_point.clone());

    wait_for_branch_switch_state_or_mount_result(
        &filesystem,
        branch.name(),
        true,
        &mut blocking_mount,
        Duration::from_secs(10),
    );
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "blocking-mount-probe",
        Duration::from_secs(10),
    );
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );

    external_unmount_with_timeout(&mount_point, Duration::from_secs(10));

    assert_eq!(blocking_mount.finish(Duration::from_secs(10)), Ok(()));
    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    wait_for_branch_switch_state(&filesystem, branch.name(), false, Duration::from_secs(10));
    filesystem
        .switch_branch(branch.name())
        .expect("branch switch is allowed after unmount");
}

#[test]
fn background_mount_reports_explicit_unmount_failure_after_external_unmount() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "background-mount-probe",
        Duration::from_secs(10),
    );

    wait_for_branch_switch_state(&filesystem, branch.name(), true, Duration::from_secs(10));
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );

    external_unmount_with_timeout(&mount_point, Duration::from_secs(10));

    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    assert_eq!(mounted.unmount(), Err(FilesystemError::FilesystemOperation));
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );

    drop(filesystem);

    let reopened = Filesystem::open(directories.configuration()).expect("filesystem reopens");
    let remounted = reopened
        .spawn_mount()
        .expect("reopened filesystem mounts after the external unmount");
    wait_for_probe_path_state(&probe_path, true, Duration::from_secs(10));
    remounted
        .unmount()
        .expect("reopened filesystem unmounts explicitly");
}

#[test]
fn dropping_background_mount_after_external_unmount_keeps_mount_state_conservative() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "drop-after-external-unmount-probe",
        Duration::from_secs(10),
    );

    wait_for_branch_switch_state(&filesystem, branch.name(), true, Duration::from_secs(10));
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );

    external_unmount_with_timeout(&mount_point, Duration::from_secs(10));
    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));

    drop(mounted);

    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );

    drop(filesystem);

    let reopened = Filesystem::open(directories.configuration()).expect("filesystem reopens");
    let remounted = reopened
        .spawn_mount()
        .expect("reopened filesystem mounts after dropping the externally unmounted handle");
    wait_for_probe_path_state(&probe_path, true, Duration::from_secs(10));
    remounted
        .unmount()
        .expect("reopened filesystem unmounts explicitly");
}

#[test]
fn dropping_background_mount_unmounts_and_releases_mount_state() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "drop-background-mount-probe",
        Duration::from_secs(10),
    );

    wait_for_branch_switch_state(&filesystem, branch.name(), true, Duration::from_secs(10));

    drop(mounted);

    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    wait_for_branch_switch_state(&filesystem, branch.name(), false, Duration::from_secs(10));
    filesystem
        .switch_branch(branch.name())
        .expect("branch switch is allowed after dropping the background mount");

    let remounted = filesystem
        .spawn_mount()
        .expect("filesystem remounts after dropping the background mount");
    wait_for_probe_path_state(&probe_path, true, Duration::from_secs(10));
    remounted
        .unmount()
        .expect("remounted filesystem unmounts explicitly");
}

#[test]
fn duplicate_background_mount_failure_does_not_leak_mount_state() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "duplicate-background-mount-probe",
        Duration::from_secs(10),
    );

    wait_for_branch_switch_state(&filesystem, branch.name(), true, Duration::from_secs(10));

    let error = filesystem
        .spawn_mount()
        .expect_err("a duplicate background mount attempt is rejected");
    assert_eq!(error, FilesystemError::FilesystemOperation);
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );

    mounted
        .unmount()
        .expect("original background mount unmounts explicitly");
    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    wait_for_branch_switch_state(&filesystem, branch.name(), false, Duration::from_secs(10));
    filesystem
        .switch_branch(branch.name())
        .expect("branch switch is allowed after the surviving mount unmounts");

    let remounted = filesystem
        .spawn_mount()
        .expect("filesystem remounts after the duplicate mount failure");
    wait_for_probe_path_state(&probe_path, true, Duration::from_secs(10));
    remounted
        .unmount()
        .expect("remounted filesystem unmounts explicitly");
}

#[test]
fn duplicate_blocking_mount_failure_does_not_leak_mount_state() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "duplicate-blocking-mount-probe",
        Duration::from_secs(10),
    );

    wait_for_branch_switch_state(&filesystem, branch.name(), true, Duration::from_secs(10));

    let duplicate_mount = BlockingMountSession::spawn(filesystem.clone(), mount_point.clone());
    assert_eq!(
        duplicate_mount.finish(Duration::from_secs(10)),
        Err(FilesystemError::FilesystemOperation)
    );
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );
    assert_eq!(
        fs::read(&probe_path).expect("surviving mount keeps the probe file readable"),
        b"probe"
    );

    mounted
        .unmount()
        .expect("original background mount unmounts explicitly");
    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    wait_for_branch_switch_state(&filesystem, branch.name(), false, Duration::from_secs(10));
    filesystem
        .switch_branch(branch.name())
        .expect("branch switch is allowed after the surviving mount unmounts");

    let remounted = filesystem
        .spawn_mount()
        .expect("filesystem remounts after the duplicate blocking mount failure");
    wait_for_probe_path_state(&probe_path, true, Duration::from_secs(10));
    remounted
        .unmount()
        .expect("remounted filesystem unmounts explicitly");
}

#[test]
fn duplicate_background_mount_failure_during_blocking_mount_does_not_leak_mount_state() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mut blocking_mount = BlockingMountSession::spawn(filesystem.clone(), mount_point.clone());

    wait_for_branch_switch_state_or_mount_result(
        &filesystem,
        branch.name(),
        true,
        &mut blocking_mount,
        Duration::from_secs(10),
    );
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "duplicate-background-during-blocking-probe",
        Duration::from_secs(10),
    );

    let error = filesystem
        .spawn_mount()
        .expect_err("a duplicate background mount attempt is rejected while blocking mounted");
    assert_eq!(error, FilesystemError::FilesystemOperation);
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );
    assert_eq!(
        fs::read(&probe_path).expect("blocking mount keeps the probe file readable"),
        b"probe"
    );

    external_unmount_with_timeout(&mount_point, Duration::from_secs(10));

    assert_eq!(blocking_mount.finish(Duration::from_secs(10)), Ok(()));
    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    wait_for_branch_switch_state(&filesystem, branch.name(), false, Duration::from_secs(10));
    filesystem
        .switch_branch(branch.name())
        .expect("branch switch is allowed after the surviving blocking mount unmounts");

    let remounted = filesystem
        .spawn_mount()
        .expect("filesystem remounts after the duplicate background mount failure");
    wait_for_probe_path_state(&probe_path, true, Duration::from_secs(10));
    remounted
        .unmount()
        .expect("remounted filesystem unmounts explicitly");
}

#[test]
fn duplicate_blocking_mount_failure_during_blocking_mount_does_not_leak_mount_state() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");
    let mount_point = directories.mount_point_path().to_path_buf();
    let mut blocking_mount = BlockingMountSession::spawn(filesystem.clone(), mount_point.clone());

    wait_for_branch_switch_state_or_mount_result(
        &filesystem,
        branch.name(),
        true,
        &mut blocking_mount,
        Duration::from_secs(10),
    );
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "duplicate-blocking-during-blocking-probe",
        Duration::from_secs(10),
    );

    let duplicate_mount = BlockingMountSession::spawn(filesystem.clone(), mount_point.clone());
    assert_eq!(
        duplicate_mount.finish(Duration::from_secs(10)),
        Err(FilesystemError::FilesystemOperation)
    );
    assert_eq!(
        filesystem.switch_branch(branch.name()),
        Err(FilesystemError::FilesystemOperation)
    );
    assert_eq!(
        fs::read(&probe_path).expect("blocking mount keeps the probe file readable"),
        b"probe"
    );

    external_unmount_with_timeout(&mount_point, Duration::from_secs(10));

    assert_eq!(blocking_mount.finish(Duration::from_secs(10)), Ok(()));
    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    wait_for_branch_switch_state(&filesystem, branch.name(), false, Duration::from_secs(10));
    filesystem
        .switch_branch(branch.name())
        .expect("branch switch is allowed after the surviving blocking mount unmounts");

    let remounted = filesystem
        .spawn_mount()
        .expect("filesystem remounts after the duplicate blocking mount failure");
    wait_for_probe_path_state(&probe_path, true, Duration::from_secs(10));
    remounted
        .unmount()
        .expect("remounted filesystem unmounts explicitly");
}

#[test]
fn blocking_mount_rejects_missing_mount_point_and_allows_later_mount() {
    let directories = TestDirectories::new();
    let mount_point = directories.root_path().join("mount-created-later");
    let filesystem = Filesystem::open(configuration_for(
        directories.database_directory_path().to_path_buf(),
        mount_point.clone(),
    ))
    .expect("filesystem opens");
    let branch = filesystem
        .current_branch()
        .expect("current branch is returned");

    assert_eq!(
        filesystem.mount(),
        Err(FilesystemError::FilesystemOperation)
    );
    filesystem
        .switch_branch(branch.name())
        .expect("failed blocking mount does not leave the filesystem mounted");

    fs::create_dir(&mount_point).expect("mount point is created after the initial failure");

    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts after the mount point appears");
    let probe_path = create_probe_file_through_mount(
        &filesystem,
        &mount_point,
        "late-mount-point-probe",
        Duration::from_secs(10),
    );

    wait_for_branch_switch_state(&filesystem, branch.name(), true, Duration::from_secs(10));
    assert_eq!(
        fs::read(&probe_path).expect("probe file is readable through the later mount"),
        b"probe"
    );

    mounted
        .unmount()
        .expect("later mounted filesystem unmounts explicitly");
    wait_for_probe_path_state(&probe_path, false, Duration::from_secs(10));
    wait_for_branch_switch_state(&filesystem, branch.name(), false, Duration::from_secs(10));
}

#[test]
fn rocksdb_open_failures_map_to_database_errors() {
    let directories = TestDirectories::new();
    let database_path = directories.root_path().join("not-a-directory");
    fs::write(&database_path, b"file").expect("database path file is written");

    let error = Filesystem::open(configuration_for(
        database_path,
        directories.root_path().join("database-error-mount"),
    ))
    .expect_err("database path file is rejected");

    assert_eq!(error, FilesystemError::Database);
}

struct BlockingMountSession {
    join_handle: Option<thread::JoinHandle<Result<(), FilesystemError>>>,
    mount_point: PathBuf,
    result: Option<Result<(), FilesystemError>>,
}

impl BlockingMountSession {
    fn spawn(filesystem: Filesystem, mount_point: PathBuf) -> Self {
        Self {
            join_handle: Some(thread::spawn(move || filesystem.mount())),
            mount_point,
            result: None,
        }
    }

    fn finish(mut self, timeout: Duration) -> Result<(), FilesystemError> {
        let deadline = Instant::now() + timeout;
        while self.poll_result().is_none() {
            if Instant::now() >= deadline {
                panic!("timed out waiting for the blocking mount thread to finish");
            }
            thread::sleep(Duration::from_millis(50));
        }
        self.result
            .take()
            .expect("blocking mount thread result is available")
    }

    fn poll_result(&mut self) -> Option<&Result<(), FilesystemError>> {
        if self.result.is_none()
            && self
                .join_handle
                .as_ref()
                .is_some_and(thread::JoinHandle::is_finished)
        {
            let join_handle = self
                .join_handle
                .take()
                .expect("blocking mount join handle is present");
            self.result = Some(
                join_handle
                    .join()
                    .expect("blocking mount thread finishes without panicking"),
            );
        }
        self.result.as_ref()
    }
}

impl Drop for BlockingMountSession {
    fn drop(&mut self) {
        if self.join_handle.is_none() {
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.poll_result().is_none() {
            if Instant::now() >= deadline {
                return;
            }
            let _ = try_external_unmount_with_timeout(&self.mount_point, Duration::from_secs(1));
            thread::sleep(Duration::from_millis(50));
        }
    }
}

fn wait_for_branch_switch_state_or_mount_result(
    filesystem: &Filesystem,
    branch_name: &eventfs::BranchName,
    mounted: bool,
    blocking_mount: &mut BlockingMountSession,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if branch_switch_is_blocked(filesystem, branch_name) == mounted {
            return;
        }
        if let Some(result) = blocking_mount.poll_result() {
            panic!("mount finished before reaching requested state: {result:?}");
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for branch switching to become {}",
                if mounted { "mounted" } else { "unmounted" }
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_branch_switch_state(
    filesystem: &Filesystem,
    branch_name: &eventfs::BranchName,
    mounted: bool,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    while branch_switch_is_blocked(filesystem, branch_name) != mounted {
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for branch switching to become {}",
                if mounted { "mounted" } else { "unmounted" }
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn create_probe_file_through_mount(
    filesystem: &Filesystem,
    mount_point: &Path,
    name_prefix: &str,
    timeout: Duration,
) -> PathBuf {
    let initial_events = list_all_events(filesystem).len();
    let deadline = Instant::now() + timeout;

    for attempt in 0_u64.. {
        let probe_path = mount_point.join(format!("{name_prefix}-{attempt}"));
        if fs::write(&probe_path, b"probe").is_ok()
            && list_all_events(filesystem).len() > initial_events
        {
            assert_eq!(
                fs::read(&probe_path).expect("probe file is readable through the mount"),
                b"probe"
            );
            return probe_path;
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for path operations at {} to route through the mount",
                mount_point.display()
            );
        }
        thread::sleep(Duration::from_millis(50));
    }

    unreachable!("the loop either returns the probe path or times out")
}

fn wait_for_probe_path_state(path: &Path, visible: bool, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while fs::metadata(path).is_ok() != visible {
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for probe path {} to become {}",
                path.display(),
                if visible { "visible" } else { "hidden" }
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn branch_switch_is_blocked(filesystem: &Filesystem, branch_name: &eventfs::BranchName) -> bool {
    filesystem.switch_branch(branch_name) == Err(FilesystemError::FilesystemOperation)
}

fn external_unmount_with_timeout(mount_point: &Path, timeout: Duration) {
    try_external_unmount_with_timeout(mount_point, timeout)
        .expect("external unmount succeeds within the timeout");
}

fn try_external_unmount_with_timeout(mount_point: &Path, timeout: Duration) -> io::Result<()> {
    let mut last_status = None;
    for program in ["fusermount3", "fusermount", "umount"] {
        let mut command = Command::new(program);
        if program.starts_with("fuse") {
            command.arg("-u");
        }
        command.arg(mount_point);
        match run_command_with_timeout(&mut command, timeout) {
            Ok(status) if status.success() => return Ok(()),
            Ok(status) => last_status = Some(format!("{program} exited with {status}")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::other(format!(
        "external unmount failed: {}",
        last_status.unwrap_or_else(|| "no unmount helper was available".to_owned())
    )))
}

fn run_command_with_timeout(command: &mut Command, timeout: Duration) -> io::Result<ExitStatus> {
    let mut child = command.spawn()?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("command timed out: {command:?}"),
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}
