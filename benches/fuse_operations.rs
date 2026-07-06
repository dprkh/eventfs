#[cfg(not(target_os = "linux"))]
compile_error!("eventfs fuse_operations benchmark is Linux-only");

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::CString;
    use std::fs::{self, OpenOptions};
    use std::hint::black_box;
    use std::io::{self, ErrorKind};
    use std::mem::MaybeUninit;
    use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileExt, symlink};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use criterion::measurement::WallTime;
    use criterion::{BenchmarkGroup, BenchmarkId, Criterion};
    use eventfs::{Filesystem, FilesystemConfiguration, MountedFilesystem};
    use tempfile::TempDir;

    const BENCHMARK_SAMPLE_SIZE: usize = 10;
    const BENCHMARK_WARM_UP_MS: u64 = 50;
    const BENCHMARK_MEASUREMENT_MS: u64 = 200;
    const BENCHMARK_BLOCK_SIZE: usize = 4096;
    const BENCHMARK_DIRECTORY_ENTRY_COUNT: usize = 16;
    const BENCHMARK_UNSUPPORTED_IOCTL_COMMAND: libc::Ioctl = 0;
    const BENCHMARK_XATTR_VALUE: &[u8] = b"value";

    pub(crate) fn criterion_configuration() -> Criterion {
        Criterion::default()
            .sample_size(BENCHMARK_SAMPLE_SIZE)
            .warm_up_time(Duration::from_millis(BENCHMARK_WARM_UP_MS))
            .measurement_time(Duration::from_millis(BENCHMARK_MEASUREMENT_MS))
    }

    pub(crate) fn bench_fuse_operations(criterion: &mut Criterion) {
        let fixture = FuseBenchmarkFixture::new().expect("benchmark fixture is created");
        let mut group = criterion.benchmark_group("fuse_operations");

        benchmark_metadata_operations(&mut group, &fixture);
        benchmark_node_operations(&mut group, &fixture);
        benchmark_file_operations(&mut group, &fixture);
        benchmark_directory_operations(&mut group, &fixture);
        benchmark_extended_operations(&mut group, &fixture);

        group.finish();
    }

    struct FuseBenchmarkFixture {
        root: TempDir,
        mounted: Option<MountedFilesystem>,
        eventfs_root: PathBuf,
        host_root: PathBuf,
    }

    impl FuseBenchmarkFixture {
        fn new() -> io::Result<Self> {
            let root = tempfile::tempdir()?;
            let database_directory = root.path().join("database");
            let mount_point = root.path().join("mount");
            let host_root = root.path().join("host");
            fs::create_dir(&mount_point)?;
            fs::create_dir(&host_root)?;

            let configuration = FilesystemConfiguration::new(&database_directory, &mount_point)
                .map_err(configuration_error)?;
            let filesystem = Filesystem::open(configuration).map_err(filesystem_error)?;
            let mounted = filesystem.spawn_mount().map_err(filesystem_error)?;

            Ok(Self {
                root,
                mounted: Some(mounted),
                eventfs_root: mount_point,
                host_root,
            })
        }

        fn roots(&self) -> (PathBuf, PathBuf) {
            (self.eventfs_root.clone(), self.host_root.clone())
        }
    }

    impl Drop for FuseBenchmarkFixture {
        fn drop(&mut self) {
            if let Some(mounted) = self.mounted.take() {
                let _ = mounted.unmount();
            }
            let _ = self.root.path();
        }
    }

    fn benchmark_metadata_operations(
        group: &mut BenchmarkGroup<'_, WallTime>,
        fixture: &FuseBenchmarkFixture,
    ) {
        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "lookup-file", b"lookup")
            .expect("lookup files are prepared");
        benchmark_pair(
            group,
            "lookup",
            repeated_existing_path(eventfs_root.join("lookup-file"), metadata_path),
            repeated_existing_path(host_root.join("lookup-file"), metadata_path),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "getattr-file", b"getattr")
            .expect("getattr files are prepared");
        benchmark_pair(
            group,
            "getattr",
            repeated_file_metadata(eventfs_root.join("getattr-file")),
            repeated_file_metadata(host_root.join("getattr-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "access-file", b"access")
            .expect("access files are prepared");
        benchmark_pair(
            group,
            "access",
            repeated_existing_path(eventfs_root.join("access-file"), access_path),
            repeated_existing_path(host_root.join("access-file"), access_path),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "statfs",
            repeated_existing_path(eventfs_root, statvfs_path),
            repeated_existing_path(host_root, statvfs_path),
        );
    }

    fn benchmark_node_operations(
        group: &mut BenchmarkGroup<'_, WallTime>,
        fixture: &FuseBenchmarkFixture,
    ) {
        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "mknod",
            repeated_unique_path(eventfs_root, "mknod", |path, _index| {
                timed(|| mkfifo_path(path))
            }),
            repeated_unique_path(host_root, "mknod", |path, _index| {
                timed(|| mkfifo_path(path))
            }),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "mkdir",
            repeated_unique_path(eventfs_root, "mkdir", |path, _index| {
                timed(|| fs::create_dir(path))
            }),
            repeated_unique_path(host_root, "mkdir", |path, _index| {
                timed(|| fs::create_dir(path))
            }),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "create",
            repeated_create(eventfs_root),
            repeated_create(host_root),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "unlink",
            repeated_mutating_file(eventfs_root, "unlink", |path, _index| {
                timed(|| fs::remove_file(path))
            }),
            repeated_mutating_file(host_root, "unlink", |path, _index| {
                timed(|| fs::remove_file(path))
            }),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "rmdir",
            repeated_mutating_directory(eventfs_root, "rmdir", |path, _index| {
                timed(|| fs::remove_dir(path))
            }),
            repeated_mutating_directory(host_root, "rmdir", |path, _index| {
                timed(|| fs::remove_dir(path))
            }),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "rename",
            repeated_rename(eventfs_root, false),
            repeated_rename(host_root, false),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "rename_noreplace",
            repeated_rename(eventfs_root, true),
            repeated_rename(host_root, true),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "hard-link-source", b"link")
            .expect("hard link source files are prepared");
        benchmark_pair(
            group,
            "link",
            repeated_hard_link(eventfs_root.join("hard-link-source"), eventfs_root, "link"),
            repeated_hard_link(host_root.join("hard-link-source"), host_root, "link"),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "symlink-target", b"symlink")
            .expect("symlink targets are prepared");
        benchmark_pair(
            group,
            "symlink",
            repeated_symlink(eventfs_root.join("symlink-target"), eventfs_root),
            repeated_symlink(host_root.join("symlink-target"), host_root),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_symlink_pair(&eventfs_root, &host_root).expect("readlink symlinks are prepared");
        benchmark_pair(
            group,
            "readlink",
            repeated_existing_path(eventfs_root.join("readlink-link"), readlink_path),
            repeated_existing_path(host_root.join("readlink-link"), readlink_path),
        );
    }

    fn benchmark_file_operations(
        group: &mut BenchmarkGroup<'_, WallTime>,
        fixture: &FuseBenchmarkFixture,
    ) {
        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "open-file", b"open")
            .expect("open files are prepared");
        benchmark_pair(
            group,
            "open",
            repeated_open(eventfs_root.join("open-file")),
            repeated_open(host_root.join("open-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(
            &eventfs_root,
            &host_root,
            "read-file",
            &vec![b'a'; BENCHMARK_BLOCK_SIZE],
        )
        .expect("read files are prepared");
        benchmark_pair(
            group,
            "read",
            repeated_read_at(eventfs_root.join("read-file")),
            repeated_read_at(host_root.join("read-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(
            &eventfs_root,
            &host_root,
            "write-file",
            &vec![b'a'; BENCHMARK_BLOCK_SIZE],
        )
        .expect("write files are prepared");
        benchmark_pair(
            group,
            "write",
            repeated_write_at(eventfs_root.join("write-file")),
            repeated_write_at(host_root.join("write-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "flush-file", b"flush")
            .expect("flush files are prepared");
        benchmark_pair(
            group,
            "flush",
            repeated_flush(eventfs_root.join("flush-file")),
            repeated_flush(host_root.join("flush-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "release",
            repeated_release(eventfs_root),
            repeated_release(host_root),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "fsync-file", b"fsync")
            .expect("fsync files are prepared");
        benchmark_pair(
            group,
            "fsync",
            repeated_sync_file(eventfs_root.join("fsync-file")),
            repeated_sync_file(host_root.join("fsync-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "copy_file_range",
            repeated_copy_file_range(eventfs_root),
            repeated_copy_file_range(host_root),
        );
    }

    fn benchmark_directory_operations(
        group: &mut BenchmarkGroup<'_, WallTime>,
        fixture: &FuseBenchmarkFixture,
    ) {
        let (eventfs_root, host_root) = fixture.roots();
        prepare_directory_pair(&eventfs_root, &host_root, "directory")
            .expect("directory benchmark trees are prepared");
        benchmark_pair(
            group,
            "opendir",
            repeated_opendir(eventfs_root.join("directory")),
            repeated_opendir(host_root.join("directory")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "readdir",
            repeated_readdir(eventfs_root.join("directory")),
            repeated_readdir(host_root.join("directory")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "readdirplus",
            repeated_existing_path(eventfs_root.join("directory"), readdirplus_directory),
            repeated_existing_path(host_root.join("directory"), readdirplus_directory),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "fsyncdir",
            repeated_fsync_directory(eventfs_root.join("directory")),
            repeated_fsync_directory(host_root.join("directory")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "releasedir",
            repeated_releasedir(eventfs_root.join("directory")),
            repeated_releasedir(host_root.join("directory")),
        );
    }

    fn benchmark_extended_operations(
        group: &mut BenchmarkGroup<'_, WallTime>,
        fixture: &FuseBenchmarkFixture,
    ) {
        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "xattr-file", b"xattr")
            .expect("xattr files are prepared");
        benchmark_pair(
            group,
            "setxattr",
            repeated_setxattr(eventfs_root.join("xattr-file")),
            repeated_setxattr(host_root.join("xattr-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_xattr_pair(&eventfs_root, &host_root).expect("xattrs are prepared");
        benchmark_pair(
            group,
            "getxattr",
            repeated_existing_path(eventfs_root.join("xattr-read-file"), getxattr_path),
            repeated_existing_path(host_root.join("xattr-read-file"), getxattr_path),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "listxattr",
            repeated_existing_path(eventfs_root.join("xattr-read-file"), listxattr_path),
            repeated_existing_path(host_root.join("xattr-read-file"), listxattr_path),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "removexattr",
            repeated_removexattr(eventfs_root.join("xattr-file")),
            repeated_removexattr(host_root.join("xattr-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "lock-file", b"locks")
            .expect("lock files are prepared");
        benchmark_pair(
            group,
            "getlk",
            repeated_lock(eventfs_root.join("lock-file"), libc::F_GETLK),
            repeated_lock(host_root.join("lock-file"), libc::F_GETLK),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "setlk",
            repeated_setlk(eventfs_root.join("lock-file")),
            repeated_setlk(host_root.join("lock-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_file_pair(&eventfs_root, &host_root, "ioctl-file", b"ioctl")
            .expect("ioctl files are prepared");
        benchmark_pair(
            group,
            "bmap",
            repeated_bmap(eventfs_root.join("ioctl-file")),
            repeated_bmap(host_root.join("ioctl-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "ioctl",
            repeated_ioctl_rejection(eventfs_root.join("ioctl-file")),
            repeated_ioctl_rejection(host_root.join("ioctl-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "poll",
            repeated_poll(eventfs_root.join("ioctl-file")),
            repeated_poll(host_root.join("ioctl-file")),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "fallocate_extend",
            repeated_fallocate(eventfs_root, "fallocate-extend", 0),
            repeated_fallocate(host_root, "fallocate-extend", 0),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "fallocate_keep_size",
            repeated_fallocate(
                eventfs_root,
                "fallocate-keep-size",
                libc::FALLOC_FL_KEEP_SIZE,
            ),
            repeated_fallocate(host_root, "fallocate-keep-size", libc::FALLOC_FL_KEEP_SIZE),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "fallocate_punch_hole",
            repeated_fallocate(
                eventfs_root,
                "fallocate-punch-hole",
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            ),
            repeated_fallocate(
                host_root,
                "fallocate-punch-hole",
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            ),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "fallocate_zero_range",
            repeated_fallocate(
                eventfs_root,
                "fallocate-zero-range",
                libc::FALLOC_FL_ZERO_RANGE,
            ),
            repeated_fallocate(
                host_root,
                "fallocate-zero-range",
                libc::FALLOC_FL_ZERO_RANGE,
            ),
        );

        let (eventfs_root, host_root) = fixture.roots();
        prepare_sparse_pair(&eventfs_root, &host_root).expect("sparse files are prepared");
        benchmark_pair(
            group,
            "lseek_data",
            repeated_lseek(eventfs_root.join("sparse-file"), libc::SEEK_DATA),
            repeated_lseek(host_root.join("sparse-file"), libc::SEEK_DATA),
        );

        let (eventfs_root, host_root) = fixture.roots();
        benchmark_pair(
            group,
            "lseek_hole",
            repeated_lseek(eventfs_root.join("sparse-file"), libc::SEEK_HOLE),
            repeated_lseek(host_root.join("sparse-file"), libc::SEEK_HOLE),
        );
    }

    fn benchmark_pair<Eventfs, Host>(
        group: &mut BenchmarkGroup<'_, WallTime>,
        operation: &'static str,
        mut eventfs: Eventfs,
        mut host: Host,
    ) where
        Eventfs: FnMut(u64) -> io::Result<Duration> + 'static,
        Host: FnMut(u64) -> io::Result<Duration> + 'static,
    {
        group.bench_function(BenchmarkId::new(operation, "eventfs"), move |bencher| {
            bencher.iter_custom(|iterations| {
                eventfs(iterations)
                    .unwrap_or_else(|error| panic!("{operation} eventfs benchmark failed: {error}"))
            });
        });
        group.bench_function(BenchmarkId::new(operation, "host"), move |bencher| {
            bencher.iter_custom(|iterations| {
                host(iterations)
                    .unwrap_or_else(|error| panic!("{operation} host benchmark failed: {error}"))
            });
        });
    }

    fn repeated_existing_path(
        path: PathBuf,
        operation: fn(&Path) -> io::Result<()>,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| measure_repeated(iterations, || operation(&path))
    }

    fn repeated_unique_path(
        root: PathBuf,
        operation: &'static str,
        mut run: impl FnMut(&Path, u64) -> io::Result<Duration> + 'static,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let path = iteration_path(&root, operation, index, "node");
                elapsed += run(&path, index)?;
                cleanup_path(&path)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_mutating_file(
        root: PathBuf,
        operation: &'static str,
        mut run: impl FnMut(&Path, u64) -> io::Result<Duration> + 'static,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let path = iteration_path(&root, operation, index, "file");
                fs::write(&path, b"contents")?;
                elapsed += run(&path, index)?;
                cleanup_path(&path)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_mutating_directory(
        root: PathBuf,
        operation: &'static str,
        mut run: impl FnMut(&Path, u64) -> io::Result<Duration> + 'static,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let path = iteration_path(&root, operation, index, "directory");
                fs::create_dir(&path)?;
                elapsed += run(&path, index)?;
                cleanup_path(&path)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_rename(root: PathBuf, no_replace: bool) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let source = iteration_path(&root, "rename", index, "source");
                let destination = iteration_path(&root, "rename", index, "destination");
                fs::write(&source, b"rename")?;
                elapsed += if no_replace {
                    timed(|| rename_noreplace_path(&source, &destination))?
                } else {
                    timed(|| fs::rename(&source, &destination))?
                };
                cleanup_path(&source)?;
                cleanup_path(&destination)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_hard_link(
        source: PathBuf,
        root: PathBuf,
        operation: &'static str,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let link = iteration_path(&root, operation, index, "hard-link");
                elapsed += timed(|| fs::hard_link(&source, &link))?;
                cleanup_path(&link)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_symlink(target: PathBuf, root: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let link = iteration_path(&root, "symlink", index, "symlink");
                elapsed += timed(|| symlink(&target, &link))?;
                cleanup_path(&link)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_file_metadata(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("getattr file opens");
        move |iterations| measure_repeated(iterations, || metadata_file(&file))
    }

    fn repeated_create(root: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let path = iteration_path(&root, "create", index, "file");
                let start = Instant::now();
                let file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)?;
                elapsed += start.elapsed();
                drop(file);
                cleanup_path(&path)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_open(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let start = Instant::now();
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                elapsed += start.elapsed();
                drop(file);
            }
            Ok(elapsed)
        }
    }

    fn repeated_read_at(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("read file opens");
        let mut buffer = vec![0; BENCHMARK_BLOCK_SIZE];
        move |iterations| {
            measure_repeated(iterations, || {
                let read = file.read_at(&mut buffer, 0)?;
                expect_byte_count(read, BENCHMARK_BLOCK_SIZE, "read")?;
                black_box(&buffer);
                Ok(())
            })
        }
    }

    fn repeated_write_at(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("write file opens");
        let first = vec![b'a'; BENCHMARK_BLOCK_SIZE];
        let second = vec![b'b'; BENCHMARK_BLOCK_SIZE];
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let bytes = if index.is_multiple_of(2) {
                    first.as_slice()
                } else {
                    second.as_slice()
                };
                elapsed += timed(|| {
                    let written = file.write_at(bytes, 0)?;
                    expect_byte_count(written, bytes.len(), "write")
                })?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_flush(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("flush file opens");
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let duplicated = duplicate_fd(file.as_raw_fd())?;
                elapsed += timed(|| close_fd(duplicated))?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_release(root: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let path = iteration_path(&root, "release", index, "file");
                fs::write(&path, b"release")?;
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                let fd = file.into_raw_fd();
                elapsed += timed(|| close_fd(fd))?;
                cleanup_path(&path)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_sync_file(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("fsync file opens");
        move |iterations| measure_repeated(iterations, || file.sync_all())
    }

    fn repeated_copy_file_range(root: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let source = root.join("copy-source");
        fs::write(&source, vec![b'c'; BENCHMARK_BLOCK_SIZE]).expect("copy source is prepared");
        let source_file = OpenOptions::new()
            .read(true)
            .open(&source)
            .expect("copy source opens");
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let destination = iteration_path(&root, "copy-file-range", index, "destination");
                fs::write(&destination, vec![0; BENCHMARK_BLOCK_SIZE])?;
                let destination_file = OpenOptions::new().write(true).open(&destination)?;
                elapsed += timed(|| {
                    copy_file_range_fd(source_file.as_raw_fd(), destination_file.as_raw_fd())
                })?;
                cleanup_path(&destination)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_opendir(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let start = Instant::now();
                let directory = open_directory(&path)?;
                elapsed += start.elapsed();
                closedir(directory)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_readdir(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let directory = open_directory(&path)?;
                let start = Instant::now();
                let count = read_directory_entries(directory);
                elapsed += start.elapsed();
                closedir(directory)?;
                black_box(count);
            }
            Ok(elapsed)
        }
    }

    fn repeated_fsync_directory(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let fd = open_directory_fd(&path)?;
                elapsed += timed(|| syscall_zero(unsafe { libc::fsync(fd) }))?;
                close_fd(fd)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_releasedir(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let fd = open_directory_fd(&path)?;
                elapsed += timed(|| close_fd(fd))?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_setxattr(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let name = xattr_name("set", index);
                elapsed += timed(|| setxattr_path(&path, &name, BENCHMARK_XATTR_VALUE, 0))?;
                let _ = removexattr_path(&path, &name);
            }
            Ok(elapsed)
        }
    }

    fn repeated_removexattr(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let name = xattr_name("remove", index);
                setxattr_path(&path, &name, BENCHMARK_XATTR_VALUE, 0)?;
                elapsed += timed(|| removexattr_path(&path, &name))?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_lock(
        path: PathBuf,
        command: libc::c_int,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("lock file opens");
        move |iterations| measure_repeated(iterations, || fcntl_lock(file.as_raw_fd(), command))
    }

    fn repeated_setlk(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("setlk file opens");
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                elapsed += timed(|| fcntl_lock(file.as_raw_fd(), libc::F_SETLK))?;
                unlock_file(file.as_raw_fd())?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_bmap(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("bmap file opens");
        move |iterations| measure_repeated(iterations, || bmap_fd(file.as_raw_fd()))
    }

    fn repeated_ioctl_rejection(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("ioctl file opens");
        move |iterations| measure_repeated(iterations, || ioctl_rejection_fd(file.as_raw_fd()))
    }

    fn repeated_poll(path: PathBuf) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("poll file opens");
        move |iterations| measure_repeated(iterations, || poll_fd(file.as_raw_fd()))
    }

    fn repeated_fallocate(
        root: PathBuf,
        operation: &'static str,
        mode: libc::c_int,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        move |iterations| {
            let mut elapsed = Duration::ZERO;
            for index in 0..iterations {
                let path = iteration_path(&root, operation, index, "file");
                fs::write(&path, vec![b'f'; BENCHMARK_BLOCK_SIZE * 2])?;
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                elapsed += timed(|| fallocate_fd(file.as_raw_fd(), mode))?;
                cleanup_path(&path)?;
            }
            Ok(elapsed)
        }
    }

    fn repeated_lseek(
        path: PathBuf,
        whence: libc::c_int,
    ) -> impl FnMut(u64) -> io::Result<Duration> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("lseek file opens");
        move |iterations| measure_repeated(iterations, || lseek_fd(file.as_raw_fd(), whence))
    }

    fn measure_repeated(
        iterations: u64,
        mut operation: impl FnMut() -> io::Result<()>,
    ) -> io::Result<Duration> {
        let mut elapsed = Duration::ZERO;
        for _ in 0..iterations {
            elapsed += timed(&mut operation)?;
        }
        Ok(elapsed)
    }

    fn timed(mut operation: impl FnMut() -> io::Result<()>) -> io::Result<Duration> {
        let start = Instant::now();
        operation()?;
        Ok(start.elapsed())
    }

    fn prepare_file_pair(
        eventfs_root: &Path,
        host_root: &Path,
        name: &str,
        bytes: &[u8],
    ) -> io::Result<()> {
        fs::write(eventfs_root.join(name), bytes)?;
        fs::write(host_root.join(name), bytes)
    }

    fn prepare_directory_pair(eventfs_root: &Path, host_root: &Path, name: &str) -> io::Result<()> {
        prepare_directory(eventfs_root, name)?;
        prepare_directory(host_root, name)
    }

    fn prepare_directory(root: &Path, name: &str) -> io::Result<()> {
        let directory = root.join(name);
        fs::create_dir(&directory)?;
        for index in 0..BENCHMARK_DIRECTORY_ENTRY_COUNT {
            fs::write(directory.join(format!("entry-{index:02}")), b"entry")?;
        }
        Ok(())
    }

    fn prepare_symlink_pair(eventfs_root: &Path, host_root: &Path) -> io::Result<()> {
        fs::write(eventfs_root.join("readlink-target"), b"target")?;
        fs::write(host_root.join("readlink-target"), b"target")?;
        symlink(
            eventfs_root.join("readlink-target"),
            eventfs_root.join("readlink-link"),
        )?;
        symlink(
            host_root.join("readlink-target"),
            host_root.join("readlink-link"),
        )
    }

    fn prepare_xattr_pair(eventfs_root: &Path, host_root: &Path) -> io::Result<()> {
        fs::write(eventfs_root.join("xattr-read-file"), b"xattr")?;
        fs::write(host_root.join("xattr-read-file"), b"xattr")?;
        setxattr_path(
            &eventfs_root.join("xattr-read-file"),
            "user.eventfs.benchmark.read",
            BENCHMARK_XATTR_VALUE,
            0,
        )?;
        setxattr_path(
            &host_root.join("xattr-read-file"),
            "user.eventfs.benchmark.read",
            BENCHMARK_XATTR_VALUE,
            0,
        )
    }

    fn prepare_sparse_pair(eventfs_root: &Path, host_root: &Path) -> io::Result<()> {
        prepare_sparse_file(&eventfs_root.join("sparse-file"))?;
        prepare_sparse_file(&host_root.join("sparse-file"))
    }

    fn prepare_sparse_file(path: &Path) -> io::Result<()> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.write_at(b"data", BENCHMARK_BLOCK_SIZE as u64)?;
        let written = file.write_at(&[0], (BENCHMARK_BLOCK_SIZE * 2 - 1) as u64)?;
        expect_byte_count(written, 1, "sparse extension")
    }

    fn metadata_path(path: &Path) -> io::Result<()> {
        black_box(fs::metadata(path)?);
        Ok(())
    }

    fn metadata_file(file: &fs::File) -> io::Result<()> {
        black_box(file.metadata()?);
        Ok(())
    }

    fn access_path(path: &Path) -> io::Result<()> {
        let path = c_path(path);
        syscall_zero(unsafe { libc::access(path.as_ptr(), libc::R_OK) })
    }

    fn statvfs_path(path: &Path) -> io::Result<()> {
        let path = c_path(path);
        let mut statistics = MaybeUninit::<libc::statvfs>::uninit();
        syscall_zero(unsafe { libc::statvfs(path.as_ptr(), statistics.as_mut_ptr()) })?;
        black_box(unsafe { statistics.assume_init() });
        Ok(())
    }

    fn mkfifo_path(path: &Path) -> io::Result<()> {
        let path = c_path(path);
        syscall_zero(unsafe { libc::mkfifo(path.as_ptr(), 0o644) })
    }

    fn readlink_path(path: &Path) -> io::Result<()> {
        black_box(fs::read_link(path)?);
        Ok(())
    }

    fn rename_noreplace_path(source: &Path, destination: &Path) -> io::Result<()> {
        let source = c_path(source);
        let destination = c_path(destination);
        syscall_zero(unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        })
    }

    fn readdirplus_directory(path: &Path) -> io::Result<()> {
        let mut count = 0usize;
        for entry in fs::read_dir(path)? {
            black_box(entry?.metadata()?);
            count += 1;
        }
        black_box(count);
        Ok(())
    }

    fn getxattr_path(path: &Path) -> io::Result<()> {
        let mut value = vec![0; BENCHMARK_XATTR_VALUE.len()];
        let read = getxattr_into(path, "user.eventfs.benchmark.read", &mut value)?;
        expect_byte_count(read, BENCHMARK_XATTR_VALUE.len(), "getxattr")?;
        black_box(value);
        Ok(())
    }

    fn listxattr_path(path: &Path) -> io::Result<()> {
        let mut value = vec![0; 256];
        let read = listxattr_into(path, &mut value)?;
        if read == 0 {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "listxattr returned no attributes",
            ));
        }
        black_box(value);
        Ok(())
    }

    fn bmap_fd(fd: RawFd) -> io::Result<()> {
        let mut block: libc::c_int = 0;
        syscall_zero(unsafe {
            libc::ioctl(fd, linux_raw_sys::ioctl::FIBMAP as libc::Ioctl, &mut block)
        })?;
        black_box(block);
        Ok(())
    }

    fn ioctl_rejection_fd(fd: RawFd) -> io::Result<()> {
        let result = unsafe {
            libc::ioctl(
                fd,
                BENCHMARK_UNSUPPORTED_IOCTL_COMMAND,
                std::ptr::null_mut::<libc::c_void>(),
            )
        };
        if result == -1 {
            black_box(io::Error::last_os_error().raw_os_error());
            Ok(())
        } else {
            Err(io::Error::other("unsupported ioctl unexpectedly succeeded"))
        }
    }

    fn poll_fd(fd: RawFd) -> io::Result<()> {
        let mut descriptor = libc::pollfd {
            fd,
            events: libc::POLLIN | libc::POLLOUT,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
        if result >= 0 {
            black_box(descriptor.revents);
            Ok(())
        } else {
            Err(last_os_error())
        }
    }

    fn copy_file_range_fd(source: RawFd, destination: RawFd) -> io::Result<()> {
        let mut source_offset: libc::loff_t = 0;
        let mut destination_offset: libc::loff_t = 0;
        let copied = unsafe {
            libc::copy_file_range(
                source,
                &mut source_offset,
                destination,
                &mut destination_offset,
                BENCHMARK_BLOCK_SIZE,
                0,
            )
        };
        if copied >= 0 {
            expect_byte_count(copied as usize, BENCHMARK_BLOCK_SIZE, "copy_file_range")
        } else {
            Err(last_os_error())
        }
    }

    fn setxattr_path(path: &Path, name: &str, value: &[u8], flags: libc::c_int) -> io::Result<()> {
        let path = c_path(path);
        let name = CString::new(name).expect("xattr name has no interior NUL bytes");
        syscall_zero(unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                flags,
            )
        })
    }

    fn getxattr_into(path: &Path, name: &str, value: &mut [u8]) -> io::Result<usize> {
        let path = c_path(path);
        let name = CString::new(name).expect("xattr name has no interior NUL bytes");
        let result = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
            )
        };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(last_os_error())
        }
    }

    fn listxattr_into(path: &Path, value: &mut [u8]) -> io::Result<usize> {
        let path = c_path(path);
        let result =
            unsafe { libc::listxattr(path.as_ptr(), value.as_mut_ptr().cast(), value.len()) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(last_os_error())
        }
    }

    fn removexattr_path(path: &Path, name: &str) -> io::Result<()> {
        let path = c_path(path);
        let name = CString::new(name).expect("xattr name has no interior NUL bytes");
        syscall_zero(unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) })
    }

    fn fcntl_lock(fd: RawFd, command: libc::c_int) -> io::Result<()> {
        let mut lock = libc::flock {
            l_type: libc::F_WRLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: 0,
            l_len: 1,
            l_pid: 0,
        };
        syscall_zero(unsafe { libc::fcntl(fd, command, &mut lock) })?;
        black_box(lock);
        Ok(())
    }

    fn unlock_file(fd: RawFd) -> io::Result<()> {
        let mut lock = libc::flock {
            l_type: libc::F_UNLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: 0,
            l_len: 1,
            l_pid: 0,
        };
        syscall_zero(unsafe { libc::fcntl(fd, libc::F_SETLK, &mut lock) })
    }

    fn fallocate_fd(fd: RawFd, mode: libc::c_int) -> io::Result<()> {
        syscall_zero(unsafe { libc::fallocate(fd, mode, 0, BENCHMARK_BLOCK_SIZE as libc::off_t) })
    }

    fn lseek_fd(fd: RawFd, whence: libc::c_int) -> io::Result<()> {
        let result = unsafe { libc::lseek(fd, 0, whence) };
        if result >= 0 {
            black_box(result);
            Ok(())
        } else {
            Err(last_os_error())
        }
    }

    fn open_directory(path: &Path) -> io::Result<*mut libc::DIR> {
        let path = c_path(path);
        let directory = unsafe { libc::opendir(path.as_ptr()) };
        if directory.is_null() {
            Err(last_os_error())
        } else {
            Ok(directory)
        }
    }

    fn read_directory_entries(directory: *mut libc::DIR) -> usize {
        let mut count = 0usize;
        loop {
            let entry = unsafe { libc::readdir(directory) };
            if entry.is_null() {
                break;
            }
            count += 1;
        }
        count
    }

    fn closedir(directory: *mut libc::DIR) -> io::Result<()> {
        syscall_zero(unsafe { libc::closedir(directory) })
    }

    fn open_directory_fd(path: &Path) -> io::Result<RawFd> {
        let path = c_path(path);
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
        if fd >= 0 {
            Ok(fd)
        } else {
            Err(last_os_error())
        }
    }

    fn duplicate_fd(fd: RawFd) -> io::Result<RawFd> {
        let duplicated = unsafe { libc::dup(fd) };
        if duplicated >= 0 {
            Ok(duplicated)
        } else {
            Err(last_os_error())
        }
    }

    fn close_fd(fd: RawFd) -> io::Result<()> {
        syscall_zero(unsafe { libc::close(fd) })
    }

    fn syscall_zero(result: libc::c_int) -> io::Result<()> {
        if result == 0 {
            Ok(())
        } else {
            Err(last_os_error())
        }
    }

    fn expect_byte_count(actual: usize, expected: usize, operation: &str) -> io::Result<()> {
        if actual == expected {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!("{operation} returned {actual} bytes instead of {expected}"),
            ))
        }
    }

    fn cleanup_path(path: &Path) -> io::Result<()> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
            Ok(_) => fs::remove_file(path),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn iteration_path(root: &Path, operation: &str, index: u64, role: &str) -> PathBuf {
        root.join(format!("{operation}-{index}-{role}"))
    }

    fn xattr_name(operation: &str, index: u64) -> String {
        format!("user.eventfs.benchmark.{operation}.{index}")
    }

    fn c_path(path: &Path) -> CString {
        CString::new(path.as_os_str().as_bytes()).expect("path has no interior NUL bytes")
    }

    fn configuration_error(error: eventfs::ConfigurationError) -> io::Error {
        io::Error::new(ErrorKind::InvalidInput, error)
    }

    fn filesystem_error(error: eventfs::FilesystemError) -> io::Error {
        io::Error::other(error)
    }

    fn last_os_error() -> io::Error {
        io::Error::last_os_error()
    }
}

#[cfg(target_os = "linux")]
criterion::criterion_group! {
    name = benches;
    config = linux::criterion_configuration();
    targets = linux::bench_fuse_operations
}

#[cfg(target_os = "linux")]
criterion::criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {}
