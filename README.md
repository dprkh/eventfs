This library implements a [FUSE](https://www.kernel.org/doc/html/next/filesystems/fuse.html) filesystem that uses [event sourcing](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing) to store data. This approach provides visibility into filesystem changes and allows restoring the filesystem to any previous point. In particular this library supports branching from specific points in the event history (see [examples](./examples)).

# Example

```rust
use std::{fs, path::PathBuf};

use eventfs::{EventPageLimit, Filesystem, FilesystemConfiguration};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create the local mount point used by FUSE.
    let mount_point = PathBuf::from("eventfs.mount");

    fs::create_dir_all(&mount_point)?;

    // Open the filesystem and mount it in the background.
    let configuration = FilesystemConfiguration::new("eventfs.db", mount_point.clone())?;

    let filesystem = Filesystem::open(configuration)?;

    let _ = filesystem.spawn_mount()?;

    // Write a file through the mounted filesystem.
    fs::write(mount_point.join("hello.txt"), "Hello from eventfs!")?;

    // Print the events created by filesystem changes.
    for event in filesystem.events(EventPageLimit::new(10)?) {
        println!("{:?}", event?);
    }

    Ok(())
}
```

# FUSE operation support

- [x] `lookup`
- [x] `getattr`
- [x] `setattr`
- [x] `readlink`
- [x] `mknod`
- [x] `mkdir`
- [x] `unlink`
- [x] `rmdir`
- [x] `symlink`
- [x] `rename`
- [x] `link`
- [x] `open`
- [x] `read`
- [x] `write`
- [x] `flush`
- [x] `release`
- [x] `fsync`
- [x] `opendir`
- [x] `readdir`
- [x] `releasedir`
- [x] `fsyncdir`
- [x] `statfs`
- [x] `setxattr`
- [x] `getxattr`
- [x] `listxattr`
- [x] `removexattr`
- [x] `access`
- [x] `create`
- [x] `readdirplus`
- [ ] `getlk`
- [ ] `setlk`
- [ ] `bmap`
- [ ] `ioctl`
- [ ] `poll`
- [ ] `fallocate`
- [ ] `lseek`
- [ ] `copy_file_range`

# Benchmarks

- Measured with `./dev.sh bench` on July 7, 2026.
- Apple M4 Pro host, macOS 26.2 25C56.
- Apple container Linux VM, Linux 6.18.15 aarch64.
- 14 CPUs and 49.3 GB RAM exposed to the container.
- FUSE 3.14.0.

| Operation | eventfs mean | host mean | eventfs / host |
| --- | ---: | ---: | ---: |
| `lookup` | 218.10 ns | 215.93 ns | 1.0x |
| `getattr` | 126.87 ns | 134.23 ns | 0.9x |
| `setattr_metadata` | 35.335 µs | 281.02 ns | 125.7x |
| `setattr_size` | 69.746 µs | 460.16 ns | 151.6x |
| `access` | 207.64 ns | 195.96 ns | 1.1x |
| `statfs` | 28.540 µs | 272.90 ns | 104.6x |
| `mknod` | 83.455 µs | 827.73 ns | 100.8x |
| `mkdir` | 86.459 µs | 53.654 µs | 1.6x |
| `create` | 96.319 µs | 1.0122 µs | 95.2x |
| `unlink` | 71.465 µs | 1.0185 µs | 70.2x |
| `rmdir` | 73.510 µs | 1.3399 µs | 54.9x |
| `rename` | 96.569 µs | 984.11 ns | 98.1x |
| `rename_noreplace` | 97.273 µs | 1.0241 µs | 95.0x |
| `link` | 96.447 µs | 665.70 ns | 144.9x |
| `symlink` | 94.376 µs | 998.58 ns | 94.5x |
| `readlink` | 25.142 µs | 248.45 ns | 101.2x |
| `open` | 25.858 µs | 254.93 ns | 101.4x |
| `read` | 28.343 µs | 201.09 ns | 140.9x |
| `write` | 55.232 µs | 235.94 ns | 234.1x |
| `flush` | 108.30 ns | 107.50 ns | 1.0x |
| `release` | 2.8680 µs | 153.36 ns | 18.7x |
| `fsync` | 54.416 µs | 26.272 µs | 2.1x |
| `opendir` | 22.105 µs | 381.32 ns | 58.0x |
| `readdir` | 51.691 µs | 535.39 ns | 96.5x |
| `readdirplus` | 111.06 µs | 4.3872 µs | 25.3x |
| `fsyncdir` | 51.064 µs | 26.538 µs | 1.9x |
| `releasedir` | 2.6518 µs | 156.46 ns | 16.9x |
| `setxattr` | 55.985 µs | 370.63 ns | 151.1x |
| `getxattr` | 23.552 µs | 292.10 ns | 80.6x |
| `listxattr` | 23.789 µs | 261.39 ns | 91.0x |
| `removexattr` | 56.521 µs | 351.28 ns | 160.9x |
