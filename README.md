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

- DigitalOcean `c-2` CPU-Optimized droplet in `sfo2`
- 2 Intel vCPUs, 3.8 GiB RAM, no swap, 25 GB disk
- Intel(R) Xeon(R) Platinum 8358 CPU @ 2.60GHz
- Ubuntu 24.04 x64, Linux 6.8.0-124-generic
- FUSE 3.14.0

| Operation | eventfs mean | host mean | eventfs / host |
| --- | ---: | ---: | ---: |
| `lookup` | 968.22 ns | 901.04 ns | 1.1x |
| `getattr` | 740.21 ns | 737.59 ns | 1.0x |
| `access` | 32.717 us | 734.32 ns | 44.6x |
| `statfs` | 31.113 us | 909.04 ns | 34.2x |
| `mknod` | 89.561 us | 5.2850 us | 16.9x |
| `mkdir` | 91.145 us | 14.585 us | 6.2x |
| `create` | 92.955 us | 6.5674 us | 14.2x |
| `unlink` | 71.810 us | 8.6320 us | 8.3x |
| `rmdir` | 74.567 us | 10.957 us | 6.8x |
| `rename` | 102.17 us | 6.4034 us | 16.0x |
| `rename_noreplace` | 101.99 us | 6.5282 us | 15.6x |
| `link` | 99.578 us | 3.5533 us | 28.0x |
| `symlink` | 101.71 us | 5.7821 us | 17.6x |
| `readlink` | 37.300 us | 916.93 ns | 40.7x |
| `open` | 34.745 us | 1.3152 us | 26.4x |
| `read` | 490.74 ns | 537.27 ns | 0.9x |
| `write` | 154.89 us | 747.69 ns | 207.2x |
| `flush` | 27.127 us | 334.74 ns | 81.0x |
| `release` | 24.526 us | 585.73 ns | 41.9x |
| `fsync` | 60.436 us | 26.270 us | 2.3x |
| `opendir` | 53.816 us | 1.7222 us | 31.2x |
| `readdir` | 123.48 us | 4.2413 us | 29.1x |
| `readdirplus` | 258.98 us | 20.143 us | 12.9x |
| `fsyncdir` | 63.865 us | 408.28 ns | 156.4x |
| `releasedir` | 5.2092 us | 569.03 ns | 9.2x |
| `setxattr` | 178.65 us | 2.3739 us | 75.3x |
| `getxattr` | 83.247 us | 1.2457 us | 66.8x |
| `listxattr` | 56.712 us | 1.1131 us | 50.9x |
| `removexattr` | 176.73 us | 2.2281 us | 79.3x |
