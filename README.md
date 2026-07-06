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

# Benchmarks

FUSE operation benchmarks compare eventfs against a sibling directory on the
host's normal filesystem. These results were measured on July 6, 2026 with the
`fuse_operations` Criterion benchmark, cross-compiled locally for
`x86_64-unknown-linux-gnu` with the bench profile, fat LTO, and one codegen
unit, then run on the benchmark host with `./fuse_operations --bench --noplot`.

Benchmark host:

- DigitalOcean `c-2` CPU-Optimized droplet in `sfo2`
- 2 Intel vCPUs, 4096 MB RAM, 25 GB disk
- Intel(R) Xeon(R) Platinum 8168 CPU @ 2.70GHz
- Ubuntu 24.04.4 LTS x64, Linux 6.8.0-124-generic
- Root and benchmark host directories on `/dev/vda1` ext4
- FUSE 3.14.0
- Benchmark binary SHA-256:
  `11db53204932218c78ebe6b3c9796294c026aa6eabd3a1b284cebbbef0b902fe`

Criterion reports mean time per operation:

| Operation | eventfs mean | host mean | eventfs / host |
| --- | ---: | ---: | ---: |
| `lookup` | 1.77 us | 1.71 us | 1.0x |
| `getattr` | 1.44 us | 1.42 us | 1.0x |
| `setattr` | 1.62 ms | 3.49 us | 463.6x |
| `access` | 35.14 us | 1.54 us | 22.9x |
| `statfs` | 32.72 us | 1.67 us | 19.6x |
| `mknod` | 1.61 ms | 7.21 us | 223.6x |
| `mkdir` | 1.80 ms | 20.58 us | 87.5x |
| `create` | 1.78 ms | 9.11 us | 195.3x |
| `unlink` | 1.59 ms | 12.26 us | 129.6x |
| `rmdir` | 1.71 ms | 14.89 us | 115.1x |
| `rename` | 1.78 ms | 8.97 us | 198.3x |
| `rename_noreplace` | 1.77 ms | 9.12 us | 194.1x |
| `link` | 1.76 ms | 5.32 us | 331.1x |
| `symlink` | 2.27 ms | 8.23 us | 276.2x |
| `readlink` | 41.60 us | 1.76 us | 23.7x |
| `open` | 45.84 us | 2.25 us | 20.4x |
| `read` | 1.14 us | 1.11 us | 1.0x |
| `write` | 2.05 ms | 1.32 us | 1548.6x |
| `truncate` | 2.08 ms | 4.45 us | 468.8x |
| `flush` | 29.09 us | 817.10 ns | 35.6x |
| `release` | 35.78 us | 1.12 us | 32.1x |
| `fsync` | 27.64 us | 26.17 us | 1.1x |
| `copy_file_range` | 2.11 ms | 2.95 us | 715.8x |
| `opendir` | 42.02 us | 3.11 us | 13.5x |
| `readdir` | 172.99 us | 6.19 us | 27.9x |
| `readdirplus` | 374.18 us | 35.60 us | 10.5x |
| `fsyncdir` | 28.68 us | 1.18 us | 24.3x |
| `releasedir` | 5.53 us | 1.10 us | 5.0x |
| `setxattr` | 2.67 ms | 3.66 us | 730.6x |
| `getxattr` | 127.86 us | 2.16 us | 59.2x |
| `listxattr` | 80.51 us | 1.96 us | 41.1x |
| `removexattr` | 2.20 ms | 3.46 us | 636.9x |
| `getlk` | 78.18 us | 905.45 ns | 86.3x |
| `setlk` | 78.91 us | 1.11 us | 71.2x |
| `bmap` | 978.27 ns | 1.18 us | 0.8x |
| `ioctl` | 79.86 us | 811.60 ns | 98.4x |
| `poll` | 79.78 us | 879.32 ns | 90.7x |
| `fallocate_extend` | 210.48 us | 8.58 us | 24.5x |
| `fallocate_keep_size` | 212.57 us | 8.43 us | 25.2x |
| `fallocate_punch_hole` | 2.43 ms | 6.15 us | 395.0x |
| `fallocate_zero_range` | 2.18 ms | 11.72 us | 186.0x |
| `lseek_data` | 134.07 us | 1.09 us | 123.5x |
| `lseek_hole` | 135.92 us | 970.25 ns | 140.1x |
