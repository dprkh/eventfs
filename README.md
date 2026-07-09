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

- Measured with `./dev.sh bench` on July 9, 2026.
- Apple M4 Pro host, macOS 26.2 25C56.
- Apple container Linux VM, Linux 6.18.15 aarch64.
- 14 CPUs and 49.3 GB RAM exposed to the container.
- FUSE 3.14.0.

**Read metadata by path.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 215 ns | 226 ns | 1.1x |

**Read metadata from an open file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 128 ns | 128 ns | 1.0x |

**Change file permissions.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 280 ns | 38.91 µs | 139.2x |

**Resize a file between 2 KiB and 4 KiB.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 523 ns | 62.03 µs | 118.6x |

**Check file read access.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 198 ns | 211 ns | 1.1x |

**Read filesystem statistics.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 282 ns | 23.71 µs | 83.9x |

**Create a named pipe.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 772 ns | 81.73 µs | 105.9x |

**Create a directory.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 51.42 µs | 82.45 µs | 1.6x |

**Create a file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 886 ns | 87.43 µs | 98.7x |

**Delete a file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 1.008 µs | 67.49 µs | 66.9x |

**Remove an empty directory.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 1.307 µs | 66.4 µs | 50.8x |

**Rename a file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 874 ns | 84.57 µs | 96.8x |

**Rename a file without replacing the destination.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 885 ns | 84.26 µs | 95.3x |

**Create a hard link.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 576 ns | 84.61 µs | 146.8x |

**Create a symbolic link.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 830 ns | 84.34 µs | 101.6x |

**Read a symbolic link target.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 247 ns | 22.44 µs | 90.7x |

**Open a file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 253 ns | 24.1 µs | 95.4x |

**Read 4 KiB.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 201 ns | 27.67 µs | 137.4x |

**Overwrite 4 KiB.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 241 ns | 56.77 µs | 235.6x |

**Flush a file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 104 ns | 106 ns | 1.0x |

**Close a file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 156 ns | 2.699 µs | 17.3x |

**Synchronize a file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 27.35 µs | 54.79 µs | 2.0x |

**Open a directory containing 16 files.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 392 ns | 25.57 µs | 65.2x |

**Read a directory containing 16 files.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 538 ns | 51.12 µs | 95.1x |

**Read a directory and metadata for its 16 files.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 4.353 µs | 108.4 µs | 24.9x |

**Synchronize a directory.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 26.36 µs | 53.42 µs | 2.0x |

**Close a directory.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 158 ns | 2.624 µs | 16.7x |

**Set a 5-byte extended attribute.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 370 ns | 56.76 µs | 153.5x |

**Read a 5-byte extended attribute.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 292 ns | 23.13 µs | 79.2x |

**List extended attributes.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 261 ns | 24.28 µs | 93.0x |

**Remove an extended attribute.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 364 ns | 56.21 µs | 154.4x |

**Write a new 256 MiB file sequentially.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 32.5 ms | 407.1 ms | 12.5x |

**Write the first 64 MiB of a new file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 7.471 ms | 95.7 ms | 12.8x |

**Append 64 MiB to a 192 MiB file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 6.538 ms | 97.66 ms | 14.9x |

**Overwrite the middle 64 MiB of a 256 MiB file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 3.694 ms | 133.8 ms | 36.2x |

**Write 64 MiB at a 512 MiB offset in a sparse file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 6.929 ms | 96.24 ms | 13.9x |

**Truncate a 256 MiB file to 128 MiB, then extend it to 320 MiB.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 4.629 ms | 61.8 ms | 13.4x |

**Write and synchronize a new 256 MiB file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 194.7 ms | 407 ms | 2.1x |

**Write, synchronize, and rename a 256 MiB file.**

| Host | eventfs | Difference |
| ---: | ---: | ---: |
| 510.1 ms | 403.8 ms | 0.8x |
