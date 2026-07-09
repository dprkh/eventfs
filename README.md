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

### Read metadata by path

**Host**<br>
215 ns

**eventfs**<br>
226 ns

**Overhead**<br>
1.1x

### Read metadata from an open file

**Host**<br>
128 ns

**eventfs**<br>
128 ns

**Overhead**<br>
1.0x

### Change file permissions

**Host**<br>
280 ns

**eventfs**<br>
38.91 µs

**Overhead**<br>
139.2x

### Resize a file between 2 KiB and 4 KiB

**Host**<br>
523 ns

**eventfs**<br>
62.03 µs

**Overhead**<br>
118.6x

### Check file read access

**Host**<br>
198 ns

**eventfs**<br>
211 ns

**Overhead**<br>
1.1x

### Read filesystem statistics

**Host**<br>
282 ns

**eventfs**<br>
23.71 µs

**Overhead**<br>
83.9x

### Create a named pipe

**Host**<br>
772 ns

**eventfs**<br>
81.73 µs

**Overhead**<br>
105.9x

### Create a directory

**Host**<br>
51.42 µs

**eventfs**<br>
82.45 µs

**Overhead**<br>
1.6x

### Create a file

**Host**<br>
886 ns

**eventfs**<br>
87.43 µs

**Overhead**<br>
98.7x

### Delete a file

**Host**<br>
1.008 µs

**eventfs**<br>
67.49 µs

**Overhead**<br>
66.9x

### Remove an empty directory

**Host**<br>
1.307 µs

**eventfs**<br>
66.4 µs

**Overhead**<br>
50.8x

### Rename a file

**Host**<br>
874 ns

**eventfs**<br>
84.57 µs

**Overhead**<br>
96.8x

### Rename a file without replacing the destination

**Host**<br>
885 ns

**eventfs**<br>
84.26 µs

**Overhead**<br>
95.3x

### Create a hard link

**Host**<br>
576 ns

**eventfs**<br>
84.61 µs

**Overhead**<br>
146.8x

### Create a symbolic link

**Host**<br>
830 ns

**eventfs**<br>
84.34 µs

**Overhead**<br>
101.6x

### Read a symbolic link target

**Host**<br>
247 ns

**eventfs**<br>
22.44 µs

**Overhead**<br>
90.7x

### Open a file

**Host**<br>
253 ns

**eventfs**<br>
24.1 µs

**Overhead**<br>
95.4x

### Read 4 KiB

**Host**<br>
201 ns

**eventfs**<br>
27.67 µs

**Overhead**<br>
137.4x

### Overwrite 4 KiB

**Host**<br>
241 ns

**eventfs**<br>
56.77 µs

**Overhead**<br>
235.6x

### Flush a file

**Host**<br>
104 ns

**eventfs**<br>
106 ns

**Overhead**<br>
1.0x

### Close a file

**Host**<br>
156 ns

**eventfs**<br>
2.699 µs

**Overhead**<br>
17.3x

### Synchronize a file

**Host**<br>
27.35 µs

**eventfs**<br>
54.79 µs

**Overhead**<br>
2.0x

### Open a directory containing 16 files

**Host**<br>
392 ns

**eventfs**<br>
25.57 µs

**Overhead**<br>
65.2x

### Read a directory containing 16 files

**Host**<br>
538 ns

**eventfs**<br>
51.12 µs

**Overhead**<br>
95.1x

### Read a directory and metadata for its 16 files

**Host**<br>
4.353 µs

**eventfs**<br>
108.4 µs

**Overhead**<br>
24.9x

### Synchronize a directory

**Host**<br>
26.36 µs

**eventfs**<br>
53.42 µs

**Overhead**<br>
2.0x

### Close a directory

**Host**<br>
158 ns

**eventfs**<br>
2.624 µs

**Overhead**<br>
16.7x

### Set a 5-byte extended attribute

**Host**<br>
370 ns

**eventfs**<br>
56.76 µs

**Overhead**<br>
153.5x

### Read a 5-byte extended attribute

**Host**<br>
292 ns

**eventfs**<br>
23.13 µs

**Overhead**<br>
79.2x

### List extended attributes

**Host**<br>
261 ns

**eventfs**<br>
24.28 µs

**Overhead**<br>
93.0x

### Remove an extended attribute

**Host**<br>
364 ns

**eventfs**<br>
56.21 µs

**Overhead**<br>
154.4x

### Write a new 256 MiB file sequentially

**Host**<br>
32.5 ms

**eventfs**<br>
407.1 ms

**Overhead**<br>
12.5x

### Write the first 64 MiB of a new file

**Host**<br>
7.471 ms

**eventfs**<br>
95.7 ms

**Overhead**<br>
12.8x

### Append 64 MiB to a 192 MiB file

**Host**<br>
6.538 ms

**eventfs**<br>
97.66 ms

**Overhead**<br>
14.9x

### Overwrite the middle 64 MiB of a 256 MiB file

**Host**<br>
3.694 ms

**eventfs**<br>
133.8 ms

**Overhead**<br>
36.2x

### Write 64 MiB at a 512 MiB offset in a sparse file

**Host**<br>
6.929 ms

**eventfs**<br>
96.24 ms

**Overhead**<br>
13.9x

### Truncate a 256 MiB file to 128 MiB, then extend it to 320 MiB

**Host**<br>
4.629 ms

**eventfs**<br>
61.8 ms

**Overhead**<br>
13.4x

### Write and synchronize a new 256 MiB file

**Host**<br>
194.7 ms

**eventfs**<br>
407 ms

**Overhead**<br>
2.1x

### Write, synchronize, and rename a 256 MiB file

**Host**<br>
510.1 ms

**eventfs**<br>
403.8 ms

**Overhead**<br>
0.8x
