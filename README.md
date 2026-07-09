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

*Host*
215 ns

*eventfs*
226 ns

*Difference*
1.1x

### Read metadata from an open file

*Host*
128 ns

*eventfs*
128 ns

*Difference*
1.0x

### Change file permissions

*Host*
280 ns

*eventfs*
38.91 µs

*Difference*
139.2x

### Resize a file between 2 KiB and 4 KiB

*Host*
523 ns

*eventfs*
62.03 µs

*Difference*
118.6x

### Check file read access

*Host*
198 ns

*eventfs*
211 ns

*Difference*
1.1x

### Read filesystem statistics

*Host*
282 ns

*eventfs*
23.71 µs

*Difference*
83.9x

### Create a named pipe

*Host*
772 ns

*eventfs*
81.73 µs

*Difference*
105.9x

### Create a directory

*Host*
51.42 µs

*eventfs*
82.45 µs

*Difference*
1.6x

### Create a file

*Host*
886 ns

*eventfs*
87.43 µs

*Difference*
98.7x

### Delete a file

*Host*
1.008 µs

*eventfs*
67.49 µs

*Difference*
66.9x

### Remove an empty directory

*Host*
1.307 µs

*eventfs*
66.4 µs

*Difference*
50.8x

### Rename a file

*Host*
874 ns

*eventfs*
84.57 µs

*Difference*
96.8x

### Rename a file without replacing the destination

*Host*
885 ns

*eventfs*
84.26 µs

*Difference*
95.3x

### Create a hard link

*Host*
576 ns

*eventfs*
84.61 µs

*Difference*
146.8x

### Create a symbolic link

*Host*
830 ns

*eventfs*
84.34 µs

*Difference*
101.6x

### Read a symbolic link target

*Host*
247 ns

*eventfs*
22.44 µs

*Difference*
90.7x

### Open a file

*Host*
253 ns

*eventfs*
24.1 µs

*Difference*
95.4x

### Read 4 KiB

*Host*
201 ns

*eventfs*
27.67 µs

*Difference*
137.4x

### Overwrite 4 KiB

*Host*
241 ns

*eventfs*
56.77 µs

*Difference*
235.6x

### Flush a file

*Host*
104 ns

*eventfs*
106 ns

*Difference*
1.0x

### Close a file

*Host*
156 ns

*eventfs*
2.699 µs

*Difference*
17.3x

### Synchronize a file

*Host*
27.35 µs

*eventfs*
54.79 µs

*Difference*
2.0x

### Open a directory containing 16 files

*Host*
392 ns

*eventfs*
25.57 µs

*Difference*
65.2x

### Read a directory containing 16 files

*Host*
538 ns

*eventfs*
51.12 µs

*Difference*
95.1x

### Read a directory and metadata for its 16 files

*Host*
4.353 µs

*eventfs*
108.4 µs

*Difference*
24.9x

### Synchronize a directory

*Host*
26.36 µs

*eventfs*
53.42 µs

*Difference*
2.0x

### Close a directory

*Host*
158 ns

*eventfs*
2.624 µs

*Difference*
16.7x

### Set a 5-byte extended attribute

*Host*
370 ns

*eventfs*
56.76 µs

*Difference*
153.5x

### Read a 5-byte extended attribute

*Host*
292 ns

*eventfs*
23.13 µs

*Difference*
79.2x

### List extended attributes

*Host*
261 ns

*eventfs*
24.28 µs

*Difference*
93.0x

### Remove an extended attribute

*Host*
364 ns

*eventfs*
56.21 µs

*Difference*
154.4x

### Write a new 256 MiB file sequentially

*Host*
32.5 ms

*eventfs*
407.1 ms

*Difference*
12.5x

### Write the first 64 MiB of a new file

*Host*
7.471 ms

*eventfs*
95.7 ms

*Difference*
12.8x

### Append 64 MiB to a 192 MiB file

*Host*
6.538 ms

*eventfs*
97.66 ms

*Difference*
14.9x

### Overwrite the middle 64 MiB of a 256 MiB file

*Host*
3.694 ms

*eventfs*
133.8 ms

*Difference*
36.2x

### Write 64 MiB at a 512 MiB offset in a sparse file

*Host*
6.929 ms

*eventfs*
96.24 ms

*Difference*
13.9x

### Truncate a 256 MiB file to 128 MiB, then extend it to 320 MiB

*Host*
4.629 ms

*eventfs*
61.8 ms

*Difference*
13.4x

### Write and synchronize a new 256 MiB file

*Host*
194.7 ms

*eventfs*
407 ms

*Difference*
2.1x

### Write, synchronize, and rename a 256 MiB file

*Host*
510.1 ms

*eventfs*
403.8 ms

*Difference*
0.8x
