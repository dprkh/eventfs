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

<table style="table-layout: fixed; width: 100%;">
  <colgroup>
    <col style="width: 25%">
    <col style="width: 25%">
    <col style="width: 25%">
    <col style="width: 25%">
  </colgroup>
  <thead>
    <tr>
      <th>Benchmark</th>
      <th>Host</th>
      <th>eventfs</th>
      <th>Overhead</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Read metadata by path</td>
      <td>215 ns</td>
      <td>226 ns</td>
      <td>1.1x</td>
    </tr>
    <tr>
      <td>Read metadata from an open file</td>
      <td>128 ns</td>
      <td>128 ns</td>
      <td>1.0x</td>
    </tr>
    <tr>
      <td>Change file permissions</td>
      <td>280 ns</td>
      <td>38.91 µs</td>
      <td>139.2x</td>
    </tr>
    <tr>
      <td>Resize a file between 2 KiB and 4 KiB</td>
      <td>523 ns</td>
      <td>62.03 µs</td>
      <td>118.6x</td>
    </tr>
    <tr>
      <td>Check file read access</td>
      <td>198 ns</td>
      <td>211 ns</td>
      <td>1.1x</td>
    </tr>
    <tr>
      <td>Read filesystem statistics</td>
      <td>282 ns</td>
      <td>23.71 µs</td>
      <td>83.9x</td>
    </tr>
    <tr>
      <td>Create a named pipe</td>
      <td>772 ns</td>
      <td>81.73 µs</td>
      <td>105.9x</td>
    </tr>
    <tr>
      <td>Create a directory</td>
      <td>51.42 µs</td>
      <td>82.45 µs</td>
      <td>1.6x</td>
    </tr>
    <tr>
      <td>Create a file</td>
      <td>886 ns</td>
      <td>87.43 µs</td>
      <td>98.7x</td>
    </tr>
    <tr>
      <td>Delete a file</td>
      <td>1.008 µs</td>
      <td>67.49 µs</td>
      <td>66.9x</td>
    </tr>
    <tr>
      <td>Remove an empty directory</td>
      <td>1.307 µs</td>
      <td>66.4 µs</td>
      <td>50.8x</td>
    </tr>
    <tr>
      <td>Rename a file</td>
      <td>874 ns</td>
      <td>84.57 µs</td>
      <td>96.8x</td>
    </tr>
    <tr>
      <td>Rename a file without replacing the destination</td>
      <td>885 ns</td>
      <td>84.26 µs</td>
      <td>95.3x</td>
    </tr>
    <tr>
      <td>Create a hard link</td>
      <td>576 ns</td>
      <td>84.61 µs</td>
      <td>146.8x</td>
    </tr>
    <tr>
      <td>Create a symbolic link</td>
      <td>830 ns</td>
      <td>84.34 µs</td>
      <td>101.6x</td>
    </tr>
    <tr>
      <td>Read a symbolic link target</td>
      <td>247 ns</td>
      <td>22.44 µs</td>
      <td>90.7x</td>
    </tr>
    <tr>
      <td>Open a file</td>
      <td>253 ns</td>
      <td>24.1 µs</td>
      <td>95.4x</td>
    </tr>
    <tr>
      <td>Read 4 KiB</td>
      <td>201 ns</td>
      <td>27.67 µs</td>
      <td>137.4x</td>
    </tr>
    <tr>
      <td>Overwrite 4 KiB</td>
      <td>241 ns</td>
      <td>56.77 µs</td>
      <td>235.6x</td>
    </tr>
    <tr>
      <td>Flush a file</td>
      <td>104 ns</td>
      <td>106 ns</td>
      <td>1.0x</td>
    </tr>
    <tr>
      <td>Close a file</td>
      <td>156 ns</td>
      <td>2.699 µs</td>
      <td>17.3x</td>
    </tr>
    <tr>
      <td>Synchronize a file</td>
      <td>27.35 µs</td>
      <td>54.79 µs</td>
      <td>2.0x</td>
    </tr>
    <tr>
      <td>Open a directory containing 16 files</td>
      <td>392 ns</td>
      <td>25.57 µs</td>
      <td>65.2x</td>
    </tr>
    <tr>
      <td>Read a directory containing 16 files</td>
      <td>538 ns</td>
      <td>51.12 µs</td>
      <td>95.1x</td>
    </tr>
    <tr>
      <td>Read a directory and metadata for its 16 files</td>
      <td>4.353 µs</td>
      <td>108.4 µs</td>
      <td>24.9x</td>
    </tr>
    <tr>
      <td>Synchronize a directory</td>
      <td>26.36 µs</td>
      <td>53.42 µs</td>
      <td>2.0x</td>
    </tr>
    <tr>
      <td>Close a directory</td>
      <td>158 ns</td>
      <td>2.624 µs</td>
      <td>16.7x</td>
    </tr>
    <tr>
      <td>Set a 5-byte extended attribute</td>
      <td>370 ns</td>
      <td>56.76 µs</td>
      <td>153.5x</td>
    </tr>
    <tr>
      <td>Read a 5-byte extended attribute</td>
      <td>292 ns</td>
      <td>23.13 µs</td>
      <td>79.2x</td>
    </tr>
    <tr>
      <td>List extended attributes</td>
      <td>261 ns</td>
      <td>24.28 µs</td>
      <td>93.0x</td>
    </tr>
    <tr>
      <td>Remove an extended attribute</td>
      <td>364 ns</td>
      <td>56.21 µs</td>
      <td>154.4x</td>
    </tr>
    <tr>
      <td>Write a new 256 MiB file sequentially</td>
      <td>32.5 ms</td>
      <td>407.1 ms</td>
      <td>12.5x</td>
    </tr>
    <tr>
      <td>Write the first 64 MiB of a new file</td>
      <td>7.471 ms</td>
      <td>95.7 ms</td>
      <td>12.8x</td>
    </tr>
    <tr>
      <td>Append 64 MiB to a 192 MiB file</td>
      <td>6.538 ms</td>
      <td>97.66 ms</td>
      <td>14.9x</td>
    </tr>
    <tr>
      <td>Overwrite the middle 64 MiB of a 256 MiB file</td>
      <td>3.694 ms</td>
      <td>133.8 ms</td>
      <td>36.2x</td>
    </tr>
    <tr>
      <td>Write 64 MiB at a 512 MiB offset in a sparse file</td>
      <td>6.929 ms</td>
      <td>96.24 ms</td>
      <td>13.9x</td>
    </tr>
    <tr>
      <td>Truncate a 256 MiB file to 128 MiB, then extend it to 320 MiB</td>
      <td>4.629 ms</td>
      <td>61.8 ms</td>
      <td>13.4x</td>
    </tr>
    <tr>
      <td>Write and synchronize a new 256 MiB file</td>
      <td>194.7 ms</td>
      <td>407 ms</td>
      <td>2.1x</td>
    </tr>
    <tr>
      <td>Write, synchronize, and rename a 256 MiB file</td>
      <td>510.1 ms</td>
      <td>403.8 ms</td>
      <td>0.8x</td>
    </tr>
  </tbody>
</table>
