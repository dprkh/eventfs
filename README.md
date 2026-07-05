# Example

```rust
use std::{fs, path::PathBuf};

use anyhow::Result;

use eventfs::{EventPageLimit, Filesystem, FilesystemConfiguration};

fn main() -> Result<()> {
    // Prepare a local mount point.
    let mount_point_path = PathBuf::from("eventfs.mount");
    fs::create_dir_all(&mount_point_path)?;

    // Open eventfs and mount it in the background.
    let configuration = FilesystemConfiguration::new("eventfs.db", mount_point_path.clone())?;
    let filesystem = Filesystem::open(configuration)?;
    let _mounted = filesystem.spawn_mount()?;

    // Write and read a file through the mounted filesystem.
    let file_path = mount_point_path.join("hello-world.txt");
    fs::write(&file_path, "Hello, world!")?;
    println!("{}", fs::read_to_string(&file_path)?);

    // Print every event.
    let limit = EventPageLimit::new(100)?;
    for event in filesystem.events(limit) {
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
- [ ] `setxattr`
- [ ] `getxattr`
- [ ] `listxattr`
- [ ] `removexattr`
- [x] `access`
- [x] `create`
- [ ] `readdirplus`
- [ ] `getlk`
- [ ] `setlk`
- [ ] `bmap`
- [ ] `ioctl`
- [ ] `poll`
- [ ] `fallocate`
- [ ] `lseek`
- [ ] `copy_file_range`
- [ ] `setvolname`
- [ ] `exchange`
- [ ] `getxtimes`
