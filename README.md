This library implements a [FUSE](https://www.kernel.org/doc/html/next/filesystems/fuse.html) filesystem that uses [event sourcing](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing) to store data. This approach provides visibility into filesystem changes and allows restoring the filesystem to any previous point. In particular this library supports branching from specific points in the event history (see [examples](./examples)).

# Example

```rust
use std::{fs, path::PathBuf};

use eventfs::{
    EventPageLimit, Filesystem, FilesystemConfiguration, MountOption, SessionAccessControlList,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create the local mount point used by FUSE.
    let mount_point = PathBuf::from("eventfs.mount");

    fs::create_dir_all(&mount_point)?;

    // Open the filesystem and mount it in the background.
    let configuration = FilesystemConfiguration::new("eventfs.db", mount_point.clone())?
        .with_session_access_control_list(SessionAccessControlList::Owner)
        .with_mount_options([MountOption::DefaultPermissions]);

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
