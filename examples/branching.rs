use std::fs;

use anyhow::Result;
use eventfs::{BranchName, Filesystem, FilesystemConfiguration};

fn main() -> Result<()> {
    // Prepare an isolated database and mount point.
    let root = tempfile::tempdir()?;
    let database_directory = root.path().join("eventfs.branching.db");
    let mount_point_path = root.path().join("eventfs.branching.mount");
    fs::create_dir(&mount_point_path)?;

    // Open eventfs, mount main, and write the starting version.
    let configuration = FilesystemConfiguration::new(database_directory, mount_point_path.clone())?;
    let filesystem = Filesystem::open(configuration)?;
    let file_path = mount_point_path.join("message.txt");
    let mounted = filesystem.spawn_mount()?;
    fs::write(&file_path, "hello from main")?;
    // Unmount before switching branches.
    mounted.unmount()?;

    // Create feature from main's current head.
    let main = filesystem.current_branch()?;
    let feature_name = BranchName::new("feature")?;
    filesystem.create_branch(&feature_name, main.head_position())?;

    // Switch while unmounted; the next mount shows feature.
    filesystem.switch_branch(&feature_name)?;
    let mounted = filesystem.spawn_mount()?;
    fs::write(&file_path, "hello from feature")?;
    println!("feature: {}", fs::read_to_string(&file_path)?);
    // Unmount before switching again.
    mounted.unmount()?;

    // Return to main and mount its unchanged contents.
    filesystem.switch_branch(main.name())?;
    let mounted = filesystem.spawn_mount()?;
    println!("main: {}", fs::read_to_string(&file_path)?);
    mounted.unmount()?;

    Ok(())
}
