use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use eventfs::{
    EventKind, EventPageLimit, EventRecord, EventSequence, FileIdentifier, Filesystem,
    FilesystemConfiguration,
};

fn main() -> Result<()> {
    let mount_point_path = PathBuf::from("eventfs.diff.mount");
    fs::create_dir_all(&mount_point_path)?;

    let configuration = FilesystemConfiguration::builder()
        .database_directory(PathBuf::from("eventfs.diff.db"))
        .mount_point(mount_point_path.clone())
        .build()?;
    let filesystem = Filesystem::open(configuration)?;
    let mounted = filesystem.spawn_mount()?;

    let file_path = mount_point_path.join("message.txt");
    fs::write(&file_path, b"hello world\n")?;
    fs::write(&file_path, b"hi mom\n")?;
    mounted.unmount()?;

    let events = list_all_events(&filesystem)?;
    let file_identifier = file_identifier_for_path(&events, "/message.txt")?;
    let file_writes = events
        .iter()
        .filter(|event| {
            event.kind() == EventKind::FileWritten
                && event.file_identifier() == Some(file_identifier)
        })
        .collect::<Vec<_>>();
    let first_write = file_writes.first().context("first write event exists")?;
    let final_write = file_writes.last().context("final write event exists")?;

    let before = snapshot_bytes_at(&filesystem, file_identifier, first_write.sequence())?;
    let after = snapshot_bytes_at(&filesystem, file_identifier, final_write.sequence())?;

    print_simple_diff(&before, &after);
    Ok(())
}

fn list_all_events(filesystem: &Filesystem) -> Result<Vec<EventRecord>> {
    let limit = EventPageLimit::try_from(100)?;
    let mut after = None;
    let mut events = Vec::new();
    loop {
        let page = filesystem.list_events(after, limit)?;
        events.extend_from_slice(page.records());
        match page.next_after() {
            Some(next_after) => after = Some(next_after),
            None => return Ok(events),
        }
    }
}

fn file_identifier_for_path(events: &[EventRecord], path: &str) -> Result<FileIdentifier> {
    events
        .iter()
        .find(|event| event.kind() == EventKind::FileCreated && event.path() == Some(path))
        .and_then(EventRecord::file_identifier)
        .context("file creation event has an identifier")
}

fn snapshot_bytes_at(
    filesystem: &Filesystem,
    file_identifier: FileIdentifier,
    sequence: EventSequence,
) -> Result<Vec<u8>> {
    let snapshot = filesystem
        .file_snapshot_at_or_before(file_identifier, sequence)?
        .context("file snapshot exists")?;
    Ok(filesystem.read_file_snapshot_range(&snapshot, 0, snapshot.file_size())?)
}

fn print_simple_diff(before: &[u8], after: &[u8]) {
    let before = String::from_utf8_lossy(before);
    let after = String::from_utf8_lossy(after);

    for line in before.lines() {
        println!("-{line}");
    }
    for line in after.lines() {
        println!("+{line}");
    }
}
