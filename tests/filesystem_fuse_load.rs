mod support;

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use support::{
    TestDirectories, access_path, assert_event_sequences_increase, event_page_limit,
    file_identifier_for_path, fsync_directory, list_all_events, mkfifo, mount,
    open_test_filesystem, statvfs,
};

#[test]
fn concurrent_mounted_writes_and_event_listing_preserve_final_contents() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let watched_path = directories.mount_point_path().join("listed-file");
    fs::write(&watched_path, b"seed").expect("listed file is created");
    let watched_file_identifier = file_identifier_for_path(&filesystem, "/listed-file");
    let barrier = Arc::new(Barrier::new(5));
    let mut writers = Vec::new();

    for thread_index in 0..4 {
        let barrier = Arc::clone(&barrier);
        let root = directories.mount_point_path().to_path_buf();
        let watched_path = watched_path.clone();
        writers.push(thread::spawn(move || {
            barrier.wait();
            for file_index in 0..6 {
                let path = root.join(format!("thread-{thread_index}-file-{file_index}"));
                let initial = initial_bytes(thread_index, file_index);
                fs::write(&path, &initial).expect("concurrent file is written");
                let replacement = replacement_bytes(thread_index, file_index);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("concurrent file opens");
                file.seek(SeekFrom::Start(0))
                    .expect("concurrent file seeks");
                file.write_all(&replacement)
                    .expect("concurrent file is overwritten");
                file.set_len(12).expect("concurrent file is truncated");
                file.flush().expect("concurrent file flushes");
                if thread_index == 0 {
                    fs::write(&watched_path, format!("watched-{file_index}"))
                        .expect("listed file is updated under load");
                }
            }
        }));
    }

    let listing_filesystem = filesystem.clone();
    let listing_barrier = Arc::clone(&barrier);
    let lister = thread::spawn(move || {
        listing_barrier.wait();
        for _ in 0..40 {
            let events = list_all_events(&listing_filesystem);
            assert_event_sequences_increase(&events);
            let file_events = listing_filesystem
                .list_file_events(watched_file_identifier, None, event_page_limit(100))
                .expect("file events are listed under load");
            assert_event_sequences_increase(file_events.records());
            thread::yield_now();
        }
    });

    for writer in writers {
        writer.join().expect("writer thread joins");
    }
    lister.join().expect("listing thread joins");

    for thread_index in 0..4 {
        for file_index in 0..6 {
            let path = directories
                .mount_point_path()
                .join(format!("thread-{thread_index}-file-{file_index}"));
            assert_eq!(
                fs::read(path).expect("concurrent final file is read"),
                expected_bytes(thread_index, file_index)
            );
        }
    }
    assert_eq!(
        fs::read(&watched_path).expect("listed file final contents are read"),
        b"watched-5"
    );
    assert!(
        filesystem
            .list_file_events(watched_file_identifier, None, event_page_limit(100))
            .expect("listed file events are available after load")
            .records()
            .len()
            > 1,
        "file event listing observed the updated file"
    );
    mounted.unmount().expect("filesystem unmounts");
    assert_event_sequences_increase(&list_all_events(&filesystem));
}

#[test]
fn mounted_fuse_stress_repeats_supported_operation_combinations() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let work = root.join("work");
    let archive = root.join("archive");
    let links = root.join("links");
    let mut expected = BTreeMap::new();

    fs::create_dir(&work).expect("work directory is created");
    fs::create_dir(&archive).expect("archive directory is created");
    fs::create_dir(&links).expect("links directory is created");
    for index in 0..3 {
        let directory = work.join(format!("dir-{index}"));
        fs::create_dir(&directory).expect("nested directory is created");
        fsync_directory(&directory);
    }

    for index in 0..12 {
        let file_name = format!("file-{index}");
        let source = work.join(format!("dir-{}", index % 3)).join(&file_name);
        let hard_link = links.join(format!("hard-{index}"));
        let symbolic_link = links.join(format!("sym-{index}"));
        let fifo = links.join(format!("fifo-{index}"));
        let renamed = archive.join(format!("renamed-{index}"));
        let mut bytes = format!("stress-file-{index}-initial").into_bytes();

        fs::write(&source, &bytes).expect("stress file is written");
        mutate_file(&source, index, &mut bytes);
        fs::set_permissions(&source, fs::Permissions::from_mode(0o600))
            .expect("stress file metadata is updated");
        assert_eq!(access_path(&source, libc::R_OK), 0);
        assert_eq!(statvfs(root).f_namemax, 255);
        fs::hard_link(&source, &hard_link).expect("hard link is created");
        assert_eq!(fs::read(&hard_link).expect("hard link is read"), bytes);
        symlink(&source, &symbolic_link).expect("symbolic link is created");
        assert_eq!(
            fs::read_link(&symbolic_link).expect("symbolic link target is read"),
            source
        );
        mkfifo(&fifo);
        assert_eq!(access_path(&fifo, libc::R_OK), 0);
        fs::rename(&source, &renamed).expect("file is renamed into archive");
        assert_eq!(fs::read(&renamed).expect("renamed file is read"), bytes);
        assert_eq!(
            fs::read(&hard_link).expect("hard link remains readable after rename"),
            bytes
        );
        fs::remove_file(&symbolic_link).expect("symbolic link is removed");
        fs::remove_file(&hard_link).expect("hard link is removed");
        fs::remove_file(&fifo).expect("fifo is removed");
        fsync_directory(&archive);
        fsync_directory(&links);
        expected.insert(
            renamed.file_name().expect("file name exists").to_owned(),
            bytes,
        );
    }

    for index in 0..3 {
        fs::remove_dir(work.join(format!("dir-{index}"))).expect("nested directory is removed");
    }
    fs::remove_dir(&work).expect("work directory is removed");
    fs::remove_dir(&links).expect("links directory is removed");
    assert_event_sequences_increase(&list_all_events(&filesystem));
    mounted.unmount().expect("filesystem unmounts");

    let mounted = mount(&filesystem);
    for (file_name, bytes) in expected {
        assert_eq!(
            fs::read(archive.join(file_name)).expect("archived file is readable after remount"),
            bytes
        );
    }
    assert_event_sequences_increase(&list_all_events(&filesystem));
    mounted.unmount().expect("filesystem unmounts");
}

fn initial_bytes(thread_index: usize, file_index: usize) -> Vec<u8> {
    format!("thread-{thread_index}-file-{file_index}-initial").into_bytes()
}

fn replacement_bytes(thread_index: usize, file_index: usize) -> Vec<u8> {
    format!("T{thread_index}F{file_index}").into_bytes()
}

fn expected_bytes(thread_index: usize, file_index: usize) -> Vec<u8> {
    let mut bytes = initial_bytes(thread_index, file_index);
    let replacement = replacement_bytes(thread_index, file_index);
    bytes[..replacement.len()].copy_from_slice(&replacement);
    bytes.truncate(12);
    bytes
}

fn mutate_file(path: &Path, index: usize, bytes: &mut Vec<u8>) {
    let patch = format!("P{index:02}").into_bytes();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("stress file opens");
    file.seek(SeekFrom::Start(3)).expect("stress file seeks");
    file.write_all(&patch).expect("stress file is overwritten");
    bytes[3..3 + patch.len()].copy_from_slice(&patch);
    if index.is_multiple_of(2) {
        file.set_len((bytes.len() + 5) as u64)
            .expect("stress file grows");
        bytes.resize(bytes.len() + 5, 0);
    } else {
        file.set_len((bytes.len() - 2) as u64)
            .expect("stress file shrinks");
        bytes.truncate(bytes.len() - 2);
    }
    file.flush().expect("stress file flushes");
    file.sync_all().expect("stress file syncs");
}
