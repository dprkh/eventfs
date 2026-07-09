mod support;

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use support::{
    TestDirectories, access_path, assert_event_sequences_increase, event_page_limit,
    file_identifier_for_path, fsync_directory, get_xattr, list_all_events, list_xattr, mkfifo,
    mount, open_test_filesystem, remove_xattr, set_xattr, statvfs, write_mounted_file,
};

#[test]
fn concurrent_mounted_writes_and_event_listing_preserve_final_contents() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = filesystem
        .spawn_mount()
        .expect("filesystem mounts in the background");
    let watched_path = directories.mount_point_path().join("listed-file");
    write_mounted_file(&watched_path, b"seed").expect("listed file is created");
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
                write_mounted_file(&path, &initial).expect("concurrent file is written");
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
                file.flush().expect("concurrent file flushes");
                if thread_index == 0 {
                    write_mounted_file(&watched_path, format!("watched-{file_index}"))
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
fn mounted_file_matrix_covers_sizes_patterns_and_operations() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let mounted = mount(&filesystem);
    let root = directories.mount_point_path();
    let matrix = root.join("matrix");
    let sizes = [
        0,
        1,
        4095,
        4096,
        4097,
        16 * 1024,
        64 * 1024,
        256 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
    ];

    fs::create_dir(&matrix).expect("matrix directory is created");
    for (index, size) in sizes.into_iter().enumerate() {
        let path = matrix.join(format!("case-{index}"));
        let renamed = matrix.join(format!("case-{index}-renamed"));
        let hard_link = matrix.join(format!("case-{index}-hard"));
        let symbolic_link = matrix.join(format!("case-{index}-symlink"));
        let mut expected = patterned_workflow_bytes(size, index as u8);
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("matrix file is created");

        for chunk in expected.chunks(matrix_write_chunk_size(index)) {
            file.write_all(chunk).expect("matrix file chunk is written");
        }
        file.flush().expect("matrix file is flushed");
        file.sync_data().expect("matrix file data is synchronized");

        let patch = patterned_workflow_bytes(8193.min(expected.len().max(1)), 100 + index as u8);
        let overwrite_offset = expected.len().saturating_sub(patch.len()) / 2;
        file.seek(SeekFrom::Start(overwrite_offset as u64))
            .expect("matrix file seeks for overwrite");
        file.write_all(&patch).expect("matrix overwrite is written");
        if expected.len() < overwrite_offset + patch.len() {
            expected.resize(overwrite_offset + patch.len(), 0);
        }
        expected[overwrite_offset..overwrite_offset + patch.len()].copy_from_slice(&patch);

        let sparse_offset = expected.len() + 4096 + index;
        let sparse = patterned_workflow_bytes(4097, 150 + index as u8);
        file.seek(SeekFrom::Start(sparse_offset as u64))
            .expect("matrix file seeks for sparse write");
        file.write_all(&sparse)
            .expect("matrix sparse write succeeds");
        expected.resize(sparse_offset, 0);
        expected.extend_from_slice(&sparse);

        let truncated_len = expected.len() / 2;
        file.set_len(truncated_len as u64)
            .expect("matrix file truncates");
        expected.truncate(truncated_len);
        let extended_len = expected.len() + 2048 + index;
        file.set_len(extended_len as u64)
            .expect("matrix file extends");
        expected.resize(extended_len, 0);
        drop(file);

        fs::rename(&path, &renamed).expect("matrix file is renamed");
        fs::hard_link(&renamed, &hard_link).expect("matrix hard link is created");
        symlink(&renamed, &symbolic_link).expect("matrix symbolic link is created");
        assert_eq!(
            fs::read_link(&symbolic_link).expect("matrix symbolic link target is read"),
            renamed
        );
        set_xattr(&renamed, "user.matrix", b"value", 0).expect("matrix xattr is set");
        assert_eq!(
            get_xattr(&renamed, "user.matrix").expect("matrix xattr is read"),
            b"value"
        );
        assert!(
            list_xattr(&renamed)
                .expect("matrix xattrs are listed")
                .windows(b"user.matrix\0".len())
                .any(|window| window == b"user.matrix\0")
        );
        remove_xattr(&renamed, "user.matrix").expect("matrix xattr is removed");
        assert_eq!(
            fs::read(&renamed).expect("matrix renamed file is read"),
            expected
        );
        assert_eq!(
            fs::read(&hard_link).expect("matrix hard link is read"),
            expected
        );

        fs::remove_file(&symbolic_link).expect("matrix symbolic link is removed");
        fs::remove_file(&hard_link).expect("matrix hard link is removed");
        fs::remove_file(&renamed).expect("matrix renamed file is removed");
    }

    assert!(
        fs::read_dir(&matrix)
            .expect("matrix directory is read")
            .next()
            .is_none(),
        "matrix workflow cleans up all cases"
    );
    assert_eq!(statvfs(root).f_namemax, 255);
    fs::remove_dir(&matrix).expect("matrix directory is removed");
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

        write_mounted_file(&source, &bytes).expect("stress file is written");
        mutate_file(&source, index, &mut bytes);
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

fn patterned_workflow_bytes(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
}

fn matrix_write_chunk_size(index: usize) -> usize {
    match index % 4 {
        0 => 257,
        1 => 4096,
        2 => 16 * 1024,
        _ => 64 * 1024,
    }
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
        let new_len = bytes.len() + 5;
        file.seek(SeekFrom::Start((new_len - 1) as u64))
            .expect("stress file seeks to extension point");
        file.write_all(&[0]).expect("stress file grows");
        bytes.resize(new_len, 0);
    }
    file.flush().expect("stress file flushes");
    file.sync_all().expect("stress file syncs");
}
