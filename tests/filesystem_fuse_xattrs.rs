mod support;

use std::sync::{Arc, Mutex};

use eventfs::EventKind;

use support::{
    TestDirectories, assert_callback_errors_include, event_count, expect_event_kinds,
    expect_no_events, filesystem_with_fuse_error_callback, get_xattr, get_xattr_into_buffer,
    list_xattr, list_xattr_into_buffer, mount, open_test_filesystem, recorded_callback_errors,
    remove_xattr, set_xattr, write_mounted_file,
};

#[test]
fn mounted_extended_attributes_round_trip_and_append_exact_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("file");
    let name = "user.eventfs.supported";

    write_mounted_file(&file_path, b"contents").expect("file is written");
    let mut events = event_count(&filesystem);

    set_xattr(&file_path, name, b"value", libc::XATTR_CREATE).expect("xattr is created");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::ExtendedAttributeSet],
        "xattr create appends one extended-attribute-set event",
    );
    assert_eq!(
        get_xattr(&file_path, name).expect("xattr value is read"),
        b"value"
    );
    expect_no_events(&mut events, &filesystem, "getxattr does not append events");
    assert!(
        list_xattr(&file_path)
            .expect("xattr list is read")
            .windows(name.len())
            .any(|window| window == name.as_bytes()),
        "xattr list includes the created attribute"
    );
    expect_no_events(&mut events, &filesystem, "listxattr does not append events");

    set_xattr(&file_path, name, b"replacement", libc::XATTR_REPLACE).expect("xattr is replaced");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::ExtendedAttributeSet],
        "xattr replace appends one extended-attribute-set event",
    );
    assert_eq!(
        get_xattr(&file_path, name).expect("replacement xattr value is read"),
        b"replacement"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "replacement getxattr does not append events",
    );

    remove_xattr(&file_path, name).expect("xattr is removed");
    expect_event_kinds(
        &mut events,
        &filesystem,
        &[EventKind::ExtendedAttributeRemoved],
        "xattr removal appends one extended-attribute-removed event",
    );
    assert!(
        get_xattr(&file_path, name).is_err(),
        "removed xattr is no longer readable"
    );
    expect_no_events(
        &mut events,
        &filesystem,
        "failed getxattr for a missing xattr does not append events",
    );
}

#[test]
fn mounted_extended_attribute_small_buffers_return_range_errors_without_events() {
    let directories = TestDirectories::new();
    let filesystem = open_test_filesystem(&directories);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("file");
    let name = "user.eventfs.small-buffer";

    write_mounted_file(&file_path, b"contents").expect("file is written");
    set_xattr(&file_path, name, b"value", libc::XATTR_CREATE).expect("xattr is created");
    let mut events = event_count(&filesystem);

    let value_error =
        get_xattr_into_buffer(&file_path, name, 1).expect_err("small getxattr buffer is rejected");
    assert_eq!(value_error.raw_os_error(), Some(libc::ERANGE));
    expect_no_events(
        &mut events,
        &filesystem,
        "small getxattr buffer does not append events",
    );

    let list_error =
        list_xattr_into_buffer(&file_path, 1).expect_err("small listxattr buffer is rejected");
    assert_eq!(list_error.raw_os_error(), Some(libc::ERANGE));
    expect_no_events(
        &mut events,
        &filesystem,
        "small listxattr buffer does not append events",
    );
}

#[test]
fn fuse_error_callback_receives_supported_xattr_failures_without_events() {
    let directories = TestDirectories::new();
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let filesystem = filesystem_with_fuse_error_callback(&directories, &callback_errors);
    let _mounted = mount(&filesystem);
    let file_path = directories.mount_point_path().join("file");
    let name = "user.eventfs.missing";

    write_mounted_file(&file_path, b"contents").expect("file is written");
    let mut events = event_count(&filesystem);
    let error = set_xattr(&file_path, name, b"value", libc::XATTR_REPLACE)
        .expect_err("replacing a missing xattr fails");

    let errors = recorded_callback_errors(&callback_errors);
    assert_callback_errors_include(&errors, "setxattr", error.raw_os_error().unwrap(), false);
    expect_no_events(
        &mut events,
        &filesystem,
        "failed xattr replace does not append events",
    );
}
