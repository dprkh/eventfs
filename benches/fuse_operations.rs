#[cfg(not(target_os = "linux"))]
compile_error!("eventfs supports only Linux targets");

#[cfg(target_os = "linux")]
include!("fuse_operations/linux.rs");
