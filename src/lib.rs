//! Event-sourced filesystem library mounted through FUSE and persisted in RocksDB.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

#[cfg(target_os = "linux")]
include!("linux.rs");

#[cfg(not(target_os = "linux"))]
compile_error!("eventfs supports only Linux targets");
