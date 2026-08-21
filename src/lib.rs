//! pulse-cutover library: ceremony config, journal, state machine, verify.
//! The binary in `main.rs` is a thin CLI over these modules; tests drive the
//! machine with a mock `ChainOps`.

pub mod config;
pub mod doctor;
pub mod journal;
pub mod looper;
pub mod machine;
pub mod ops;
pub mod report;
pub mod sanitize;
pub mod scan;
pub mod state;
pub mod verify;
