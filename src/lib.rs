//! pulse-cutover library: ceremony config, journal, state machine, verify.
//! The binary in `main.rs` is a thin CLI over these modules; tests drive the
//! machine with a mock `ChainOps`.

pub mod beacon;
pub mod config;
pub mod coord;
pub mod doctor;
pub mod journal;
pub mod keys;
pub mod looper;
pub mod machine;
pub mod ops;
pub mod report;
pub mod sanitize;
pub mod scan;
pub mod state;
pub mod upstream;
pub mod verify;
