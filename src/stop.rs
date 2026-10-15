//! r30 round 13: a graceful stop. SIGTERM / SIGINT only set a flag; the ceremony checks it at safe stop points (between
//! states and inside its long waits, never inside a pause or resume episode), journals `stopped_by_signal` and exits
//! with [`EXIT_STOPPED`]. An in-flight pause or resume therefore always reaches its ack or closing record first, so
//! `cutover.sh abort` (SIGTERM, wait, then `rollback`) cannot manufacture an "ownership unknown" pause of the
//! agent's own.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

static STOP: AtomicBool = AtomicBool::new(false);

thread_local! {
    // Tests: a stop requested for the calling thread only (parallel tests must not see each other's stops).
    static THREAD_STOP: Cell<bool> = const { Cell::new(false) };
}

/// Exit code of a run stopped by SIGTERM / SIGINT at a safe stop point (nothing half-done; `run` resumes it,
/// `rollback` rolls it back).
pub const EXIT_STOPPED: i32 = 6;

/// Prefix of the error a stopped run returns.
pub const STOPPED: &str = "STOPPED by signal";

extern "C" fn on_signal(_sig: libc::c_int) {
    // Async-signal-safe: one atomic store.
    STOP.store(true, Ordering::SeqCst);
}

/// Installs the SIGTERM / SIGINT handlers (`run`, `join`, `loop`).
pub fn install() {
    let handler = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
}

/// True once a stop was requested (by a signal, or for this thread by [`request_for_this_thread`]).
pub fn requested() -> bool {
    STOP.load(Ordering::SeqCst) || THREAD_STOP.with(|s| s.get())
}

/// Tests: request a stop seen by the calling thread only.
pub fn request_for_this_thread() {
    THREAD_STOP.with(|s| s.set(true));
}

/// Tests: clear this thread's stop request.
pub fn clear_for_this_thread() {
    THREAD_STOP.with(|s| s.set(false));
}
