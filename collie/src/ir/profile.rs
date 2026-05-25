//! Per-op profiling. Off by default — zero overhead in the eval loop until
//! enabled via `profile.start`. Records each op call's wall-clock time and
//! invocation count, then prints a sorted summary on `profile.print`.
//!
//! Times are *self time* (exclusive of children), in the Linux `perf`
//! sense: when a body-bearing op like `let` or `each` calls into nested
//! ops, the parent's accumulated time excludes the children's time. This
//! makes the share % honest — children appear under their own names, not
//! double-counted under their parents.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::time::{Duration, Instant};

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static STATS: RefCell<HashMap<String, (u64, Duration)>> = RefCell::new(HashMap::new());
    /// Stack of "total child time accumulated in this frame so far". One
    /// entry per nested eval level. Lets us compute self_time = elapsed - children.
    static CHILD_STACK: RefCell<Vec<Duration>> = RefCell::new(Vec::new());
}

/// Is profiling currently active? Hot-path query for the eval loop.
#[inline]
pub fn is_enabled() -> bool { ENABLED.with(|e| e.get()) }

/// Run `f` under profiling. The eval loop calls this for each op when
/// profiling is enabled. Records self-time (elapsed minus children's time)
/// against `name`, and bills the full elapsed to the parent's child total.
#[inline]
pub fn time_op<F, R>(name: &str, f: F) -> R
where F: FnOnce() -> R
{
    // Push a fresh "0 children" slot for this op's own children to fill.
    CHILD_STACK.with(|s| s.borrow_mut().push(Duration::ZERO));
    let start = Instant::now();
    let r = f();
    let elapsed = start.elapsed();
    // Pop this op's accumulated child time. Self = elapsed - children.
    let children = CHILD_STACK.with(|s| s.borrow_mut().pop().unwrap_or(Duration::ZERO));
    let self_time = elapsed.checked_sub(children).unwrap_or(Duration::ZERO);
    // Record self time.
    STATS.with(|s| {
        let mut s = s.borrow_mut();
        let entry = s.entry(name.to_string()).or_insert((0, Duration::ZERO));
        entry.0 += 1;
        entry.1 += self_time;
    });
    // Bill the full elapsed to the parent's child accumulator.
    CHILD_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        if let Some(parent) = stack.last_mut() {
            *parent += elapsed;
        }
    });
    r
}

/// Enable profiling and clear previously-accumulated stats.
pub fn start() {
    STATS.with(|s| s.borrow_mut().clear());
    CHILD_STACK.with(|s| s.borrow_mut().clear());
    ENABLED.with(|e| e.set(true));
}

/// Print a sorted summary (longest-running first) to stdout and disable
/// profiling. Stats are cleared.
pub fn print_and_stop() {
    ENABLED.with(|e| e.set(false));
    let entries: Vec<(String, u64, Duration)> = STATS.with(|s| {
        let s = s.borrow();
        let mut v: Vec<_> = s.iter()
            .map(|(k, (c, d))| (k.clone(), *c, *d))
            .collect();
        v.sort_by(|a, b| b.2.cmp(&a.2));
        v
    });
    STATS.with(|s| s.borrow_mut().clear());
    let total_ms: f64 = entries.iter()
        .map(|(_, _, d)| d.as_secs_f64() * 1000.0)
        .sum();
    println!("profile:");
    println!("  {:>10}   {:>10}   {:>10}   {:>6}   {}",
             "total ms", "n calls", "avg µs", "share", "op");
    for (name, count, total) in &entries {
        let ms = total.as_secs_f64() * 1000.0;
        let avg_us = if *count == 0 { 0.0 } else {
            total.as_secs_f64() * 1_000_000.0 / (*count as f64)
        };
        let share = if total_ms == 0.0 { 0.0 } else { ms / total_ms * 100.0 };
        println!("  {:>10.2}   {:>10}   {:>10.2}   {:>5.1}%   {}",
                 ms, count, avg_us, share, name);
    }
    println!("  {:>10.2}   (total)", total_ms);
}
