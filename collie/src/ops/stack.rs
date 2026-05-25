//! Stack manipulation — `dup`, `drop`, `swap`, `over`, `rot`,
//! `pick N`, `roll N` — plus the diagnostic ops `time`,
//! `profile.start`, `profile.print`. Storage-mode by construction:
//! these touch the stack without inspecting widths or
//! interpretations, so there's one impl per op.
//!
//! `pick`/`roll` carry a runtime index, which makes them less
//! idiomatic than `>name` / `name>` bindings for anything beyond
//! a couple of values; we keep them because some join code uses
//! them.

use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::Value;

#[derive(Debug)] pub struct Dup;
impl PrimOp for Dup {
    fn name(&self) -> &str { "dup" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 2)) }
    fn routing_map(&self) -> Option<Vec<usize>> { Some(vec![0, 0]) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = st.last().cloned().ok_or("dup: empty stack")?;
        st.push(v);
        Ok(())
    }
}
impl Typed for Dup {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = st.last().ok_or("dup: empty")?.clone();
        st.push(v); Ok(())
    }
}

#[derive(Debug)] pub struct Drop_;
impl PrimOp for Drop_ {
    fn name(&self) -> &str { "drop" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 0)) }
    fn routing_map(&self) -> Option<Vec<usize>> { Some(vec![]) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        pop(st)?; Ok(())
    }
}
impl Typed for Drop_ {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        tc_pop(st, "drop")?; Ok(())
    }
}

#[derive(Debug)] pub struct Swap;
impl PrimOp for Swap {
    fn name(&self) -> &str { "swap" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 2)) }
    fn routing_map(&self) -> Option<Vec<usize>> { Some(vec![1, 0]) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let b = pop(st)?; let a = pop(st)?;
        st.push(b); st.push(a); Ok(())
    }
}
impl Typed for Swap {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if st.len() < 2 { return Err("swap: <2".into()); }
        let n = st.len(); st.swap(n-1, n-2); Ok(())
    }
}

#[derive(Debug)] pub struct Over;
impl PrimOp for Over {
    fn name(&self) -> &str { "over" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 3)) }
    fn routing_map(&self) -> Option<Vec<usize>> { Some(vec![0, 1, 0]) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        if st.len() < 2 { return Err("over: stack too short".into()); }
        let v = st[st.len() - 2].clone();
        st.push(v); Ok(())
    }
}
impl Typed for Over {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if st.len() < 2 { return Err("over: <2".into()); }
        let v = st[st.len()-2].clone(); st.push(v); Ok(())
    }
}

#[derive(Debug)] pub struct Rot;
impl PrimOp for Rot {
    fn name(&self) -> &str { "rot" }
    fn arity(&self) -> Option<(usize, usize)> { Some((3, 3)) }
    fn routing_map(&self) -> Option<Vec<usize>> { Some(vec![1, 2, 0]) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        if st.len() < 3 { return Err("rot: stack too short".into()); }
        let c = pop(st)?; let b = pop(st)?; let a = pop(st)?;
        st.push(b); st.push(c); st.push(a); Ok(())
    }
}
impl Typed for Rot {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if st.len() < 3 { return Err("rot: <3".into()); }
        let n = st.len(); let a = st.remove(n-3); st.push(a); Ok(())
    }
}

#[derive(Debug)] pub struct Pick { pub n: usize }
impl PrimOp for Pick {
    fn name(&self) -> &str { "pick" }
    // Reads from depth n (so stack needs n+1 elements) and pushes a
    // copy — net +1, but the dependency is on those n+1 elements.
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n + 1, self.n + 2)) }
    // n+1 inputs pass through unchanged; output n+1 is a copy of input 0
    // (the deepest of the n+1, i.e. the n-th-from-top).
    fn routing_map(&self) -> Option<Vec<usize>> {
        let mut m: Vec<usize> = (0..=self.n).collect();
        m.push(0);
        Some(m)
    }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        if st.len() <= self.n { return Err(format!("pick {}: stack too short ({})", self.n, st.len())); }
        let v = st[st.len() - 1 - self.n].clone();
        st.push(v); Ok(())
    }
}
impl Typed for Pick {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if st.len() <= self.n { return Err(format!("pick {}: stack {}", self.n, st.len())); }
        let v = st[st.len()-1-self.n].clone(); st.push(v); Ok(())
    }
}

#[derive(Debug)] pub struct Roll { pub n: usize }
impl PrimOp for Roll {
    fn name(&self) -> &str { "roll" }
    // Moves the n-deep element to the top — touches n+1 stack slots.
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n + 1, self.n + 1)) }
    // input 0 (the n-th-from-top) moves to the top; the rest shift down.
    fn routing_map(&self) -> Option<Vec<usize>> {
        let mut m: Vec<usize> = (1..=self.n).collect();
        m.push(0);
        Some(m)
    }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        if st.len() <= self.n { return Err(format!("roll {}: stack too short ({})", self.n, st.len())); }
        let pos = st.len() - 1 - self.n;
        let v = st.remove(pos);
        st.push(v); Ok(())
    }
}
impl Typed for Roll {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if st.len() <= self.n { return Err(format!("roll {}: stack {}", self.n, st.len())); }
        let pos = st.len()-1-self.n; let v = st.remove(pos); st.push(v); Ok(())
    }
}

/// `time` — print wall-clock elapsed since the previous `time` call (or
/// program start) to stdout, then reset the timer. Stack-shape-preserving.
///
/// The timer is thread-local. First call prints `time:    0 ms` and starts
/// the clock. Subsequent calls print elapsed and restart.
///
/// Use to bracket regions of a program (e.g., data load vs query
/// execution) the way datatoad's `.time` directive does.
#[derive(Debug)] pub struct TimeOp;
impl PrimOp for TimeOp {
    fn name(&self) -> &str { "time" }
    fn arity(&self) -> Option<(usize, usize)> { Some((0, 0)) }
    fn run(&self, _st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        use std::cell::RefCell;
        use std::time::Instant;
        thread_local! {
            static TIMER: RefCell<Option<Instant>> = const { RefCell::new(None) };
        }
        TIMER.with(|t| {
            let mut t = t.borrow_mut();
            match *t {
                None => {
                    *t = Some(Instant::now());
                    println!("time:    0 ms  (start)");
                }
                Some(prev) => {
                    let elapsed = prev.elapsed();
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    println!("time: {:>6.1} ms", ms);
                    *t = Some(Instant::now());
                }
            }
        });
        Ok(())
    }
}
impl Typed for TimeOp {}

/// `profile.start` — enable per-op profiling (clears any previous stats).
/// `profile.print` — print sorted summary (longest first) and disable.
///
/// Stack-shape-preserving. Profiling overhead is one branch per op when
/// disabled (cheap), and one `Instant::now()` + map insert per op when
/// enabled. Use to attribute wall-clock time across collie ops in a
/// program region.
#[derive(Debug)] pub struct ProfileStart;
impl PrimOp for ProfileStart {
    fn name(&self) -> &str { "profile.start" }
    fn arity(&self) -> Option<(usize, usize)> { Some((0, 0)) }
    fn run(&self, _st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        crate::ir::profile::start();
        Ok(())
    }
}
impl Typed for ProfileStart {}

#[derive(Debug)] pub struct ProfilePrint;
impl PrimOp for ProfilePrint {
    fn name(&self) -> &str { "profile.print" }
    fn arity(&self) -> Option<(usize, usize)> { Some((0, 0)) }
    fn run(&self, _st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        crate::ir::profile::print_and_stop();
        Ok(())
    }
}
impl Typed for ProfilePrint {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::from_vec;

    fn run_op(op: &dyn PrimOp, stack: Vec<Value>) -> Vec<Value> {
        let mut st = stack;
        let mut env = Vec::new();
        op.run(&mut st, &mut env).unwrap();
        st
    }

    #[test]
    fn dup_duplicates_top() {
        let st = run_op(&Dup, vec![from_vec::<u64>(vec![1, 2, 3])]);
        assert_eq!(st.len(), 2);
        assert_eq!(st[0], st[1]);
    }

    #[test]
    fn drop_removes_top() {
        let st = run_op(&Drop_, vec![from_vec::<u64>(vec![1])]);
        assert_eq!(st.len(), 0);
    }

    #[test]
    fn swap_swaps() {
        let a = from_vec::<u64>(vec![1]);
        let b = from_vec::<u64>(vec![2]);
        let st = run_op(&Swap, vec![a, b]);
        assert_eq!(st[0], from_vec::<u64>(vec![2]));
        assert_eq!(st[1], from_vec::<u64>(vec![1]));
    }

    #[test]
    fn over_copies_second() {
        let a = from_vec::<u64>(vec![1]);
        let b = from_vec::<u64>(vec![2]);
        let st = run_op(&Over, vec![a, b]);
        assert_eq!(st.len(), 3);
        assert_eq!(st[2], from_vec::<u64>(vec![1]));
    }

    #[test]
    fn rot_cycles_top_three() {
        let st = run_op(&Rot, vec![
            from_vec::<u64>(vec![1]),
            from_vec::<u64>(vec![2]),
            from_vec::<u64>(vec![3]),
        ]);
        // (a b c) -> (b c a)
        assert_eq!(st[0], from_vec::<u64>(vec![2]));
        assert_eq!(st[1], from_vec::<u64>(vec![3]));
        assert_eq!(st[2], from_vec::<u64>(vec![1]));
    }

    #[test]
    fn pick_copies_from_depth() {
        let st = run_op(&Pick { n: 2 }, vec![
            from_vec::<u64>(vec![10]),
            from_vec::<u64>(vec![20]),
            from_vec::<u64>(vec![30]),
        ]);
        // top was 30; pick 2 = index 2 from top = 10
        assert_eq!(st.last().unwrap(), &from_vec::<u64>(vec![10]));
    }

    #[test]
    fn roll_moves_from_depth() {
        let st = run_op(&Roll { n: 2 }, vec![
            from_vec::<u64>(vec![10]),
            from_vec::<u64>(vec![20]),
            from_vec::<u64>(vec![30]),
        ]);
        // 10 moves to top, leaving 20, 30
        assert_eq!(st.len(), 3);
        assert_eq!(st[0], from_vec::<u64>(vec![20]));
        assert_eq!(st[1], from_vec::<u64>(vec![30]));
        assert_eq!(st[2], from_vec::<u64>(vec![10]));
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "dup"  => Some(Box::new(Dup)),
            "drop" => Some(Box::new(Drop_)),
            "swap" => Some(Box::new(Swap)),
            "over" => Some(Box::new(Over)),
            "rot"  => Some(Box::new(Rot)),
            "time"          => Some(Box::new(TimeOp)),
            "profile.start" => Some(Box::new(ProfileStart)),
            "profile.print" => Some(Box::new(ProfilePrint)),
            _ => None,
        }
    });
    // pick N and roll N are space-separated in source ("pick 3"), so the
    // parser handles them; nothing to register here.
}
