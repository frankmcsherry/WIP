//! Binding and reference — `Let` and `Ref`. Two ops underneath the
//! three surface forms `{| name … | body }` (Factor-style),
//! `>name` (Forth-style sequential bind), and `name>` / `name`
//! (take-on-last-use vs clone).
//!
//! `Let` pushes its bound names onto an env stack, runs its body,
//! then pops. `Ref` reads (and optionally `mem::take`s) the slot.
//! The take-on-last-use machinery enables Arc-1 buffer reuse for
//! anonymous-intermediate chains; see `src/syntax/inference.rs`.

use crate::ir::op::{PrimOp, eval};
use crate::ir::stack::Stack;
use crate::ir::typecheck::{Op, Typed, TypeStack, TypeEnv, typecheck};
use crate::ir::value::Value;

#[derive(Debug)]
pub struct Let { pub names: Vec<String>, pub body: Vec<Box<dyn Op>> }

impl PrimOp for Let {
    fn name(&self) -> &str { "let" }
    // Net stack effect depends on the body. The e-graph driver
    // special-cases Let — recurses on the body separately and
    // ignores the outer effect — so we return None as a barrier.
    fn arity(&self) -> Option<(usize, usize)> { None }
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        if st.len() < self.names.len() {
            return Err(format!("let: stack has {} but need {}", st.len(), self.names.len()));
        }
        let mut bound = Vec::with_capacity(self.names.len());
        for _ in 0..self.names.len() { bound.push(st.pop().unwrap()); }
        bound.reverse();
        let old_len = env.len();
        for v in bound { env.push(v); }
        eval(&self.body, st, env)?;
        env.truncate(old_len);
        Ok(())
    }
}
impl Typed for Let {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        if st.len() < self.names.len() {
            return Err(format!("let: stack underflow ({} required, {} available)", self.names.len(), st.len()));
        }
        let mut bound = Vec::with_capacity(self.names.len());
        for _ in 0..self.names.len() { bound.push(st.pop().unwrap()); }
        bound.reverse();
        let old_len = env.len();
        for s in bound { env.push(s); }
        typecheck(&self.body, st, env).map_err(|e| format!("let body: {}", e))?;
        env.truncate(old_len);
        Ok(())
    }
}

/// Reference to a bound value in the env.
///
/// `take: false` (the default) Arc::clones the value, leaving the env
/// slot intact. `take: true` moves the value out of the env, replacing
/// it with `Value::default()` (an empty Prim sentinel). The parser-side
/// `name>` form emits the take variant; bare `name` emits the clone
/// variant.
///
/// The parser also verifies use-after-take: once a binding has been
/// taken via `name>`, any subsequent reference to `name` errors at
/// parse time. So a defaulted slot should never be read at runtime; the
/// Default impl on `Value` is just a safety net.
#[derive(Debug)]
pub struct Ref { pub idx: usize, pub take: bool }

impl PrimOp for Ref {
    fn name(&self) -> &str { "ref" }
    fn arity(&self) -> Option<(usize, usize)> { Some((0, 1)) }  // env → stack
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        if self.idx >= env.len() {
            return Err(format!("ref {}: env has {} bindings", self.idx, env.len()));
        }
        if self.take {
            // Move out, leaving the default empty-Prim sentinel.
            st.push(std::mem::take(&mut env[self.idx]));
        } else {
            st.push(env[self.idx].clone());
        }
        Ok(())
    }
}
impl Typed for Ref {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        if self.idx >= env.len() {
            return Err(format!("ref {}: env len {}", self.idx, env.len()));
        }
        st.push(env[self.idx].clone());
        Ok(())
    }
}
