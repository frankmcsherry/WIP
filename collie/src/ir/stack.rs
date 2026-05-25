//! Layer 1: stack discipline. The core's value stack is just a Vec<Value>.
//!
//! Two pops:
//!
//! - `pop()`: the default. Materializes any `Value::View` on top via
//!   `gather(source, selector)` before returning. Existing ops that match
//!   directly on `Prim` / `Prod` / `Sum` / `List` see the materialized form
//!   and don't need a `View` arm. This is the path that gives the
//!   `view` op universal correctness for free.
//!
//! - `pop_raw()`: bypasses materialization. View-aware fast paths (e.g.,
//!   `search.<interp>` over a viewed query column) use this to inspect the
//!   `View` directly and avoid the gather.

use crate::ir::value::Value;

pub type Stack = Vec<Value>;

/// Pop with View materialization. Most ops want this.
pub fn pop(st: &mut Stack) -> Result<Value, String> {
    let v = st.pop().ok_or_else(|| "stack underflow".to_string())?;
    materialize_top(v)
}

/// Pop without materializing a top-level View. Use this from ops that
/// dispatch on `Value::View { .. }` themselves.
pub fn pop_raw(st: &mut Stack) -> Result<Value, String> {
    st.pop().ok_or_else(|| "stack underflow".to_string())
}

/// If `v` is a `View`, materialize it. Two paths:
///
///   - Flat selectors (`Indices`, `Range`): result is `gather(source, idxs)` —
///     keeps source's shape.
///   - `SequenceRange`: result is a real `Value::List` whose bounds are the
///     cumulative `his - los` and whose inner values are the catenation of
///     the per-row sub-slices of source. Source must be a `Value::Prim`.
///
/// Non-View values pass through unchanged.
pub fn materialize_top(v: Value) -> Result<Value, String> {
    match v {
        Value::View { source, selector } => match selector {
            crate::ir::value::Selector::Indices(_)
            | crate::ir::value::Selector::Runs(_)
            | crate::ir::value::Selector::Mask(_) => {
                let idxs = selector.to_usize_vec();
                crate::ops::helpers::gather(&source, &idxs)
            }
            crate::ir::value::Selector::SequenceRange { los, his } => {
                materialize_sequence_range(&source, &los, &his)
            }
        },
        other => Ok(other),
    }
}

/// Build a real `Value::List` from `(source, los, his)`. Each row of the
/// result is `source[los[i]..his[i]]`. Source must be `Value::Prim`.
fn materialize_sequence_range(
    source: &Value,
    los: &[u64],
    his: &[u64],
) -> Result<Value, String> {
    if los.len() != his.len() {
        return Err(format!(
            "materialize SequenceRange: los/his length mismatch ({} vs {})",
            los.len(), his.len()
        ));
    }
    let source_p = match source {
        Value::Prim(p) => p,
        other => return Err(format!(
            "materialize SequenceRange: source must be Prim, got {:?}", other
        )),
    };
    let mut bounds: Vec<u64> = Vec::with_capacity(los.len());
    let mut flat_idxs: Vec<usize> = Vec::new();
    let mut acc: u64 = 0;
    for i in 0..los.len() {
        let lo = los[i] as usize;
        let hi = his[i] as usize;
        if hi < lo {
            return Err(format!("materialize SequenceRange: row {} has hi {} < lo {}", i, hi, lo));
        }
        for j in lo..hi { flat_idxs.push(j); }
        acc += (hi - lo) as u64;
        bounds.push(acc);
    }
    let inner_prim = crate::ops::helpers::gather_prim(source_p, &flat_idxs);
    Ok(Value::List {
        bounds: crate::ir::value::bounds_var_from_ends(bounds),
        values: std::sync::Arc::new(Value::Prim(inner_prim)),
    })
}
