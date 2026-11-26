//! Width rules: the storage an integer op writes, from the storages it reads.
//!
//! An op on narrow integers writes the narrowest storage that holds every result the op can give
//! on values of its operands' storages, so the result is exact and is the `i64` result, held
//! narrower.
//! The width is decided from the storages alone, with no pass over the values.
//! An op whose results need `i64` takes the `i64` kernels, which wrap as they always have.
//!
//! Each kernel reads its operands at one storage and writes one storage; the pairs below are
//! every pair an op reaches, so each lane body is compiled for eleven pairs and no more.

use super::numeric::BinOp;
use crate::value::{Elem, IntWidth, Prim};
use std::sync::Arc;

/// the narrowest storage holding every integer from `lo` to `hi`, or `I64` past it.
pub(crate) fn fit_range(lo: i128, hi: i128) -> IntWidth {
    if lo < i64::MIN as i128 || hi > i64::MAX as i128 {
        return IntWidth::I64;
    }
    IntWidth::fit_range(lo as i64, hi as i64)
}

/// the storage `op` writes on two operands held at `w`.
pub(crate) fn bin_out(op: BinOp, w: IntWidth) -> IntWidth {
    let (lo, hi) = w.range();
    let (lo, hi) = (lo as i128, hi as i128);
    match op {
        BinOp::Add => fit_range(2 * lo, 2 * hi),
        BinOp::Sub => fit_range(lo - hi, hi - lo),
        BinOp::Mul => {
            let c = [lo * lo, lo * hi, hi * hi];
            fit_range(*c.iter().min().unwrap(), *c.iter().max().unwrap())
        }
        // `x / 0 = 0`; otherwise the quotient is no larger than the dividend, but `-MIN` is one
        // past the storage
        BinOp::Div => {
            let m = lo.abs().max(hi.abs());
            if lo >= 0 { fit_range(0, m) } else { fit_range(-m, m) }
        }
        // `x % 0 = x`; otherwise the remainder takes the dividend's sign and is smaller than it
        BinOp::Rem => w,
    }
}

/// the storage a negation of `w` writes.
pub(crate) fn neg_out(w: IntWidth) -> IntWidth {
    let (lo, hi) = w.range();
    fit_range(-(hi as i128), -(lo as i128))
}

/// a leaf's slice at one storage, and a leaf from a buffer at it.
pub(crate) trait Leaf: Elem {
    fn of(p: &Prim) -> &[Self];
    fn wrap(v: Vec<Self>) -> Prim;
}

macro_rules! leaf {
    ($($V:ident => $t:ty),*) => { $(
        impl Leaf for $t {
            fn of(p: &Prim) -> &[Self] {
                match p {
                    Prim::$V(v) => v,
                    _ => unreachable!("a leaf at the storage the width rules chose"),
                }
            }
            fn wrap(v: Vec<Self>) -> Prim {
                Prim::$V(Arc::new(v))
            }
        }
    )* };
}
leaf!(U8 => u8, I8 => i8, I16 => i16, I32 => i32, I64 => i64);

fn zip_at<T: Leaf, O: Leaf>(a: &Prim, b: &Prim, f: &impl Fn(i64, i64) -> i64) -> Prim {
    let (x, y) = (T::of(a), T::of(b));
    O::wrap(x.iter().zip(y).map(|(&p, &q)| O::from_int(f(p.word() as i64, q.word() as i64))).collect())
}

fn map_at<T: Leaf, O: Leaf>(a: &Prim, f: &impl Fn(i64) -> i64) -> Prim {
    O::wrap(T::of(a).iter().map(|&p| O::from_int(f(p.word() as i64))).collect())
}

/// `$go::<T, O>($args)` for the storages `($w, $out)`: every pair an op reaches, and each signed
/// narrow storage written at `i64` for the ops whose results need it.
macro_rules! pairs {
    ($w:expr, $out:expr, $go:ident($($arg:expr),*)) => {{
        use IntWidth::*;
        match ($w, $out) {
            (U8, U8) => Some($go::<u8, u8>($($arg),*)),
            (U8, I16) => Some($go::<u8, i16>($($arg),*)),
            (U8, I32) => Some($go::<u8, i32>($($arg),*)),
            (I8, I8) => Some($go::<i8, i8>($($arg),*)),
            (I8, I16) => Some($go::<i8, i16>($($arg),*)),
            (I16, I16) => Some($go::<i16, i16>($($arg),*)),
            (I16, I32) => Some($go::<i16, i32>($($arg),*)),
            (I32, I32) => Some($go::<i32, i32>($($arg),*)),
            // narrow operands of an op that needs `i64`: read in place, written wide
            (I8, I64) => Some($go::<i8, i64>($($arg),*)),
            (I16, I64) => Some($go::<i16, i64>($($arg),*)),
            (I32, I64) => Some($go::<i32, i64>($($arg),*)),
            _ => None,
        }
    }};
}

/// `f` on two integer leaves at the storage `out` (which must hold every result), both read at
/// storage `w`; `None` when the pair is not one an op reaches.
pub(crate) fn zip(a: &Prim, b: &Prim, w: IntWidth, out: IntWidth, f: impl Fn(i64, i64) -> i64) -> Option<Prim> {
    pairs!(w, out, zip_at(a, b, &f))
}

/// `f` on an integer leaf at storage `w`, written at `out`.
pub(crate) fn map(a: &Prim, w: IntWidth, out: IntWidth, f: impl Fn(i64) -> i64) -> Option<Prim> {
    pairs!(w, out, map_at(a, &f))
}

/// the integer leaves `a` and `b` held at one narrow storage (the narrowest holding both), with the
/// storage; `None` when that is `i64`.
pub(crate) fn met(a: &Prim, b: &Prim) -> Option<(IntWidth, Prim, Prim)> {
    let w = a.int_width()?.join(b.int_width()?);
    (w != IntWidth::I64).then(|| (w, a.cast(w), b.cast(w)))
}

/// the integer leaf `a` and the constant `c` at one narrow storage, with the storage; `None` when
/// that is `i64`.
pub(crate) fn met_imm(a: &Prim, c: i64) -> Option<(IntWidth, Prim)> {
    let w = a.int_width()?;
    // a constant the storage holds leaves it as it is (`5` is a byte, but an `i8` holds it too)
    let w = if w.holds(c) { w } else { w.join(IntWidth::fit(c)) };
    (w != IntWidth::I64).then(|| (w, a.cast(w)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every rule holds every result the op gives on its storage's extremes and on zero.
    #[test]
    fn rules_hold_the_extremes() {
        for w in IntWidth::ALL {
            let (lo, hi) = w.range();
            let xs = [lo, hi, 0, 1, -1].into_iter().filter(|&x| w.holds(x)).collect::<Vec<_>>();
            for op in [BinOp::Add, BinOp::Sub, BinOp::Mul, BinOp::Div, BinOp::Rem] {
                let out = bin_out(op, w);
                for &x in &xs {
                    for &y in &xs {
                        let r: i128 = match op {
                            BinOp::Add => x as i128 + y as i128,
                            BinOp::Sub => x as i128 - y as i128,
                            BinOp::Mul => x as i128 * y as i128,
                            BinOp::Div => if y == 0 { 0 } else { x as i128 / y as i128 },
                            BinOp::Rem => if y == 0 { x as i128 } else { x as i128 % y as i128 },
                        };
                        if out != IntWidth::I64 {
                            assert!(out.holds(r as i64) && r == r as i64 as i128, "{op:?} at {w:?}: {x}, {y} -> {r} past {out:?}");
                        }
                    }
                }
            }
            if neg_out(w) != IntWidth::I64 {
                assert!(xs.iter().all(|&x| neg_out(w).holds(-x)));
            }
        }
    }

    /// Every pair a rule gives below `i64` is one the kernels have.
    #[test]
    fn every_reached_pair_has_a_kernel() {
        for w in IntWidth::ALL.into_iter().filter(|&w| w != IntWidth::I64) {
            let p = Prim::ints_at(w, [0, 1].into_iter());
            let outs = [BinOp::Add, BinOp::Sub, BinOp::Mul, BinOp::Div, BinOp::Rem].map(|op| bin_out(op, w));
            for out in outs.into_iter().chain([neg_out(w)]).filter(|&o| o != IntWidth::I64) {
                assert!(zip(&p, &p, w, out, |x, y| x + y).is_some(), "{w:?} -> {out:?}");
            }
        }
    }
}
