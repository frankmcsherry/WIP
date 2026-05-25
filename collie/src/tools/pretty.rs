//! Compact, REPL-friendly Value rendering.
//!
//! The default `Display` for `Value` prints every element verbatim, which is
//! fine for the demo runner but exhausting in a REPL. `pretty` truncates long
//! Prims, renders Lists as nested rows, and aims for something a human can
//! scan in one line.
//!
//! Formats:
//!   Prim:    `P64[1, 2, 3]`     or `P64[0, 1, 2, …, 997, 998, 999]·1000`
//!   Prod:    `(a, b, c)`
//!   Sum:     `Sum<P8[0,1,0] | lane0, lane1>`
//!   List:    `List<[1, 2, 3] ; [4, 5] ; [6]>`  (rows separated by ` ; `)
//!   Stride:  `Stride<2×3><[1,2,3] ; [4,5,6]>`  (prefix shows shape, then rows)

use crate::ir::value::{Value, Prim, BoundsRepr};

/// Max Prim elements to show inline before truncating.
pub const MAX_PRIM: usize = 16;
/// Max List rows to show before truncating.
pub const MAX_ROWS: usize = 8;
/// Elements/rows shown at head and tail when truncating.
pub const EDGE: usize = 3;

pub fn pretty(v: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, v);
    out
}

fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Prim(p) => write_prim(out, p),
        Value::Prod(fs) => {
            out.push('(');
            for (i, f) in fs.iter().enumerate() {
                if i > 0 { out.push_str(", "); }
                write_value(out, f);
            }
            out.push(')');
        }
        Value::Sum { disc, lanes } => {
            out.push_str("Sum<");
            write_prim(out, disc);
            out.push_str(" | ");
            for (i, l) in lanes.iter().enumerate() {
                if i > 0 { out.push_str(", "); }
                write_value(out, l);
            }
            out.push('>');
        }
        Value::List { bounds, values } => write_list(out, bounds, values),
        Value::View { selector, .. } => {
            // Render the materialized form, prefixed with a tag so the reader
            // knows the storage form. Display ergonomics > faithfulness here.
            out.push_str(&format!("View<n={}>", selector.len()));
            match crate::ops::helpers::materialize_ref(v) {
                Ok(mat) => write_value(out, mat.as_ref()),
                Err(e) => out.push_str(&format!("«{}»", e)),
            }
        }
    }
}

fn write_prim(out: &mut String, p: &Prim) {
    out.push_str(&format!("{}", p.width()));
    out.push('[');
    write_prim_items(out, p, 0, p.len());
    out.push(']');
    if p.len() > MAX_PRIM {
        out.push_str(&format!("·{}", p.len()));
    }
}

fn write_prim_items(out: &mut String, p: &Prim, lo: usize, hi: usize) {
    let len = hi - lo;
    let trunc = len > MAX_PRIM;
    let render_at = |out: &mut String, i: usize| {
        let abs = lo + i;
        match p {
            Prim::P8(v)  => out.push_str(&format!("{}", v[abs])),
            Prim::P16(v) => out.push_str(&format!("{}", v[abs])),
            Prim::P32(v) => out.push_str(&format!("{}", v[abs])),
            Prim::P64(v) => out.push_str(&format!("{}", v[abs])),
        }
    };
    if !trunc {
        for i in 0..len {
            if i > 0 { out.push_str(", "); }
            render_at(out, i);
        }
    } else {
        for i in 0..EDGE {
            if i > 0 { out.push_str(", "); }
            render_at(out, i);
        }
        out.push_str(", …, ");
        for i in (len - EDGE)..len {
            if i > len - EDGE { out.push_str(", "); }
            render_at(out, i);
        }
    }
}

fn write_list(out: &mut String, bounds: &BoundsRepr, values: &Value) {
    // Special case: List<P8> with valid UTF-8 → render as Str rows, one
    // per line. Useful for ASCII art and text payloads.
    if let Value::Prim(Prim::P8(bytes)) = values {
        let bytes_slice: &[u8] = bytes;
        if std::str::from_utf8(bytes_slice).is_ok() {
            out.push_str("Str<\n");
            for (lo, hi) in bounds.iter_pairs() {
                let slice = &bytes_slice[lo as usize..hi as usize];
                out.push_str("  ");
                out.push_str(std::str::from_utf8(slice).unwrap());
                out.push('\n');
            }
            out.push('>');
            return;
        }
    }
    // Show the Stride shape signal up front when applicable.
    if let BoundsRepr::Stride { stride, count } = bounds {
        out.push_str(&format!("Stride<{}×{}>", count, stride));
    }
    out.push_str("List<");
    let bnds: Vec<u64> = bounds.iter_starts().collect();
    let n = bounds.len();
    let trunc = n > MAX_ROWS;
    let emit_row = |out: &mut String, i: usize| {
        let lo = bnds[i] as usize;
        let hi = bnds[i + 1] as usize;
        write_row(out, values, lo, hi);
    };
    if !trunc {
        for i in 0..n {
            if i > 0 { out.push_str(" ; "); }
            emit_row(out, i);
        }
    } else {
        for i in 0..EDGE {
            if i > 0 { out.push_str(" ; "); }
            emit_row(out, i);
        }
        out.push_str(" ; … ; ");
        for i in (n - EDGE)..n {
            if i > n - EDGE { out.push_str(" ; "); }
            emit_row(out, i);
        }
    }
    out.push('>');
    if trunc {
        out.push_str(&format!("·{}", n));
    }
}

/// Render one row of a List's inner value (a slice of the inner).
/// For inner Prim, emits `[x, y, z]` (no width tag — the outer List already
/// established the width via `write_value` on the parent List value if you
/// asked for that, but we omit it here for compactness).
fn write_row(out: &mut String, inner: &Value, lo: usize, hi: usize) {
    match inner {
        Value::Prim(p) => {
            out.push('[');
            write_prim_items(out, p, lo, hi);
            out.push(']');
        }
        Value::Prod(fs) => {
            out.push('(');
            for (i, f) in fs.iter().enumerate() {
                if i > 0 { out.push_str(", "); }
                write_row(out, f, lo, hi);
            }
            out.push(')');
        }
        Value::List { bounds: sub_bnds, values: sub_inner } => {
            // Row of a List<List<X>>: slice the sub-bounds to [lo, hi).
            let bnds: Vec<u64> = sub_bnds.iter_starts().collect();
            out.push('<');
            for j in lo..hi {
                if j > lo { out.push_str(" ; "); }
                write_row(out, sub_inner, bnds[j] as usize, bnds[j + 1] as usize);
            }
            out.push('>');
        }
        Value::Sum { .. } => out.push_str("«sum-in-list»"),
        Value::View { .. } => out.push_str("«view-in-list»"),
    }
}

// Tiny smoke tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::{from_vec, bounds_var_from_ends, list, prod};

    #[test]
    fn short_prim() {
        assert_eq!(pretty(&from_vec::<u64>(vec![1, 2, 3])), "P64[1, 2, 3]");
    }

    #[test]
    fn long_prim_truncates() {
        let v = from_vec::<u64>((0..100u64).collect());
        let s = pretty(&v);
        assert!(s.contains("…"));
        assert!(s.ends_with("·100"));
    }

    #[test]
    fn list_of_prim_rows() {
        let l = list(
            bounds_var_from_ends(vec![3, 5, 6]),
            from_vec::<u64>(vec![1, 2, 3, 4, 5, 6]),
        );
        assert_eq!(pretty(&l), "List<[1, 2, 3] ; [4, 5] ; [6]>");
    }

    #[test]
    fn prod_of_prims() {
        let p = prod(vec![
            from_vec::<u64>(vec![1, 2]),
            from_vec::<u8>(vec![10, 20]),
        ]);
        assert_eq!(pretty(&p), "(P64[1, 2], P8[10, 20])");
    }
}
