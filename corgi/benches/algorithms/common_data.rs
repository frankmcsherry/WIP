//! Column builders the data-processing cases share: lists of fixed-width `u64` tuples.

use corgi::{Bounds, Value};

/// a `List<(U64, .., U64)>` column of `N`-tuples (`N > 1`), one row per `Vec`.
pub fn tuple_lists<const N: usize>(rows: &[Vec<[u64; N]>]) -> Value {
    let mut ends = Vec::with_capacity(rows.len());
    let mut fields: Vec<Vec<u64>> = vec![Vec::new(); N];
    for r in rows {
        for t in r {
            for (f, &x) in fields.iter_mut().zip(t) {
                f.push(x);
            }
        }
        ends.push(fields[0].len());
    }
    Value::List(Bounds::offsets(ends), Box::new(Value::Prod(fields.into_iter().map(Value::u64).collect())))
}
