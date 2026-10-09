//! Column builders the data-processing cases share: lists of `Int` tuples.

use corgi::{Bounds, Value};

/// a `List<(Int, .., Int)>` column of `N`-tuples (`N > 1`), one row per `Vec`.
pub fn tuple_lists<const N: usize>(rows: &[Vec<[i64; N]>]) -> Value {
    let mut ends = Vec::with_capacity(rows.len());
    let mut fields: Vec<Vec<i64>> = vec![Vec::new(); N];
    for r in rows {
        for t in r {
            for (f, &x) in fields.iter_mut().zip(t) {
                f.push(x);
            }
        }
        ends.push(fields[0].len());
    }
    Value::List(Bounds::offsets(ends), Box::new(Value::Prod(fields.into_iter().map(Value::i64).collect())))
}
