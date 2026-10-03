//! Least-squares slope and intercept of each row's points, in `f64`.

use crate::common::{enc_f64, run_case, Cfg, Rng};
use corgi::{Bounds, Value};

fn fit(pts: &[(f64, f64)]) -> (f64, f64) {
    if pts.is_empty() {
        return (0.0, 0.0);
    }
    let n = pts.len() as f64;
    let (mut sx, mut sy, mut sxx, mut sxy) = (0.0, 0.0, 0.0, 0.0);
    for &(x, y) in pts {
        sx += x;
        sy += y;
        sxx += x * x;
        sxy += x * y;
    }
    let den = n * sxx - sx * sx;
    let slope = if den == 0.0 { 0.0 } else { (n * sxy - sx * sy) / den };
    (slope, (sy - slope * sx) / n)
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(13);
    let rows: Vec<Vec<(f64, f64)>> = (0..cfg.list_rows())
        .map(|_| {
            let n = cfg.list_len(&mut rng, 33);
            // x often repeats, so some rows have no spread; y is noisy around a line.
            (0..n).map(|_| { let x = rng.below(8) as f64; (x, 3.0 * x + rng.below(20) as f64) }).collect()
        })
        .collect();
    let mut ends = Vec::new();
    let (mut xs, mut ys) = (Vec::new(), Vec::new());
    for r in &rows {
        for &(x, y) in r {
            xs.push(enc_f64(x));
            ys.push(enc_f64(y));
        }
        ends.push(xs.len());
    }
    let input = Value::List(Bounds::offsets(ends), Box::new(Value::Prod(vec![Value::u64(xs), Value::u64(ys)])));
    let out: Vec<(f64, f64)> = rows.iter().map(|r| fit(r)).collect();
    let expected = Value::Prod(vec![
        Value::u64(out.iter().map(|o| enc_f64(o.0)).collect()),
        Value::u64(out.iter().map(|o| enc_f64(o.1)).collect()),
    ]);
    let rust = || rows.iter().map(|r| fit(r)).collect::<Vec<_>>();
    run_case(cfg, "linear_regression", "0-32 points, f64", include_str!("../../algorithms/linear_regression.col"), input, expected, rust);
}
