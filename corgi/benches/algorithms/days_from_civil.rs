//! Days since 1970-01-01 and weekday of a civil date (Howard Hinnant's `days_from_civil`).

use crate::common::{run_case, Cfg, Rng};
use corgi::Value;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn weekday(z: i64) -> i64 {
    if z >= -4 { (z + 4) % 7 } else { (z + 5) % 7 + 6 }
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(10);
    let dates: Vec<(i64, i64, i64)> =
        (0..cfg.rows).map(|_| (1900 + rng.below(201), 1 + rng.below(12), 1 + rng.below(28))).collect();
    let input = Value::Prod(vec![
        Value::i64(dates.iter().map(|d| d.0).collect()),
        Value::i64(dates.iter().map(|d| d.1).collect()),
        Value::i64(dates.iter().map(|d| d.2).collect()),
    ]);
    let days: Vec<i64> = dates.iter().map(|&(y, m, d)| days_from_civil(y, m, d)).collect();
    let expected = Value::Prod(vec![
        Value::i64(days.clone()),
        Value::i64(days.iter().map(|&z| weekday(z)).collect()),
    ]);
    let rust = || dates.iter().map(|&(y, m, d)| { let z = days_from_civil(y, m, d); (z, weekday(z)) }).collect::<Vec<_>>();
    run_case(cfg, "days_from_civil", "dates in 1900-2100", include_str!("../../algorithms/days_from_civil.col"), input, expected, rust);
}
