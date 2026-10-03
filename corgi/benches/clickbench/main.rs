//! ClickBench's queries written in corgi, checked against DuckDB's answers and timed against DuckDB.
//!
//!   python3 benches/clickbench/prepare.py DIR hits_0.parquet      # the data, DuckDB's answers and times
//!   CORGI_CLICKBENCH=DIR cargo bench --bench clickbench [-- NAME ...] [--check]
//!
//! Each query is `algorithms/clickbench/NAME.col`, whose header names the SQL it answers (`# sql:`), the
//! columns it reads (`# columns:`, the program's input in that order) and its output's kinds
//! (`# output:`: `u` unsigned, `i` signed, `f` float, `s` string; `[..]` for a list of rows). The whole
//! table is one row: each column is a one-row `List`.

use corgi::{dec_i64, Bounds, Program, Value};
use std::collections::HashMap;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct Query {
    name: String,
    src: String,
    columns: Vec<String>,
    output: String,
}

fn queries() -> Vec<Query> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("algorithms/clickbench");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir).expect("algorithms/clickbench").map(|e| e.unwrap().path()).collect();
    files.retain(|p| p.extension().is_some_and(|e| e == "col"));
    files.sort();
    files
        .into_iter()
        .map(|p| {
            let src = std::fs::read_to_string(&p).unwrap();
            let header = |key: &str| {
                src.lines().find_map(|l| l.strip_prefix(key)).unwrap_or_else(|| panic!("{}: no `{key}`", p.display())).trim().to_string()
            };
            let columns = header("# columns:").split_whitespace().map(String::from).collect();
            Query { name: p.file_stem().unwrap().to_string_lossy().into_owned(), output: header("# output:"), columns, src }
        })
        .collect()
}

fn read_u64s(path: &Path) -> Vec<u64> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    bytes.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect()
}

/// a column as a one-row list: numbers as `List<U64>`, strings as `List<List<U8>>`.
fn column(dir: &Path, name: &str) -> Value {
    let one_row = |v: Value| Value::List(Bounds::offsets(vec![v.len()]), Box::new(v));
    let numbers = dir.join(format!("{name}.u64"));
    if numbers.exists() {
        return one_row(Value::u64(read_u64s(&numbers)));
    }
    let ends: Vec<usize> = read_u64s(&dir.join(format!("{name}.ends"))).into_iter().map(|e| e as usize).collect();
    let bytes = std::fs::read(dir.join(format!("{name}.bytes"))).unwrap();
    one_row(Value::List(Bounds::offsets(ends), Box::new(Value::u8(bytes))))
}

fn dec_f64(u: u64) -> f64 {
    f64::from_bits(if u >> 63 == 1 { u ^ (1 << 63) } else { !u })
}

/// field `j` of a column, in the canonical text form `prepare.py` writes DuckDB's answers in.
fn field(kind: &str, col: &Value, j: usize) -> String {
    if kind == "s" {
        let Value::List(bounds, bytes) = col else { panic!("a string column is a List<U8>") };
        let ends = bounds.to_vec();
        let (s, e) = (if j == 0 { 0 } else { ends[j - 1] }, ends[j]);
        return bytes.as_u8("string")
            .map(|b| b[s..e].iter().map(|x| format!("{x:02x}")).collect())
            .unwrap_or_else(|_| panic!("a string is a List<U8>"));
    }
    let x = col.as_u64("number").unwrap_or_else(|e| panic!("{e}"))[j];
    match kind {
        "u" => x.to_string(),
        "i" => dec_i64(x).to_string(),
        "f" => format!("{:.4}", dec_f64(x)),
        other => panic!("unknown output kind {other}"),
    }
}

/// the output as text: one line per row, fields tab-separated. `[k1,k2]` is a list (of the one input
/// row) whose elements are rows; `k1,k2` is one row of fields.
fn canonical(output: &str, v: Value) -> String {
    let (list, kinds) = match output.strip_prefix('[').and_then(|o| o.strip_suffix(']')) {
        Some(inner) => (true, inner),
        None => (false, output),
    };
    let kinds: Vec<&str> = kinds.split(',').collect();
    let (rows, v) = if list {
        let (bounds, vals) = v.into_list("query output").unwrap();
        (bounds.to_vec().last().copied().unwrap_or(0), vals)
    } else {
        (v.len(), v)
    };
    let cols = if kinds.len() == 1 { vec![v] } else { v.into_prod("query output").unwrap() };
    let mut out = String::new();
    for j in 0..rows {
        let fields: Vec<String> = kinds.iter().zip(&cols).map(|(k, c)| field(k, c, j)).collect();
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
    out
}

fn best(mut f: impl FnMut()) -> Duration {
    let (mut best, start, mut runs) = (Duration::MAX, Instant::now(), 0);
    while runs < 3 || (start.elapsed() < Duration::from_millis(1500) && runs < 50) {
        let t = Instant::now();
        f();
        best = best.min(t.elapsed());
        runs += 1;
    }
    best
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    let check = args.iter().any(|a| a == "--check");
    let names: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let Some(dir) = std::env::var_os("CORGI_CLICKBENCH").map(PathBuf::from) else {
        println!("clickbench: set CORGI_CLICKBENCH to a directory made by benches/clickbench/prepare.py");
        return;
    };
    let duckdb: HashMap<String, (String, String)> = std::fs::read_to_string(dir.join("duckdb.tsv"))
        .unwrap_or_default()
        .lines()
        .skip(1)
        .filter_map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            (f.len() == 3).then(|| (f[0].to_string(), (f[1].to_string(), f[2].to_string())))
        })
        .collect();
    let mut cache: HashMap<String, Value> = HashMap::new();
    println!("{:<6} {:>10} {:>12} {:>8} {:>12}", "query", "corgi ms", "duckdb 1t ms", "ratio", "duckdb all ms");
    for q in queries() {
        if !names.is_empty() && !names.iter().any(|n| q.name.contains(n.as_str())) {
            continue;
        }
        let cols: Vec<Value> = q.columns.iter().map(|c| cache.entry(c.clone()).or_insert_with(|| column(&dir, c)).clone()).collect();
        let input = if cols.len() == 1 { cols.into_iter().next().unwrap() } else { Value::Prod(cols) };
        let p = Program::compile_ml(&q.src).unwrap_or_else(|e| panic!("{}: {e}", q.name));
        let got = canonical(&q.output, p.run(input.clone()));
        let want = std::fs::read_to_string(dir.join("expected").join(format!("{}.txt", q.name))).unwrap_or_default();
        if got != want {
            let first = got.lines().zip(want.lines()).position(|(a, b)| a != b);
            println!("{:<6} DIFFERS from DuckDB (corgi {} lines, duckdb {}; first difference at line {first:?})", q.name, got.lines().count(), want.lines().count());
            for (a, b) in got.lines().zip(want.lines()).skip(first.unwrap_or(0)).take(3) {
                println!("       corgi  {a}\n       duckdb {b}");
            }
            continue;
        }
        if check {
            println!("{:<6} ok", q.name);
            continue;
        }
        let t = best(|| { black_box(p.run(black_box(input.clone()))); });
        let ms = t.as_secs_f64() * 1e3;
        let (d1, dn) = duckdb.get(&q.name).cloned().unwrap_or_default();
        let ratio = d1.parse::<f64>().map(|d| format!("{:.2}x", ms / d)).unwrap_or_default();
        println!("{:<6} {:>10.2} {:>12} {:>8} {:>12}", q.name, ms, d1, ratio, dn);
    }
}
