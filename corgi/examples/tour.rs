//! A tour of corgi: it loads the `programs/*.col` corpus — the SAME files `tests/corpus.rs`
//! golden-checks — and displays each (description, program, inferred type, output). Each program is
//! self-contained: it GENERATES its own data with `iota`, seeded only by the size in its `# n =`
//! header. Run with `cargo run --example tour`.

use corgi::{shape_of_value, show, Program, Value};
use std::path::Path;

/// parse a `.col` file into (seed `n`, program). `#` lines are headers; the rest is the program.
fn parse_col(text: &str) -> (u64, String) {
    let mut n = 8;
    let mut prog = String::new();
    for line in text.lines() {
        if let Some(r) = line.strip_prefix("# n =") {
            n = r.trim().parse().expect("bad `# n =` header");
        } else if !line.trim_start().starts_with('#') && !line.trim().is_empty() {
            prog.push_str(line.trim());
            prog.push(' ');
        }
    }
    (n, prog.trim().to_string())
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("programs");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("programs/ dir")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "col"))
        .collect();
    files.sort();

    println!("corgi tour — each program generates its own data via iota\n");
    for path in files {
        // describe from the filename: "03-map-arith" -> "map arith"
        let stem = path.file_stem().unwrap().to_string_lossy();
        let desc = stem.split_once('-').map_or(stem.as_ref(), |(_, r)| r).replace('-', " ");
        let (n, prog) = parse_col(&std::fs::read_to_string(&path).unwrap());
        let p = Program::compile_ml(&prog).expect("parse error");
        let seed = Value::u64(vec![n]);
        let ty = p.shape(&shape_of_value(&seed)).expect("type error");
        println!("• {desc}  (n = {n})");
        println!("    {prog}");
        println!("    : {ty}");
        println!("    = {}\n", show(&p.run(seed)));
    }
}
