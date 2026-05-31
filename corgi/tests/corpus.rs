//! The example corpus: `programs/*.col` files, each a self-contained `ml` program that GENERATES
//! its own data with `iota` (seeded only by a size). A `# n = …` header gives the seed and `# =`
//! the expected `show` output; everything else is the program. This test runs each and golden-checks
//! it; `examples/tour.rs` displays the SAME files — one source of truth for "how each thing is used".

use corgi::{shape_of_value, show, Program, Value};
use std::path::{Path, PathBuf};

/// the `.col` files under `programs/`, sorted by name.
pub fn col_files() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("programs");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("programs/ dir")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "col"))
        .collect();
    files.sort();
    files
}

/// parse a `.col` file into (seed `n`, expected output, program). `#` lines are headers/comments,
/// stripped before the program reaches the parser; body lines are joined into one program string.
pub fn parse_col(text: &str) -> (u64, String, String) {
    let mut n = 8;
    let mut expected = String::new();
    let mut prog = String::new();
    for line in text.lines() {
        if let Some(r) = line.strip_prefix("# n =") {
            n = r.trim().parse().expect("bad `# n =` header");
        } else if let Some(r) = line.strip_prefix("# =") {
            expected = r.trim().to_string();
        } else if !line.trim_start().starts_with('#') && !line.trim().is_empty() {
            prog.push_str(line.trim());
            prog.push(' ');
        }
    }
    (n, expected, prog.trim().to_string())
}

#[test]
fn corpus_matches_goldens() {
    let files = col_files();
    assert!(!files.is_empty(), "no .col programs found");
    for path in files {
        let who = path.file_name().unwrap().to_string_lossy().into_owned();
        let (n, expected, prog) = parse_col(&std::fs::read_to_string(&path).unwrap());
        let p = Program::compile_ml(&prog).unwrap_or_else(|e| panic!("{who}: parse: {e}"));
        p.check();
        let seed = Value::u64(vec![n]);
        let out = p.run(seed.clone());
        let inferred = p.shape(&shape_of_value(&seed)).unwrap_or_else(|e| panic!("{who}: type: {e}"));
        assert_eq!(inferred, shape_of_value(&out), "{who}: typer disagrees with evaluator");
        assert_eq!(show(&out), expected, "{who}: output mismatch");
    }
}
