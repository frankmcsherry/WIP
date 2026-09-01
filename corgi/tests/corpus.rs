//! The example corpus: `programs/*.col` files, each a self-contained `ml` program that GENERATES
//! its own data with `iota` (seeded only by a size). A `# n = …` header gives the seed and `# =`
//! the expected `show` output; everything else is the program. This test runs each (`run_partial`) and
//! golden-checks it: a total program shows its pure value; a partial program (an un-`try`'d fallible
//! stage) shows its output as the `Sum{T | Unit}` a trailing `try` would reveal.
//! Re-bless the goldens after an intended change with `CORGI_BLESS=1 cargo test --test corpus`.

use corgi::{show, Program, Shape, Value};
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

/// Programs skipped for a known reason (none).
const PENDING: &[&str] = &[];

#[test]
fn corpus_matches_goldens() {
    let bless = std::env::var("CORGI_BLESS").is_ok();
    let files = col_files();
    assert!(!files.is_empty(), "no .col programs found");
    for path in files {
        let who = path.file_name().unwrap().to_string_lossy().into_owned();
        if PENDING.contains(&who.as_str()) {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let (n, expected, prog) = parse_col(&text);
        let p = Program::compile_ml(&prog).unwrap_or_else(|e| panic!("{who}: parse: {e}"));
        p.check();
        // the lowered program is well-typed in the pure vocabulary — the typer covers effects.
        p.shape(&Shape::Prim(64)).unwrap_or_else(|e| panic!("{who}: shape: {e}"));
        let got = show(&p.run_partial(Value::u64(vec![n])));
        if bless {
            let blessed: Vec<String> = text
                .lines()
                .map(|l| if l.starts_with("# =") { format!("# = {got}") } else { l.to_string() })
                .collect();
            std::fs::write(&path, blessed.join("\n") + "\n").unwrap();
        } else {
            assert_eq!(got, expected, "{who}: output mismatch");
        }
    }
}
