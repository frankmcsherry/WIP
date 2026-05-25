//! Walk the `examples/` directory, run each `.col` file, and print results
//! with the pretty-printer. Used by `cargo run --release` to demonstrate the
//! language without anyone editing Rust source.
//!
//! Each example is run on a fresh empty stack. Errors are caught and reported;
//! the runner continues to the next file rather than bailing.

use std::path::{Path, PathBuf};

use crate::pipeline::{build, eval_graph};
use crate::syntax::parse::parse_file;
use crate::tools::pretty::pretty;
use crate::syntax::registry::OpRegistry;
use crate::ir::shape::shape_of;
use crate::ir::value::Value;

const EXAMPLES_DIR: &str = "examples";

pub fn run_all() -> Result<(), String> {
    let reg = OpRegistry::standard();
    let files = collect_col_files(Path::new(EXAMPLES_DIR))?;
    if files.is_empty() {
        println!("no .col files found in {}/", EXAMPLES_DIR);
        return Ok(());
    }
    let mut pass = 0usize;
    let mut fail = 0usize;
    for path in &files {
        match run_one(&reg, path) {
            Ok(()) => pass += 1,
            Err(e) => { fail += 1; eprintln!("  error: {}\n", e); }
        }
    }
    println!("\n{} examples — {} passed, {} failed", files.len(), pass, fail);
    if fail > 0 { Err(format!("{} examples failed", fail)) } else { Ok(()) }
}

fn run_one(reg: &OpRegistry, path: &Path) -> Result<(), String> {
    println!("--- {} ---", path.display());
    let prog = parse_file(path, reg)?;
    // build does both shape-inference (used as typecheck) and graph
    // construction; errors here are the equivalent of typecheck errors.
    let (graph, _shapes) = build(prog).map_err(|e| format!("build: {}", e))?;
    let mut env: Vec<Value> = Vec::new();
    let stack = eval_graph(&graph, &mut env).map_err(|e| format!("eval: {}", e))?;
    if stack.is_empty() {
        println!("  (stack empty)");
    } else {
        let total = stack.len();
        for (i, v) in stack.iter().enumerate() {
            let depth = total - 1 - i;
            let stack_shape = shape_of(v);
            println!("  [{}] : {}", depth, stack_shape);
            println!("       {}", pretty(v));
        }
    }
    println!();
    Ok(())
}

fn collect_col_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let read = std::fs::read_dir(dir)
        .map_err(|e| format!("read_dir {}: {}", dir.display(), e))?;
    let mut paths: Vec<PathBuf> = read
        .filter_map(|entry| entry.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |x| x == "col"))
        .collect();
    paths.sort();
    Ok(paths)
}
