//! collie binary: thin runner over the `collie` library. The language
//! itself lives in `lib.rs` (and `ir/`, `ops/`, `syntax/`). This file
//! only handles argv dispatch and the `tools/` modules that provide the
//! binary's features (REPL, bench, pretty-printer, examples runner).

use collie::pipeline::{build, eval_graph};
use collie::ir::op::eval as legacy_eval;
use collie::ir::typecheck::typecheck;
use collie::ir::shape::Shape;
use collie::ir::value::Value;
use collie::syntax::{parse, registry};
use collie::tools;

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    // Optional `--legacy` flag (anywhere in argv) selects the pre-graph
    // pipeline. Useful for A/B perf comparisons against the graph path.
    let use_legacy = args.iter().any(|a| a == "--legacy");
    let elide = args.iter().any(|a| a == "--elide");
    let mut args_iter = args.iter().skip(1)
        .filter(|a| a.as_str() != "--legacy" && a.as_str() != "--elide");
    match args_iter.next().map(|s| s.as_str()) {
        Some("bench") => tools::bench::run_bench(),
        Some("repl") => tools::repl::run(),
        Some("examples") => tools::examples_runner::run_all(),
        Some("graph") => match args_iter.next() {
            Some(path) => dump_graph(path, elide),
            None => Err("graph: expected a .col path".into()),
        },
        Some(path) if path.ends_with(".col") || std::path::Path::new(path).exists() => {
            run_script(path, use_legacy)
        }
        _ => {
            tools::examples_runner::run_all()?;
            tools::demos::run_extra(tools::ops_extra::register)
        }
    }
}

/// Print the built term graph for a script: each term as
/// `tN: op(child, …) -> outputs`, then roots. With `--elide` (passed as
/// the flag handled in main), routing ops are resolved away first.
fn dump_graph(path: &str, elide: bool) -> Result<(), String> {
    use collie::pipeline::{build, elide_routing, cse};
    let reg = registry::OpRegistry::standard();
    let prog = parse::parse_file(std::path::Path::new(path), &reg)?;
    let (g, _shapes) = build(prog)?;
    let raw_terms = g.terms.len();
    // With --elide: run the optimize stage (routing elision then CSE),
    // which is where routing-resolution exposes mergeable subexpressions.
    let (g, cse_hits) = if elide {
        let (g, hits) = cse(elide_routing(g));
        (g, Some(hits))
    } else {
        (g, None)
    };
    for (i, term) in g.terms.iter().enumerate() {
        let args: Vec<String> = term.children.iter()
            .map(|c| if c.idx == 0 { format!("t{}", c.term) } else { format!("t{}.{}", c.term, c.idx) })
            .collect();
        println!("t{:<4} {:<14} ({})", i, term.op.name(), args.join(", "));
    }
    let roots: Vec<String> = g.roots.iter()
        .map(|r| if r.idx == 0 { format!("t{}", r.term) } else { format!("t{}.{}", r.term, r.idx) })
        .collect();
    println!("roots: [{}]", roots.join(", "));
    match cse_hits {
        Some(h) => println!("terms: {} (was {} pre-optimize), cse merges: {}", g.terms.len(), raw_terms, h),
        None => println!("terms: {}", g.terms.len()),
    }
    Ok(())
}

fn run_script(path: &str, use_legacy: bool) -> Result<(), String> {
    let reg = registry::OpRegistry::standard();
    let prog = parse::parse_file(std::path::Path::new(path), &reg)?;
    let stack: Vec<Value> = if use_legacy {
        let mut ts: Vec<Shape> = Vec::new();
        let mut tenv: Vec<Shape> = Vec::new();
        typecheck(&prog, &mut ts, &mut tenv)?;
        let mut st: Vec<Value> = Vec::new();
        let mut env: Vec<Value> = Vec::new();
        legacy_eval(&prog, &mut st, &mut env)?;
        st
    } else {
        let (graph, _shapes) = build(prog)?;
        let mut env: Vec<Value> = Vec::new();
        eval_graph(&graph, &mut env)?
    };
    println!("{}", path);
    if stack.is_empty() {
        println!("  (stack empty)");
    } else {
        let total = stack.len();
        for (i, v) in stack.iter().enumerate() {
            let depth = total - 1 - i;
            println!("  [{}] {}", depth, tools::pretty::pretty(v));
        }
    }
    Ok(())
}
