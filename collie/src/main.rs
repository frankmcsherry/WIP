//! collie binary: thin runner over the `collie` library. The language
//! itself lives in `lib.rs` (and `ir/`, `ops/`, `syntax/`). This file
//! only handles argv dispatch and the `tools/` modules that provide the
//! binary's features (bench, pretty-printer, examples runner).

use collie::pipeline::{build, eval_graph, optimize};
use collie::ir::value::Value;
use collie::syntax::{parse, registry};
use collie::tools;

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    // `--no-opt` runs the graph engine without the optimizer (the
    // `Graph → Graph` passes are never load-bearing for execution; see
    // dev/LAYERING.md). The graph engine is the only evaluator.
    let no_opt = args.iter().any(|a| a == "--no-opt");
    let elide = args.iter().any(|a| a == "--elide");
    let mut args_iter = args.iter().skip(1)
        .filter(|a| !matches!(a.as_str(), "--no-opt" | "--elide"));
    match args_iter.next().map(|s| s.as_str()) {
        Some("bench") => tools::bench::run_bench(),
        Some("examples") => tools::examples_runner::run_all(),
        Some("graph") => match args_iter.next() {
            Some(path) => dump_graph(path, elide),
            None => Err("graph: expected a .col path".into()),
        },
        Some(path) if path.ends_with(".col") || std::path::Path::new(path).exists() => {
            run_script(path, no_opt)
        }
        _ => {
            tools::examples_runner::run_all()?;
            tools::demos::run_extra(tools::ops_extra::register)
        }
    }
}

/// Print the built term graph for a script (the `--emit-ir` view): each
/// term as `tN: op(child, …) -> outputs`, then roots. With `--elide`, runs
/// the full default `optimize` pipeline first, so the dump matches the
/// graph the engine actually executes.
fn dump_graph(path: &str, elide: bool) -> Result<(), String> {
    let reg = registry::OpRegistry::standard();
    let prog = parse::parse_file(std::path::Path::new(path), &reg)?;
    let (g, _shapes) = build(prog)?;
    let raw_terms = g.terms.len();
    let (g, optimized) = if elide { (optimize(g), true) } else { (g, false) };
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
    if optimized {
        println!("terms: {} (was {} pre-optimize)", g.terms.len(), raw_terms);
    } else {
        println!("terms: {}", g.terms.len());
    }
    Ok(())
}

fn run_script(path: &str, no_opt: bool) -> Result<(), String> {
    let reg = registry::OpRegistry::standard();
    let prog = parse::parse_file(std::path::Path::new(path), &reg)?;
    let (graph, _shapes) = build(prog)?;
    let graph = if no_opt { graph } else { optimize(graph) };
    let stack: Vec<Value> = eval_graph(&graph)?;
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
