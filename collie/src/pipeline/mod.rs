//! The term-graph IR and the compile-and-run pipeline over it.
//!
//! This module owns both the IR vocabulary — `graph` (Term/Graph) and
//! `sysop` (the SystemOp operator enum) — and the stages that transform a
//! program through it. It sits *above* `ops/` (kernels) and `ir/` (the
//! value model): a graph is built out of operators, so it lives above them.
//!
//! Stages, each a plain function taking the graph (or parsed ops) and
//! returning the graph (or a result):
//!
//! ```text
//! parsed ops ──lower::build──▶ Graph ──optimize::{elide_routing,cse,
//!              eliminate_dead}──▶ Graph ──execute::eval_graph──▶ Vec<Value>
//! ```
//!
//! `ir/` holds the *vocabulary* (Value, Shape, Graph, Op); this module
//! holds the *stages* that transform a program through it. No stage
//! reaches backward — each consumes what it needs and hands the graph on.

pub mod graph;
pub mod sysop;
pub mod lower;
pub mod optimize;
pub mod execute;

pub use lower::build;
pub use optimize::{cse, elide_routing, eliminate_dead};
pub use execute::{eval_graph, use_counts};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::Value;
    use crate::syntax::registry::OpRegistry;
    use crate::syntax::parse::parse;
    use crate::ir::op::eval as legacy_eval;

    /// Run via legacy eval; return the final stack.
    fn legacy(src: &str) -> Result<Vec<Value>, String> {
        let reg = OpRegistry::standard();
        let prog = parse(src, &reg)?;
        let mut st: Vec<Value> = Vec::new();
        let mut env: Vec<Value> = Vec::new();
        legacy_eval(&prog, &mut st, &mut env)?;
        Ok(st)
    }

    /// Build a graph, evaluate, return the final stack.
    fn via_graph(src: &str) -> Result<Vec<Value>, String> {
        let reg = OpRegistry::standard();
        let prog = parse(src, &reg)?;
        let (g, _shapes) = build(prog)?;
        let mut env: Vec<Value> = Vec::new();
        eval_graph(&g, &mut env)
    }

    /// Assert legacy and graph paths produce the same stack.
    fn agree(src: &str) {
        let a = legacy(src).expect("legacy eval failed");
        let b = via_graph(src).expect("graph eval failed");
        assert_eq!(a, b, "legacy vs graph diverged on:\n  {}", src);
    }

    #[test]
    fn flat_arith_chain() {
        agree("u64[1 2 3 4 5] u64[10 20 30 40 50] +.u64");
    }

    #[test]
    fn flat_reduce() {
        agree("u64[1 2 3 4 5] reduce.+.u64");
    }

    #[test]
    fn multi_output_intersect_then_drop() {
        agree("u64[1 2 3 5 7] u64[2 3 4 5 6] intersect drop");
    }

    #[test]
    fn structural_zip_detuple_roundtrip() {
        agree("u64[1 2 3] u64[10 20 30] entuple.2 detuple.2");
    }

    #[test]
    fn projection_field_one() {
        agree("u64[1 2 3] u64[10 20 30] entuple.2 .0");
    }

    #[test]
    fn let_block_resolves_to_data_edges() {
        agree("u64[1 2 3] :[a] a a +.u64");
    }

    #[test]
    fn binding_clone_and_inferred_take() {
        agree("u64[1 2 3] :x  x x +.u64");
        // last-use of `x` is auto-inferred as a take (and the graph derives
        // it from use-counting); no explicit take syntax needed.
        agree("u64[1 2 3] :x  x u64[10 20 30] +.u64");
    }

    #[test]
    fn each_body_stays_opaque() {
        agree("u64[1 2 3 4 5] u64[0 2 5] nest each { reduce.+.u64 }");
    }

    #[test]
    fn match_body_stays_opaque() {
        agree("u8[0 1 0] u64[10 20] u64[100] inject2 match { -> 1u64 like -> 1u64 like }");
    }

    #[test]
    fn cse_dedupes_redundant_subexpressions() {
        // build is pure 1:1, so CSE runs as a separate pass.
        // Raw: 2 lits + add, twice, + outer add = 7 terms.
        // After cse: lit, lit, inner add (all shared) + outer add = 4 terms.
        let src = "u64[1 2 3] u64[10 20 30] +.u64  u64[1 2 3] u64[10 20 30] +.u64  +.u64";
        let reg = OpRegistry::standard();
        let (g, _) = build(parse(src, &reg).unwrap()).unwrap();
        assert_eq!(g.terms.len(), 7, "build should be 1:1 (no CSE)");
        let (g2, hits) = cse(g);
        assert!(hits > 0, "cse should merge the duplicate sub-tree");
        assert!(g2.terms.len() <= 4, "after cse expected ≤4 terms, got {}", g2.terms.len());
        let mut env: Vec<Value> = Vec::new();
        assert_eq!(eval_graph(&g2, &mut env).unwrap(), legacy(src).unwrap());
    }

    #[test]
    fn dead_term_elim_drops_unused_ops() {
        let src = "u64[1 2 3] u64[10 20 30] +.u64 \
                   u64[100 200 300] u64[1000 2000 3000] +.u64 \
                   drop";
        let reg = OpRegistry::standard();
        let (g, _) = build(parse(src, &reg).unwrap()).unwrap();
        let pre = g.terms.len();
        let g2 = eliminate_dead(g);
        assert!(g2.terms.len() < pre, "dead-term-elim should remove terms: {} → {}", pre, g2.terms.len());
        let mut env: Vec<Value> = Vec::new();
        assert_eq!(eval_graph(&g2, &mut env).unwrap(), legacy(src).unwrap());
    }

    #[test]
    fn dead_term_elim_preserves_side_effects() {
        let reg = OpRegistry::standard();
        let (g, _) = build(parse("time u64[1 2 3] reduce.+.u64", &reg).unwrap()).unwrap();
        let g2 = eliminate_dead(g);
        assert!(g2.terms.iter().any(|t| t.op.name() == "time"), "time wrongly eliminated");
    }

    #[test]
    fn cse_skips_side_effecting() {
        let reg = OpRegistry::standard();
        let (g, _) = build(parse("time u64[1] reduce.+.u64 time", &reg).unwrap()).unwrap();
        let (g2, _) = cse(g);
        let n_time = g2.terms.iter().filter(|t| t.op.name() == "time").count();
        assert_eq!(n_time, 2, "cse wrongly merged side-effecting time ops; got {}", n_time);
    }

    /// Per-example term counts through the optimize pipeline. Run with
    /// `-- --nocapture` to see the table.
    #[test]
    fn transform_corpus_stats() {
        let reg = OpRegistry::standard();
        let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir("examples")
            .expect("read examples/").filter_map(|e| e.ok()).map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "col")).collect();
        paths.sort();
        let (mut t_raw, mut t_elide, mut t_cse, mut t_dce) = (0, 0, 0, 0);
        println!();
        println!("{:<40} {:>6} {:>8} {:>6} {:>6}", "file", "raw", "+elide", "+cse", "+dce");
        println!("{}", "-".repeat(70));
        for path in &paths {
            let prog = crate::syntax::parse::parse_file(path, &reg).expect("parse");
            let (g, _shapes) = build(prog).expect("build");
            let raw = g.terms.len();
            let g = elide_routing(g);
            let elide = g.terms.len();
            let (g, _hits) = cse(g);
            let cse_n = g.terms.len();
            let g = eliminate_dead(g);
            let dce = g.terms.len();
            t_raw += raw; t_elide += elide; t_cse += cse_n; t_dce += dce;
            println!("{:<40} {:>6} {:>8} {:>6} {:>6}",
                     path.file_name().unwrap().to_string_lossy(), raw, elide, cse_n, dce);
        }
        println!("{}", "-".repeat(70));
        println!("{:<40} {:>6} {:>8} {:>6} {:>6}", "TOTAL", t_raw, t_elide, t_cse, t_dce);
        println!();
    }

    #[test]
    fn build_is_routing_free_by_construction() {
        // Routing ops are resolved during lowering, so a built graph never
        // contains dup/drop/swap/over/rot/pick/roll — and elide_routing is
        // therefore idempotent on it. Example 08 is routing-heavy in source.
        let src = std::fs::read_to_string("examples/08_sort_merge_equijoin.col").unwrap();
        let reg = OpRegistry::standard();
        let (g, _) = build(parse(&src, &reg).unwrap()).unwrap();
        assert!(
            g.terms.iter().all(|t| t.op.routing_map().is_none()),
            "built graph still contains routing terms",
        );
        let pre = g.terms.len();
        let g2 = elide_routing(g);
        assert_eq!(g2.terms.len(), pre, "elide_routing should be a no-op on a built graph");
        let mut env: Vec<Value> = Vec::new();
        assert_eq!(eval_graph(&g2, &mut env).unwrap(), legacy(&src).unwrap());
    }

    #[test]
    fn elide_routing_corpus_preserves_results() {
        let reg = OpRegistry::standard();
        let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir("examples")
            .unwrap().filter_map(|e| e.ok()).map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "col")).collect();
        paths.sort();
        for path in &paths {
            let src = std::fs::read_to_string(path).unwrap();
            let (g, _) = build(parse(&src, &reg).unwrap()).unwrap();
            let g2 = elide_routing(g);
            let mut env: Vec<Value> = Vec::new();
            assert_eq!(eval_graph(&g2, &mut env).unwrap(), legacy(&src).unwrap(),
                       "elided diverged from legacy on {}", path.display());
        }
    }

    #[test]
    fn wco_small_smoke() {
        let src = std::fs::read_to_string("examples/18_wco_lftj_def.col").unwrap();
        let src = src.replace("1000000u64", "10u64");
        agree(&src);
    }
}
