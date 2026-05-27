//! Demos that need Rust glue (and so can't live as plain `.col` files in
//! examples/). Today this is just the `ops_extra` registration showcase,
//! which exercises the registry-extension story from outside the standard
//! op set. The serialization round-trip moved to a unit test in
//! `tools/serialize.rs`.

use crate::pipeline::{build, eval_graph};
use crate::syntax::parse::parse;
use crate::syntax::registry::OpRegistry;

/// Run a demo that exercises an *external* op family registered via the
/// passed-in `register` function. The core, type checker, parser, and
/// standard operators have no knowledge of these ops.
pub fn run_extra<F: Fn(&mut OpRegistry)>(register: F) -> Result<(), String> {
    let mut reg = OpRegistry::standard();
    register(&mut reg);  // bolt the extra ops on top of the standard registry

    println!("--- external ops: square + clamp via ops_extra ---");
    let src = "i32[-3 -2 -1 0 1 2 3 4 5 6 7] square.i32 0i32 30i32 clamp.i32";
    println!("program:  {}", src);
    let prog = parse(src, &reg)?;
    let (g, _shapes) = build(prog)?;
    let st = eval_graph(&g)?;
    println!("stack out: [");
    for v in &st { println!("  {}", v); }
    println!("]");
    println!("(the ops `square.i32` and `clamp.i32` live in src/ops_extra.rs —");
    println!(" outside src/ops/, with no core changes.)");
    println!();
    Ok(())
}
