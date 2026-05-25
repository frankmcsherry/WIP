//! Interactive REPL — minimal, zero external deps.
//!
//! Read a line, parse against the standard registry, typecheck against the
//! current stack's shapes; if both succeed, eval on a clone and replace the
//! persistent stack. Any error (parse / typecheck / runtime) leaves the stack
//! unchanged. Lines starting with `:` are meta-commands. Lines with unclosed
//! `{` / `[` (the structural openers) continue on the next line.
//!
//! Run with: `cargo run --release -- repl`

use std::io::{self, BufRead, Write};

use crate::ir::op::eval;
use crate::syntax::parse::parse;
use crate::tools::pretty::pretty;
use crate::syntax::registry::OpRegistry;
use crate::ir::shape::{shape_of, Shape};
use crate::ir::typecheck::typecheck;
use crate::ir::value::Value;

pub fn run() -> Result<(), String> {
    // Suppress the default panic backtrace — we catch the panic ourselves
    // and turn it into a user-visible error message.
    std::panic::set_hook(Box::new(|_| {}));

    let reg = OpRegistry::standard();
    let mut stack: Vec<Value> = Vec::new();
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    println!("collie REPL — :help for commands, :quit to exit");
    println!();

    let mut buf = String::new();
    loop {
        let prompt = if buf.is_empty() { "> " } else { "... " };
        print!("{}", prompt);
        stdout.flush().ok();

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => { println!(); break; }   // EOF (Ctrl-D)
            Ok(_) => {}
            Err(e) => { eprintln!("read error: {}", e); break; }
        }

        // Meta commands only at the start of a fresh (non-continuation) entry.
        if buf.is_empty() {
            let t = line.trim_end();
            if t.starts_with(':') {
                match handle_meta(t, &mut stack, &reg) {
                    MetaResult::Quit => break,
                    MetaResult::Ok => {}
                }
                continue;
            }
        }

        buf.push_str(&line);
        if brace_depth(&buf) > 0 {
            continue;
        }

        let src = std::mem::take(&mut buf);
        let src = src.trim();
        if src.is_empty() { continue; }

        match eval_program(&reg, &mut stack, src) {
            Ok(()) => print_stack(&stack),
            Err(e) => eprintln!("  error: {}", e),
        }
    }
    Ok(())
}

/// Parse + typecheck + eval. Any failure (including a panic from a bug or an
/// op that didn't validate) leaves `stack` unchanged.
fn eval_program(reg: &OpRegistry, stack: &mut Vec<Value>, src: &str) -> Result<(), String> {
    let prog = parse(src, reg)?;
    let mut ts: Vec<Shape> = stack.iter().map(shape_of).collect();
    let mut tenv: Vec<Shape> = Vec::new();
    typecheck(&prog, &mut ts, &mut tenv)?;
    let work_in = stack.clone();
    // Wrap eval in catch_unwind so a panic doesn't kill the REPL session.
    // Panics here indicate an op that should have returned Err — but until
    // they're all hardened, the REPL stays alive and the stack stays clean.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut work = work_in;
        let mut env: Vec<Value> = Vec::new();
        eval(&prog, &mut work, &mut env).map(|()| work)
    }));
    match result {
        Ok(Ok(work)) => { *stack = work; Ok(()) }
        Ok(Err(e))   => Err(e),
        Err(panic)   => Err(format!("internal panic in op (please report): {}",
            panic_message(&panic))),
    }
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() { return (*s).to_string(); }
    if let Some(s) = payload.downcast_ref::<String>() { return s.clone(); }
    "<non-string panic payload>".to_string()
}

fn print_stack(stack: &[Value]) {
    if stack.is_empty() {
        println!("  (stack empty)");
        return;
    }
    // Print from bottom (highest depth) to top ([0]).
    let total = stack.len();
    for (i, v) in stack.iter().enumerate() {
        let depth = total - 1 - i;
        println!("  [{}] {}", depth, pretty(v));
    }
}

fn print_types(stack: &[Value]) {
    if stack.is_empty() { println!("  (stack empty)"); return; }
    let total = stack.len();
    for (i, v) in stack.iter().enumerate() {
        let depth = total - 1 - i;
        println!("  [{}] : {}", depth, shape_of(v));
    }
}

enum MetaResult { Quit, Ok }

fn handle_meta(cmd: &str, stack: &mut Vec<Value>, reg: &OpRegistry) -> MetaResult {
    let mut it = cmd.split_whitespace();
    let head = it.next().unwrap_or("");
    let args: Vec<&str> = it.collect();
    match head {
        ":q" | ":quit" | ":exit" => return MetaResult::Quit,
        ":h" | ":help" => print_help(),
        ":s" | ":stack" => print_stack(stack),
        ":t" | ":types" => print_types(stack),
        ":c" | ":clear" => { stack.clear(); println!("  (cleared)"); }
        ":d" | ":drop" => {
            let n = args.first().and_then(|s| s.parse::<usize>().ok()).unwrap_or(1);
            let n = n.min(stack.len());
            for _ in 0..n { stack.pop(); }
            print_stack(stack);
        }
        ":l" | ":load" | ":run" => {
            let path = match args.first() {
                Some(p) => *p,
                None => { eprintln!("  {}: need a file path", head); return MetaResult::Ok; }
            };
            match std::fs::read_to_string(path) {
                Ok(src) => match eval_program(reg, stack, &src) {
                    Ok(()) => print_stack(stack),
                    Err(e) => eprintln!("  error: {}", e),
                }
                Err(e) => eprintln!("  read {}: {}", path, e),
            }
        }
        other => eprintln!("  unknown command: {}  (try :help)", other),
    }
    MetaResult::Ok
}

fn print_help() {
    println!("  Meta commands (must start the line):");
    println!("    :help / :h           this listing");
    println!("    :stack / :s          print stack values (also shown after eval)");
    println!("    :types / :t          print stack types only");
    println!("    :clear / :c          empty the stack");
    println!("    :drop [N] / :d [N]   drop top N items (default 1)");
    println!("    :load PATH / :l      read and evaluate a collie file");
    println!("    :quit / :q           exit (or Ctrl-D)");
    println!();
    println!("  Programs: any collie source. See src/demos.rs for 28 examples.");
    println!("  Multi-line: unclosed `{{`/`[` continues on the next line.");
    println!();
    println!("  Quick examples:");
    println!("    > u64[1 2 3 4 5] reduce.+.u64");
    println!("    > u64[10 20 30] dup reduce.+.u64 swap count as.u64 /.u64");
    println!("    > u64[3 5 6] u64[1 2 3 4 5 6] nest each {{ reduce.+.u64 }}");
}

/// Count of unmatched `{` / `[` characters in `buf`. The collie tokenizer
/// treats `{|` and `.{` as composite single tokens, but for continuation
/// detection only the raw brace balance matters.
fn brace_depth(buf: &str) -> i32 {
    let mut depth: i32 = 0;
    for ch in buf.chars() {
        match ch {
            '{' | '[' => depth += 1,
            '}' | ']' => depth -= 1,
            _ => {}
        }
    }
    depth.max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brace_depth_basic() {
        assert_eq!(brace_depth(""), 0);
        assert_eq!(brace_depth("u64[1 2 3]"), 0);
        assert_eq!(brace_depth("{ :[a b]"), 1);
        assert_eq!(brace_depth("{ :[a b] a b +.u64 }"), 0);
        assert_eq!(brace_depth(".{ .0 ; .1"), 1);
        assert_eq!(brace_depth(".{ .0 ; .1 }"), 0);
    }

    #[test]
    fn eval_persists_stack() {
        let reg = OpRegistry::standard();
        let mut s: Vec<Value> = Vec::new();
        eval_program(&reg, &mut s, "u64[1 2 3]").unwrap();
        assert_eq!(s.len(), 1);
        eval_program(&reg, &mut s, "reduce.+.u64").unwrap();
        assert_eq!(s.len(), 1);
        // [6]
        match &s[0] {
            Value::Prim(p) => {
                use crate::ir::value::Storage;
                let xs = u64::extract(p).unwrap();
                assert_eq!(xs, &[6]);
            }
            _ => panic!("expected Prim"),
        }
    }

    #[test]
    fn eval_failure_rolls_back() {
        let reg = OpRegistry::standard();
        let mut s: Vec<Value> = Vec::new();
        eval_program(&reg, &mut s, "u64[1 2 3]").unwrap();
        let before = s.clone();
        // typecheck failure: can't reduce.+.u64 a Prod
        let r = eval_program(&reg, &mut s, "dup entuple.2 reduce.+.u64");
        assert!(r.is_err());
        assert_eq!(s, before);
    }
}
