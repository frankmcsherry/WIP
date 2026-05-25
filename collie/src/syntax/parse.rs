//! Layer 4: parser. Tokenizes whitespace-separated text. Op-name lookups are
//! delegated to an `OpRegistry` (one of several possible front-ends). The
//! parser only knows about structural forms — blocks, refs, array literals,
//! `pick`/`roll` and N-arity ops, `match`/`reduce`/`each`/`let`. Everything
//! else is a token the registry resolves.

use std::collections::HashMap;

use crate::syntax::inference::mark_last_use_in_body;
use crate::syntax::registry::OpRegistry;
use crate::ir::typecheck::Op;
use crate::ops::combinators as cm;
use crate::ops::convert as cv;
use crate::ops::letbind as lb;
use crate::ops::list as ls;
use crate::ops::stack as sk;

/// Parse-time inline definitions: `def name { body }` saves the body
/// tokens, and later occurrences of `name` re-parse the body in place.
/// Macro semantics, not first-class procedures — name lookups inside the
/// body resolve against the calling environment.
type Defs = HashMap<String, Vec<String>>;

fn tokenize(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in src.chars() {
        if ch.is_whitespace() {
            if !cur.is_empty() { out.push(std::mem::take(&mut cur)); }
        } else if ch == '[' || ch == ']' {
            if !cur.is_empty() { out.push(std::mem::take(&mut cur)); }
            out.push(ch.to_string());
        } else {
            cur.push(ch);
        }
    }
    if !cur.is_empty() { out.push(cur); }
    out
}

pub fn parse(src: &str, reg: &OpRegistry) -> Result<Vec<Box<dyn Op>>, String> {
    let stripped = strip_comments(src);
    let toks = tokenize(&stripped);
    let mut i = 0;
    let mut scopes: Vec<Vec<String>> = Vec::new();
    // Mirror of env positions: `true` means a binding has been consumed
    // via `name>` and subsequent references to it are an error. Grows
    // with `scopes`, truncated when scopes pops.
    let mut takens: Vec<bool> = Vec::new();
    let mut defs: Defs = HashMap::new();
    let mut expansions: Vec<String> = Vec::new();
    parse_block(&toks, &mut i, None, &mut scopes, &mut takens, &mut defs, &mut expansions, reg)
}

/// Strip `#` line comments (from `#` to end-of-line). Required for source
/// files that want commentary. `#` is not used by the language itself.
pub fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for (i, line) in src.lines().enumerate() {
        if i > 0 { out.push('\n'); }
        if let Some(idx) = line.find('#') {
            out.push_str(&line[..idx]);
        } else {
            out.push_str(line);
        }
    }
    out
}

/// Read a file and parse it. Comments (`#` to end-of-line) are stripped.
pub fn parse_file(path: &std::path::Path, reg: &OpRegistry) -> Result<Vec<Box<dyn Op>>, String> {
    let src = std::fs::read_to_string(path)
        .map_err(|e| format!("read {}: {}", path.display(), e))?;
    parse(&src, reg)
}

/// Is `s` a valid collie identifier — alphanumeric or underscore, first
/// char a letter or underscore? Used to distinguish `>name` (binding) from
/// `>` / `>=` (comparison ops).
fn is_ident_after_prefix(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn lookup_binding(name: &str, scopes: &[Vec<String>]) -> Option<usize> {
    let mut total: usize = scopes.iter().map(|s| s.len()).sum();
    for scope in scopes.iter().rev() {
        for n in scope.iter().rev() {
            total -= 1;
            if n == name { return Some(total); }
        }
    }
    None
}

fn expect(toks: &[String], i: &mut usize, want: &str) -> Result<(), String> {
    if *i >= toks.len() {
        return Err(format!("expected {}, got end of input", want));
    }
    if toks[*i] != want {
        return Err(format!("expected {}, got {}", want, toks[*i]));
    }
    *i += 1;
    Ok(())
}

fn parse_block(
    toks: &[String],
    i: &mut usize,
    end: Option<&str>,
    scopes: &mut Vec<Vec<String>>,
    takens: &mut Vec<bool>,
    defs: &mut Defs,
    expansions: &mut Vec<String>,
    reg: &OpRegistry,
) -> Result<Vec<Box<dyn Op>>, String> {
    // Snapshot defs at block entry; restore on exit so any defs added in
    // this block (and any shadowing of outer defs) don't leak outward.
    // Cloning a HashMap of small Vec<String>s is cheap.
    let defs_snapshot = defs.clone();
    let mut out: Vec<Box<dyn Op>> = Vec::new();
    while *i < toks.len() {
        let t = &toks[*i];
        if Some(t.as_str()) == end {
            *defs = defs_snapshot;
            return Ok(out);
        }
        *i += 1;
        match t.as_str() {
            // pick/roll take a separate numeric arg, not handled by the registry
            "pick" => {
                if *i >= toks.len() { return Err("pick: needs index".into()); }
                let n: usize = toks[*i].parse().map_err(|_| format!("pick: bad index {}", toks[*i]))?;
                *i += 1;
                out.push(Box::new(sk::Pick { n }));
            }
            "roll" => {
                if *i >= toks.len() { return Err("roll: needs index".into()); }
                let n: usize = toks[*i].parse().map_err(|_| format!("roll: bad index {}", toks[*i]))?;
                *i += 1;
                out.push(Box::new(sk::Roll { n }));
            }
            "repeat" => {
                if *i >= toks.len() { return Err("repeat: needs count".into()); }
                let n: usize = toks[*i].parse().map_err(|_| format!("repeat: bad count {}", toks[*i]))?;
                *i += 1;
                expect(toks, i, "{")?;
                let body = parse_block(toks, i, Some("}"), scopes, takens, defs, expansions, reg)?;
                expect(toks, i, "}")?;
                out.push(Box::new(ls::Repeat { n, body }));
            }
            // Body-bearing constructs
            "each" => {
                let body = if *i < toks.len() && toks[*i] == "{|" {
                    *i += 1;
                    let mut names = Vec::new();
                    while *i < toks.len() && toks[*i] != "|" { names.push(toks[*i].clone()); *i += 1; }
                    expect(toks, i, "|")?;
                    let n_names = names.len();
                    let start = takens.len();
                    scopes.push(names.clone());
                    takens.resize(takens.len() + n_names, false);
                    let mut inner = parse_block(toks, i, Some("}"), scopes, takens, defs, expansions, reg)?;
                    expect(toks, i, "}")?;
                    scopes.pop();
                    takens.truncate(takens.len() - n_names);
                    // Auto-infer last-use takes for the Let's own bindings.
                    // Refs to outer bindings inside this body are not marked
                    // (would be incorrect when body is iterated per row).
                    mark_last_use_in_body(&mut inner, start, n_names);
                    vec![Box::new(lb::Let { names, body: inner }) as Box<dyn Op>]
                } else {
                    expect(toks, i, "{")?;
                    let body = parse_block(toks, i, Some("}"), scopes, takens, defs, expansions, reg)?;
                    expect(toks, i, "}")?;
                    body
                };
                out.push(Box::new(ls::Each { body }));
            }
            "reduce" => {
                let body = if *i < toks.len() && toks[*i] == "{|" {
                    *i += 1;
                    let mut names = Vec::new();
                    while *i < toks.len() && toks[*i] != "|" { names.push(toks[*i].clone()); *i += 1; }
                    expect(toks, i, "|")?;
                    let n_names = names.len();
                    let start = takens.len();
                    scopes.push(names.clone());
                    takens.resize(takens.len() + n_names, false);
                    let mut inner = parse_block(toks, i, Some("}"), scopes, takens, defs, expansions, reg)?;
                    expect(toks, i, "}")?;
                    scopes.pop();
                    takens.truncate(takens.len() - n_names);
                    mark_last_use_in_body(&mut inner, start, n_names);
                    vec![Box::new(lb::Let { names, body: inner }) as Box<dyn Op>]
                } else {
                    expect(toks, i, "{")?;
                    let body = parse_block(toks, i, Some("}"), scopes, takens, defs, expansions, reg)?;
                    expect(toks, i, "}")?;
                    body
                };
                out.push(Box::new(ls::Reduce { body }));
            }
            ".{" => {
                // .{ p0 ; p1 ; ... ; pN } — cleave: each path runs on a fresh
                // copy of TOS, results are gathered into a Prod.
                let mut paths: Vec<Vec<Box<dyn Op>>> = Vec::new();
                let mut current: Vec<Box<dyn Op>> = Vec::new();
                loop {
                    if *i >= toks.len() {
                        return Err(".{: unterminated, expected }".into());
                    }
                    let t = &toks[*i];
                    if t == "}" {
                        *i += 1;
                        if !current.is_empty() || paths.is_empty() {
                            paths.push(std::mem::take(&mut current));
                        }
                        break;
                    }
                    if t == ";" {
                        *i += 1;
                        paths.push(std::mem::take(&mut current));
                        continue;
                    }
                    // Parse a single subprogram step, then loop.
                    let one = parse_block(toks, i, None, scopes, takens, defs, expansions, reg)?;
                    current.extend(one);
                    // parse_block returns on `;` or `}` (re-pushed via `i -= 1`),
                    // or on end-of-input. Loop to dispatch.
                }
                out.push(Box::new(cm::Cleave { paths }));
            }
            "match" => {
                expect(toks, i, "{")?;
                let mut arms = Vec::new();
                while *i < toks.len() && toks[*i] != "}" {
                    expect(toks, i, "->")?;
                    let arm = parse_block(toks, i, None, scopes, takens, defs, expansions, reg)?;
                    arms.push(arm);
                }
                expect(toks, i, "}")?;
                out.push(Box::new(cm::Match { arms }));
            }
            "{|" => {
                let mut names = Vec::new();
                while *i < toks.len() && toks[*i] != "|" { names.push(toks[*i].clone()); *i += 1; }
                expect(toks, i, "|")?;
                let n_names = names.len();
                let start = takens.len();
                scopes.push(names.clone());
                takens.resize(takens.len() + n_names, false);
                let mut body = parse_block(toks, i, Some("}"), scopes, takens, defs, expansions, reg)?;
                expect(toks, i, "}")?;
                scopes.pop();
                takens.truncate(takens.len() - n_names);
                mark_last_use_in_body(&mut body, start, n_names);
                out.push(Box::new(lb::Let { names, body }));
            }
            "->" | "}" | ";" => {
                *i -= 1;
                *defs = defs_snapshot;
                return Ok(out);
            }
            "def" => {
                // `def name { body }` or `def name {| params | body }` —
                // capture body tokens for later inline expansion. Emits
                // no op. Block-scoped: snapshot at parse_block entry is
                // restored on exit.
                //
                // For the `{` form, the captured body is the inner tokens
                // (without the braces). For the `{|` form, the entire
                // `{| ... |}` is captured so the expansion re-parses as a
                // closure — the natural way to declare def parameters.
                if *i >= toks.len() { return Err("def: missing name".into()); }
                let name = toks[*i].clone();
                *i += 1;
                if *i >= toks.len() { return Err(format!("def {}: missing body", name)); }
                let include_outer = match toks[*i].as_str() {
                    "{"  => false,
                    "{|" => true,
                    other => return Err(format!(
                        "def {}: expected `{{` or `{{|`, got `{}`", name, other)),
                };
                let outer_start = *i;
                *i += 1; // consume opener
                let inner_start = *i;
                let mut depth: usize = 1;
                while *i < toks.len() && depth > 0 {
                    match toks[*i].as_str() {
                        "{" | "{|" | ".{" => depth += 1,
                        "}" => { depth -= 1; if depth == 0 { break; } }
                        _ => {}
                    }
                    *i += 1;
                }
                if *i >= toks.len() {
                    return Err(format!("def {}: unterminated body", name));
                }
                let body_tokens: Vec<String> = if include_outer {
                    toks[outer_start..*i + 1].to_vec()
                } else {
                    toks[inner_start..*i].to_vec()
                };
                *i += 1; // consume the closing `}`
                defs.insert(name, body_tokens);
            }
            // Forth-style flat binding: `>name` pops the top of the stack
            // and binds it to `name` for the rest of the current block.
            // Equivalent to `{| name | <rest of block> }` — same IR, sweeter
            // syntax for sequential pipelines.
            //
            // Requires `name` to be a valid identifier (alphanumeric +
            // underscore, not starting with a digit). This excludes the
            // comparison ops `>` and `>=`, which start with `>` but
            // aren't bindings.
            s if s.starts_with('>') && is_ident_after_prefix(&s[1..]) => {
                let name = s[1..].to_string();
                let start = takens.len();
                scopes.push(vec![name.clone()]);
                takens.push(false);
                let mut body = parse_block(toks, i, end, scopes, takens, defs, expansions, reg)?;
                scopes.pop();
                takens.pop();
                mark_last_use_in_body(&mut body, start, 1);
                out.push(Box::new(lb::Let { names: vec![name], body }));
                *defs = defs_snapshot;
                return Ok(out);
            }
            // `name>` (take form): only matches if prefix is a valid
            // identifier. Strips the trailing `>`, looks up the binding,
            // verifies it's the last reference (errors if already taken
            // or referenced after), emits `Ref { take: true }`. Subsequent
            // uses of `name` will see takens[idx] = true and error.
            s if s.ends_with('>') && s.len() > 1 && is_ident_after_prefix(&s[..s.len()-1]) => {
                let name = &s[..s.len()-1];
                if let Some(idx) = lookup_binding(name, scopes) {
                    if takens[idx] {
                        return Err(format!(
                            "{}>: already consumed (use-after-take)", name
                        ));
                    }
                    takens[idx] = true;
                    out.push(Box::new(lb::Ref { idx, take: true }));
                    continue;
                }
                return Err(format!("{}>: unknown binding", name));
            }
            _ => {
                // Binding reference first (shadows registry and defs).
                if let Some(idx) = lookup_binding(t, scopes) {
                    if takens[idx] {
                        return Err(format!(
                            "ref {}: use-after-take (already consumed via {0}>)", t
                        ));
                    }
                    out.push(Box::new(lb::Ref { idx, take: false }));
                    continue;
                }
                // Def expansion: re-parse the saved body in place. Name
                // lookups inside resolve against the *calling* environment.
                if let Some(body_toks) = defs.get(t).cloned() {
                    if expansions.iter().any(|n| n == t) {
                        return Err(format!("def {}: recursive expansion", t));
                    }
                    expansions.push(t.to_string());
                    let mut j = 0;
                    let body_ops = parse_block(
                        &body_toks, &mut j, None,
                        scopes, takens, defs, expansions, reg,
                    )?;
                    expansions.pop();
                    out.extend(body_ops);
                    continue;
                }
                // Surface→IR desugars: ops whose meaning is fully captured by
                // a short sequence of kernel ops. Keeps the surface ergonomic
                // while shrinking the kernel surface.
                //
                // `length` (SEQ<T> → SEQ<u64>(1 elem)) = `enlist count`: wrap
                // the column as one row of a list-of-self, then read the row
                // length from bounds.
                if t == "length" {
                    if let (Some(en), Some(co)) = (reg.make("enlist"), reg.make("count")) {
                        out.push(en);
                        out.push(co);
                        continue;
                    }
                }
                // Registry lookup for plain ops, with a peephole fusion:
                // `where` followed by `gather` becomes a single `filter` op.
                // Same semantics (filter src by mask, produce values), one
                // pass instead of three. parse_block recurses into nested
                // blocks (Let bodies etc.), so the fusion fires uniformly.
                if let Some(op) = reg.make(t) {
                    if op.name() == "gather"
                        && out.last().map(|o| o.name() == "where").unwrap_or(false)
                    {
                        out.pop();
                        if let Some(filter_op) = reg.make("filter") {
                            out.push(filter_op);
                            continue;
                        }
                    }
                    out.push(op);
                    continue;
                }
                // Column literals: `<type>[ ... ]` — structural, parser-internal.
                if matches!(t.as_str(), "u64"|"u8"|"u16"|"u32"|"i64"|"i32"|"i16"|"i8"|"f64"|"f32"|"bool")
                    && *i < toks.len() && toks[*i] == "["
                {
                    let ty = t.clone();
                    *i += 1;
                    let mut elems: Vec<String> = Vec::new();
                    while *i < toks.len() && toks[*i] != "]" {
                        elems.push(toks[*i].clone());
                        *i += 1;
                    }
                    expect(toks, i, "]")?;
                    use crate::ir::value::Storage;
                    macro_rules! parse_arr { ($t:ty, $tag:literal) => {{
                        let vs: Vec<$t> = elems.iter().map(|s| s.parse::<$t>()
                            .map_err(|_| format!("{} literal: bad value '{}'", stringify!($t), s)))
                            .collect::<Result<Vec<_>, _>>()?;
                        Box::new(cv::LitArr { tag: $tag, prim: <$t as Storage>::wrap(vs) })
                            as Box<dyn Op>
                    }};}
                    let op: Box<dyn Op> = match ty.as_str() {
                        "u64" => parse_arr!(u64, "u64[]"),
                        "u32" => parse_arr!(u32, "u32[]"),
                        "u16" => parse_arr!(u16, "u16[]"),
                        "u8"  => parse_arr!(u8,  "u8[]"),
                        "i64" => parse_arr!(i64, "i64[]"),
                        "i32" => parse_arr!(i32, "i32[]"),
                        "i16" => parse_arr!(i16, "i16[]"),
                        "i8"  => parse_arr!(i8,  "i8[]"),
                        "f64" => parse_arr!(f64, "f64[]"),
                        "f32" => parse_arr!(f32, "f32[]"),
                        "bool" => {
                            let vs: Vec<u8> = elems.iter().map(|s| match s.as_str() {
                                "t"|"true"|"1" => Ok(1u8),
                                "f"|"false"|"0" => Ok(0u8),
                                _ => Err(format!("bool literal: bad value '{}'", s)),
                            }).collect::<Result<Vec<_>, _>>()?;
                            Box::new(cv::LitArr { tag: "bool[]", prim: <u8 as Storage>::wrap(vs) })
                        }
                        _ => unreachable!(),
                    };
                    out.push(op);
                    continue;
                }
                // Bare numeric (defaults to i64 / f64) — last resort.
                if let Ok(n) = t.parse::<i64>() {
                    out.push(Box::new(cv::LitNum {
                        n: n as i128, f: n as f64, interp: crate::ir::shape::Interp::I64,
                    }));
                    continue;
                }
                if let Ok(f) = t.parse::<f64>() {
                    out.push(Box::new(cv::LitNum {
                        n: f as i128, f, interp: crate::ir::shape::Interp::F64,
                    }));
                    continue;
                }
                return Err(format!("unknown token: {}", t));
            }
        }
    }
    *defs = defs_snapshot;
    Ok(out)
}
