//! Layer 4: parser. Tokenizes whitespace-separated text. Op-name lookups are
//! delegated to an `OpRegistry` (one of several possible front-ends). The
//! parser only knows about structural forms — blocks, refs, array literals,
//! `pick`/`roll` and N-arity ops, `:`-binding, and the `match`/`cleave`
//! desugarings. Everything else is a token the registry resolves.

use std::collections::HashMap;

use crate::syntax::inference::mark_last_use_in_body;
use crate::syntax::registry::OpRegistry;
use crate::ir::typecheck::Op;
use crate::ops::convert as cv;
use crate::ops::letbind as lb;
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
        if ch.is_whitespace() || ch == ',' {
            // `,` is a separator (whitespace-equivalent): `:[a, b]` ≡ `:[a b]`.
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
    let mut defs: Defs = HashMap::new();
    let mut expansions: Vec<String> = Vec::new();
    parse_block(&toks, &mut i, None, &mut scopes, &mut defs, &mut expansions, reg)
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

/// Total bindings currently in scope — the env index the next binding lands
/// at. Used to scope last-use inference to a `Let`'s own names.
fn scope_depth(scopes: &[Vec<String>]) -> usize {
    scopes.iter().map(|s| s.len()).sum()
}

/// Is `s` a valid collie identifier — alphanumeric or underscore, first
/// char a letter or underscore? Used to distinguish `:name` (binding) from
/// other tokens.
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
            // `.{ p0 ; p1 ; … }` — cleave: each path runs on a fresh copy of
            // TOS, results bundled into a Prod. Sugar, desugared here to
            // `:[g_v]  g_v <p0>  g_v <p1> … entuple.K` (bind the input, run each
            // path on a reference). No `Cleave` op is built — the paths become
            // inline op-stream. Faithful because every path is net 1→1 (it
            // consumes the one reference, produces one value); paths are
            // captured as raw token spans and the synthesized stream re-parsed.
            ".{" => {
                let tag = *i; // unique per cleave site → collision-free gensym
                let mut paths_toks: Vec<Vec<String>> = Vec::new();
                let mut start = *i;
                let mut depth: usize = 0;
                loop {
                    if *i >= toks.len() { return Err(".{: unterminated, expected }".into()); }
                    match toks[*i].as_str() {
                        "{" | ".{" | "[" => { depth += 1; *i += 1; }
                        "}" | "]" if depth > 0 => { depth -= 1; *i += 1; }
                        "}" => {
                            if start < *i || paths_toks.is_empty() { paths_toks.push(toks[start..*i].to_vec()); }
                            *i += 1;
                            break;
                        }
                        ";" if depth == 0 => { paths_toks.push(toks[start..*i].to_vec()); *i += 1; start = *i; }
                        _ => { *i += 1; }
                    }
                }
                let k = paths_toks.len();
                let g_v = format!("__c{}_v", tag);
                let mut synth: Vec<String> = vec![":".into(), "[".into(), g_v.clone(), "]".into()];
                for path in &paths_toks {
                    synth.push(g_v.clone());
                    synth.extend(path.iter().cloned());
                }
                synth.push(format!("entuple.{}", k));
                let mut j = 0;
                let body_ops = parse_block(&synth, &mut j, None, scopes, defs, expansions, reg)?;
                out.extend(body_ops);
            }
            // `match { -> arm0 -> arm1 … }` is sugar, desugared here to
            // `split :[g_disc g_l0 …] g_disc g_l0 <arm0> g_l1 <arm1> … mergeK`
            // (the merge half is `mergeK`; binding lets each arm apply to its
            // lane while disc rides through). No `Match` op is built — the arms
            // become inline op-stream visible to the optimizer. The arms are
            // captured as raw token spans and the synthesized stream is
            // re-parsed in place (so arms still see outer bindings/defs).
            "match" => {
                let tag = *i; // unique per match site → collision-free gensyms
                expect(toks, i, "{")?;
                let mut arms_toks: Vec<Vec<String>> = Vec::new();
                while *i < toks.len() && toks[*i] != "}" {
                    expect(toks, i, "->")?;
                    let start = *i;
                    let mut depth: usize = 0;
                    while *i < toks.len() {
                        match toks[*i].as_str() {
                            "{" | ".{" | "[" => depth += 1,
                            "}" | "]" if depth == 0 => break,
                            "}" | "]" => depth -= 1,
                            "->" if depth == 0 => break,
                            _ => {}
                        }
                        *i += 1;
                    }
                    arms_toks.push(toks[start..*i].to_vec());
                }
                expect(toks, i, "}")?;
                let k = arms_toks.len();
                if k == 0 { return Err("match: no arms".into()); }
                let g_disc = format!("__m{}_disc", tag);
                let g_lanes: Vec<String> = (0..k).map(|j| format!("__m{}_l{}", tag, j)).collect();
                let mut synth: Vec<String> = vec!["split".into(), ":".into(), "[".into(), g_disc.clone()];
                synth.extend(g_lanes.iter().cloned());
                synth.push("]".into());
                synth.push(g_disc.clone());
                for (j, arm) in arms_toks.iter().enumerate() {
                    synth.push(g_lanes[j].clone());
                    synth.extend(arm.iter().cloned());
                }
                synth.push(format!("merge{}", k));
                let mut j = 0;
                let body_ops = parse_block(&synth, &mut j, None, scopes, defs, expansions, reg)?;
                out.extend(body_ops);
            }
            // Standalone `{ … }` scope block: parse the body and inline its
            // ops. The braces only delimit binding scope (a `:[…]` inside
            // scopes to this `}`); there is no runtime effect of their own.
            // Lets `{| names |}` lower to `{ :[names] … }`.
            "{" => {
                let body = parse_block(toks, i, Some("}"), scopes, defs, expansions, reg)?;
                expect(toks, i, "}")?;
                out.extend(body);
            }
            "->" | "}" | ";" => {
                *i -= 1;
                *defs = defs_snapshot;
                return Ok(out);
            }
            "def" => {
                // `def name { body }` — capture the inner body tokens for
                // later inline expansion. Emits no op. Block-scoped: the defs
                // snapshot at parse_block entry is restored on exit. Parameters
                // are declared inside the body with `:[names]` / `:name`.
                if *i >= toks.len() { return Err("def: missing name".into()); }
                let name = toks[*i].clone();
                *i += 1;
                if *i >= toks.len() { return Err(format!("def {}: missing body", name)); }
                if toks[*i] != "{" {
                    return Err(format!("def {}: expected `{{`, got `{}`", name, toks[*i]));
                }
                *i += 1; // consume opener
                let inner_start = *i;
                let mut depth: usize = 1;
                while *i < toks.len() && depth > 0 {
                    match toks[*i].as_str() {
                        "{" | ".{" => depth += 1,
                        "}" => { depth -= 1; if depth == 0 { break; } }
                        _ => {}
                    }
                    *i += 1;
                }
                if *i >= toks.len() {
                    return Err(format!("def {}: unterminated body", name));
                }
                let body_tokens: Vec<String> = toks[inner_start..*i].to_vec();
                *i += 1; // consume the closing `}`
                defs.insert(name, body_tokens);
            }
            // Flat binding: `:name` pops the top of the stack and binds it to
            // `name` for the rest of the current block. Equivalent to
            // `{ :[name] <rest> }` — same IR, sweeter for sequential pipelines.
            // Body is the remainder of the block, so it boils to edges like
            // any `Let`. (`>` is reserved for comparison only.)
            s if s.starts_with(':') && is_ident_after_prefix(&s[1..]) => {
                let name = s[1..].to_string();
                let start = scope_depth(scopes);
                scopes.push(vec![name.clone()]);
                let mut body = parse_block(toks, i, end, scopes, defs, expansions, reg)?;
                scopes.pop();
                mark_last_use_in_body(&mut body, start, 1);
                out.push(Box::new(lb::Let { names: vec![name], body }));
                *defs = defs_snapshot;
                return Ok(out);
            }
            // `:[a b c]` — bind the top N stack values to names a..c in stack
            // order (a = deepest, c = top), for the rest of the scope. Commas
            // optional (the tokenizer treats `,` as a separator). `()` is
            // reserved for a future tuple-destructure pattern.
            ":" => {
                expect(toks, i, "[")?;
                let mut names = Vec::new();
                while *i < toks.len() && toks[*i] != "]" {
                    names.push(toks[*i].clone());
                    *i += 1;
                }
                expect(toks, i, "]")?;
                let n_names = names.len();
                let start = scope_depth(scopes);
                scopes.push(names.clone());
                let mut body = parse_block(toks, i, end, scopes, defs, expansions, reg)?;
                scopes.pop();
                mark_last_use_in_body(&mut body, start, n_names);
                out.push(Box::new(lb::Let { names, body }));
                *defs = defs_snapshot;
                return Ok(out);
            }
            _ => {
                // Binding reference (shadows registry and defs). Take is
                // inferred (last-use) and the graph derives it via use-counting,
                // so refs are emitted as clones; `mark_last_use_in_body` flips
                // the final one to a take.
                if let Some(idx) = lookup_binding(t, scopes) {
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
                        scopes, defs, expansions, reg,
                    )?;
                    expansions.pop();
                    out.extend(body_ops);
                    continue;
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
