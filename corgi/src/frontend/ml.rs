//! A small ML-flavoured expression surface, concatenative-by-juxtaposition: a value is followed by
//! its operator stages with no separator (`input iota reduce_sum`). `let` boils binding into shared
//! edges (no re-derivation), with product destructuring; lambdas (`x -> …`) are the map/match bodies;
//! sums are built/eliminated via `inject`/`match`. A stage-chain runs until a token that can't begin a
//! stage — notably the `let` body's `in`, the one identifier allowed to follow a complete chain.
//!
//!   expr   = 'let' pat '=' expr 'in' expr | pipe
//!   pat    = IDENT | '(' IDENT (',' IDENT)* ')'
//!   pipe   = proj apply*                               -- juxtaposition; chain ends before `in`
//!   apply  = 'map' '(' lambda ')'
//!          | 'map_variant' NUM '(' lambda ')'
//!          | 'match' '(' (NUM '(' lambda ')')(',' …)* ')'   -- MapSum + Unwrap
//!          | 'inject' NUM NUM                               -- sum construction (tag arity)
//!          | IDENT NUM?
//!   lambda = IDENT '->' expr
//!   proj   = atom ('.' NUM)*
//!   atom   = '(' expr (',' expr)* ')' | IDENT          -- 'input' is the root
//!
//! e.g.  let (subj, vals) = input.1 transpose in vals reduce_sum
//!       e match (0 (lo -> lo), 1 (hi -> hi add_u64 100))   -- exhaustive ⇒ Unwrap types it
//!       xs inject 0 2                                      -- tag xs into variant 0 of 2

use super::{resolve, str_value, takes_num};
use crate::graph::{Builder, Graph};
use crate::ops::{NumOp, Op};
use std::collections::HashMap;

// ----- tokens ------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Arrow, // ->
    Dot,
    LParen,
    RParen,
    Comma,
    Eq,
    Ident(String),
    Num(u64),
    Str(Vec<u8>),
}

fn lex(s: &str) -> Result<Vec<Tok>, String> {
    let cs: Vec<char> = s.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '-' if cs.get(i + 1) == Some(&'>') => {
                toks.push(Tok::Arrow);
                i += 2;
            }
            '.' => {
                toks.push(Tok::Dot);
                i += 1;
            }
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            '=' => {
                toks.push(Tok::Eq);
                i += 1;
            }
            '"' => {
                i += 1; // opening quote
                let mut bytes = Vec::new();
                loop {
                    match cs.get(i) {
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some(&ch) => {
                            bytes.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
                            i += 1;
                        }
                        None => return Err("unterminated string literal".to_string()),
                    }
                }
                toks.push(Tok::Str(bytes));
            }
            c if c.is_ascii_digit() => {
                let mut n = 0u64;
                while i < cs.len() && cs[i].is_ascii_digit() {
                    n = n * 10 + cs[i].to_digit(10).unwrap() as u64;
                    i += 1;
                }
                toks.push(Tok::Num(n));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut w = String::new();
                while i < cs.len() && (cs[i].is_ascii_alphanumeric() || cs[i] == '_') {
                    w.push(cs[i]);
                    i += 1;
                }
                toks.push(Tok::Ident(w));
            }
            _ => return Err(format!("unexpected character '{c}'")),
        }
    }
    Ok(toks)
}

// ----- AST ---------------------------------------------------------------

enum Pat {
    Name(String),
    Tuple(Vec<String>),
}

enum Apply {
    Op(String, Option<u64>),
    Str(Vec<u8>),
    Map(String, Box<E>),
    MapVariant(usize, String, Box<E>),
    Match(Vec<(usize, String, E)>), // arms (tag, binding, body) -> MapSum + Unwrap
    Inject(usize, usize),            // tag + arity -> Op::Inject (sum construction; other lanes ⊥)
}

enum E {
    Var(String),
    Tuple(Vec<E>),
    Proj(Box<E>, usize),
    Let(Pat, Box<E>, Box<E>),
    Pipe(Box<E>, Apply),
}

// ----- parser ------------------------------------------------------------

struct P {
    toks: Vec<Tok>,
    i: usize,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }
    fn bump(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.i).cloned();
        if t.is_some() {
            self.i += 1;
        }
        t
    }
    fn eat(&mut self, t: &Tok) -> Result<(), String> {
        if self.peek() == Some(t) {
            self.i += 1;
            Ok(())
        } else {
            Err(format!("expected {t:?}, found {:?}", self.peek()))
        }
    }
    fn ident(&mut self) -> Result<String, String> {
        match self.bump() {
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(format!("expected an identifier, found {other:?}")),
        }
    }
    fn num(&mut self) -> Result<u64, String> {
        match self.bump() {
            Some(Tok::Num(n)) => Ok(n),
            other => Err(format!("expected a number, found {other:?}")),
        }
    }
    fn is_kw(&self, s: &str) -> bool {
        self.peek() == Some(&Tok::Ident(s.to_string()))
    }

    fn expr(&mut self) -> Result<E, String> {
        if self.is_kw("let") {
            self.bump();
            let pat = if self.peek() == Some(&Tok::LParen) {
                self.bump();
                let mut names = vec![self.ident()?];
                while self.peek() == Some(&Tok::Comma) {
                    self.bump();
                    names.push(self.ident()?);
                }
                self.eat(&Tok::RParen)?;
                Pat::Tuple(names)
            } else {
                Pat::Name(self.ident()?)
            };
            self.eat(&Tok::Eq)?;
            let bound = self.expr()?;
            if !self.is_kw("in") {
                return Err(format!("expected 'in', found {:?}", self.peek()));
            }
            self.bump();
            let body = self.expr()?;
            Ok(E::Let(pat, Box::new(bound), Box::new(body)))
        } else {
            self.pipe()
        }
    }

    fn pipe(&mut self) -> Result<E, String> {
        let mut e = self.proj()?;
        // a value is followed by its stages by juxtaposition; the chain runs until a token that
        // cannot begin a stage — in particular the `let` body's `in`, the one identifier that can
        // legally follow a complete pipe without being an op.
        while self.starts_apply() {
            let ap = self.apply()?;
            e = E::Pipe(Box::new(e), ap);
        }
        Ok(e)
    }

    /// whether the next token can begin a pipe stage — a string constant, or any identifier other
    /// than the chain-terminating `in`.
    fn starts_apply(&self) -> bool {
        match self.peek() {
            Some(Tok::Str(_)) => true,
            Some(Tok::Ident(k)) => k != "in",
            _ => false,
        }
    }

    fn apply(&mut self) -> Result<Apply, String> {
        // a string literal as a stage is a constant, like `lit`: broadcast to the value.
        if let Some(Tok::Str(_)) = self.peek() {
            let Some(Tok::Str(bytes)) = self.bump() else { unreachable!() };
            return Ok(Apply::Str(bytes));
        }
        let name = self.ident()?;
        match name.as_str() {
            "map" => {
                self.eat(&Tok::LParen)?;
                let (x, body) = self.lambda()?;
                self.eat(&Tok::RParen)?;
                Ok(Apply::Map(x, Box::new(body)))
            }
            "map_variant" => {
                let k = self.num()? as usize;
                self.eat(&Tok::LParen)?;
                let (x, body) = self.lambda()?;
                self.eat(&Tok::RParen)?;
                Ok(Apply::MapVariant(k, x, Box::new(body)))
            }
            // match: one arm per variant — `match (k0 (x -> b0), k1 (y -> b1), …)`.
            "match" => {
                self.eat(&Tok::LParen)?;
                let mut arms = Vec::new();
                loop {
                    let k = self.num()? as usize;
                    self.eat(&Tok::LParen)?;
                    let (x, body) = self.lambda()?;
                    self.eat(&Tok::RParen)?;
                    arms.push((k, x, body));
                    if self.peek() == Some(&Tok::Comma) {
                        self.bump();
                    } else {
                        break;
                    }
                }
                self.eat(&Tok::RParen)?;
                Ok(Apply::Match(arms))
            }
            // inject: construct a sum — `inject tag arity`, the payload going to variant `tag` of
            // `arity` lanes; the other lanes are ⊥ and adopt their type at a later merge.
            "inject" => {
                let tag = self.num()? as usize;
                let arity = self.num()? as usize;
                Ok(Apply::Inject(tag, arity))
            }
            _ if takes_num(&name) => Ok(Apply::Op(name, Some(self.num()?))),
            _ => Ok(Apply::Op(name, None)),
        }
    }

    fn lambda(&mut self) -> Result<(String, E), String> {
        // a lambda is `IDENT -> body`; the `->` is the marker (no `fun` keyword).
        let x = self.ident()?;
        self.eat(&Tok::Arrow)?;
        let body = self.expr()?;
        Ok((x, body))
    }

    fn proj(&mut self) -> Result<E, String> {
        let mut e = self.atom()?;
        while self.peek() == Some(&Tok::Dot) {
            self.bump();
            e = E::Proj(Box::new(e), self.num()? as usize);
        }
        Ok(e)
    }

    fn atom(&mut self) -> Result<E, String> {
        match self.peek() {
            Some(Tok::LParen) => {
                self.bump();
                let mut es = vec![self.expr()?];
                while self.peek() == Some(&Tok::Comma) {
                    self.bump();
                    es.push(self.expr()?);
                }
                self.eat(&Tok::RParen)?;
                Ok(if es.len() == 1 { es.pop().unwrap() } else { E::Tuple(es) })
            }
            Some(Tok::Ident(_)) => Ok(E::Var(self.ident()?)),
            other => Err(format!("expected an expression, found {other:?}")),
        }
    }
}

// ----- lowering ----------------------------------------------------------

type Env = HashMap<String, usize>;

/// lower a lambda body into a closed sub-graph (its parameter is the body's `Input`).
fn lower_body(x: &str, body: &E) -> Result<Graph<NumOp>, String> {
    let mut bb = Builder::default();
    let bin = bb.input();
    let mut benv = Env::new();
    benv.insert(x.to_string(), bin);
    let bout = lower(body, &benv, &mut bb)?;
    Ok(bb.finish(bout))
}

fn lower(e: &E, env: &Env, b: &mut Builder<NumOp>) -> Result<usize, String> {
    match e {
        E::Var(name) => env.get(name).copied().ok_or_else(|| format!("unbound variable '{name}'")),
        E::Tuple(es) => {
            let ids = es.iter().map(|x| lower(x, env, b)).collect::<Result<Vec<_>, _>>()?;
            Ok(b.tuple(ids))
        }
        E::Proj(e, i) => {
            let id = lower(e, env, b)?;
            Ok(b.add(Op::Field(*i), vec![id]))
        }
        E::Let(pat, bound, body) => {
            let id = lower(bound, env, b)?;
            let mut env2 = env.clone();
            match pat {
                Pat::Name(x) => {
                    env2.insert(x.clone(), id);
                }
                Pat::Tuple(names) => {
                    for (i, name) in names.iter().enumerate() {
                        let fid = b.add(Op::Field(i), vec![id]);
                        env2.insert(name.clone(), fid);
                    }
                }
            }
            lower(body, &env2, b)
        }
        E::Pipe(e, ap) => {
            let id = lower(e, env, b)?;
            match ap {
                Apply::Op(name, arg) => Ok(b.add(resolve(name, *arg)?, vec![id])),
                Apply::Str(bytes) => Ok(b.add(Op::Lit(str_value(bytes.clone())), vec![id])),
                Apply::Map(x, body) => Ok(b.add(Op::MapList(Box::new(lower_body(x, body)?)), vec![id])),
                Apply::MapVariant(k, x, body) => {
                    Ok(b.add(Op::MapSum(vec![(*k, lower_body(x, body)?)]), vec![id]))
                }
                Apply::Match(arms) => {
                    let lowered = arms
                        .iter()
                        .map(|(k, x, body)| Ok((*k, lower_body(x, body)?)))
                        .collect::<Result<Vec<(usize, Graph<NumOp>)>, String>>()?;
                    let ms = b.add(Op::MapSum(lowered), vec![id]);
                    Ok(b.add(Op::Unwrap, vec![ms]))
                }
                Apply::Inject(tag, arity) => Ok(b.add(Op::Inject(*tag, *arity), vec![id])),
            }
        }
    }
}

/// parse an ML-flavoured expression into a `Graph` (with `input` bound to the root).
pub fn parse_ml(src: &str) -> Result<Graph<NumOp>, String> {
    let toks = lex(src)?;
    let mut p = P { toks, i: 0 };
    let e = p.expr()?;
    if p.i != p.toks.len() {
        return Err(format!("trailing tokens from index {}", p.i));
    }
    let mut b = Builder::default();
    let input = b.input();
    let mut env = Env::new();
    env.insert("input".to_string(), input);
    let out = lower(&e, &env, &mut b)?;
    Ok(b.finish(out))
}
