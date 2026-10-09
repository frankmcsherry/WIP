//! Balanced brackets of three kinds, with a stack.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::Value;

fn reference(s: &[u8]) -> i64 {
    let mut stack = Vec::new();
    for &c in s {
        match c {
            b'(' | b'[' | b'{' => stack.push(c),
            b')' | b']' | b'}' => {
                let want = match c { b')' => b'(', b']' => b'[', _ => b'{' };
                if stack.pop() != Some(want) {
                    return 0;
                }
            }
            _ => {}
        }
    }
    stack.is_empty() as i64
}

/// a random string that is balanced about half the time: a balanced nest, sometimes broken.
fn gen(rng: &mut Rng) -> Vec<u8> {
    let mut s = Vec::new();
    let mut open = Vec::new();
    let n = rng.below(25);
    for _ in 0..n {
        match rng.below(5) {
            0 | 1 => { let k = rng.below(3) as usize; s.push(b"([{"[k]); open.push(b")]}"[k]); }
            2 | 3 if !open.is_empty() => s.push(open.pop().unwrap()),
            _ => s.push(b"ab"[rng.below(2) as usize]),
        }
    }
    while let Some(c) = open.pop() { s.push(c); }
    if rng.below(2) == 0 && !s.is_empty() {
        let at = rng.below(s.len() as i64) as usize;
        s[at] = b"()[]{}x"[rng.below(7) as usize];
    }
    s
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(14);
    let rows: Vec<Vec<u8>> = (0..cfg.rows).map(|_| gen(&mut rng)).collect();
    let input = bytes_col(&rows);
    let expected = Value::i64(rows.iter().map(|r| reference(r)).collect());
    let rust = || rows.iter().map(|r| reference(r)).collect::<Vec<_>>();
    let what = "0-50 bytes, nested ([{ with letters, half broken";
    run_case(cfg, "balanced_brackets", what, include_str!("../../algorithms/balanced_brackets.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "balanced_brackets_levels", what, include_str!("../../algorithms/balanced_brackets_levels.col"), input, expected, rust);
}
