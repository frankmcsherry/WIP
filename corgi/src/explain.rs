//! A graph as text, for reading what a program lowers to: one node per line, each body indented
//! under the op that carries it. And, with the `profile` feature, time per op kind over a run.

use crate::graph::{Graph, NodeKind};
use crate::ops::{NumOp, Op};
use std::fmt::Write;

/// the graph, one node per line (`%i = op %inputs`), bodies indented under their op.
pub fn explain(g: &Graph<NumOp>) -> String {
    let mut s = String::new();
    write_graph(&mut s, g, 0);
    s
}

fn write_graph(s: &mut String, g: &Graph<NumOp>, depth: usize) {
    let pad = "  ".repeat(depth);
    for (i, node) in g.nodes.iter().enumerate() {
        let ins: Vec<String> = node.inputs.iter().map(|j| format!("%{j}")).collect();
        let ins = ins.join(" ");
        let out = if i == g.output { "   <- out" } else { "" };
        let _ = match &node.kind {
            NodeKind::Input => writeln!(s, "{pad}%{i} = input{out}"),
            NodeKind::Tuple => writeln!(s, "{pad}%{i} = ({ins}){out}"),
            NodeKind::Op(op) => writeln!(s, "{pad}%{i} = {} {ins}{out}", op_name(op)),
        };
        if let NodeKind::Op(op) = &node.kind {
            if let NumOp::Core(Op::MapSum(arms)) = op {
                for (t, body) in arms {
                    let _ = writeln!(s, "{pad}  arm {t}:");
                    write_graph(s, body, depth + 2);
                }
            } else {
                use crate::graph::OpLike;
                for body in op.children() {
                    write_graph(s, body, depth + 1);
                }
            }
        }
    }
}

/// a short name for an op: its variant and parameters, without the bodies it carries.
pub(crate) fn op_name(op: &NumOp) -> String {
    match op {
        NumOp::Core(c) => match c {
            Op::MapList(_) => "map".into(),
            Op::Fold(_) => "fold".into(),
            Op::FoldScan(_) => "foldscan".into(),
            Op::MapSum(arms) => {
                let tags: Vec<String> = arms.iter().map(|(t, _)| t.to_string()).collect();
                format!("match[{}]", tags.join(","))
            }
            Op::Lit(v) => {
                let mut s = crate::value::show(v);
                if s.len() > 40 { s.truncate(40); s.push('…'); }
                format!("lit {s}")
            }
            Op::Inject(t, lanes) => format!("Inject({t} of {})", lanes.len()),
            other => format!("{other:?}"),
        },
        NumOp::Cmp(c) => format!("{c:?}"),
        NumOp::Arith(a) => format!("{a:?}"),
        NumOp::Text(t) => format!("{t:?}"),
        NumOp::Host(h) => format!("{h:?}"),
    }
}

/// time per op kind: each op's own time (its bodies' ops are counted under their own names), and
/// how many times it ran. `reset`, run, `report`.
#[cfg(feature = "profile")]
pub mod profile {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct State {
        stack: Vec<Duration>,
        ops: HashMap<String, (u64, Duration)>,
    }
    thread_local! {
        static STATE: RefCell<State> = RefCell::new(State::default());
    }

    pub fn reset() {
        STATE.with(|s| *s.borrow_mut() = State::default());
    }

    /// (op, runs, own time), most time first.
    pub fn report() -> Vec<(String, u64, Duration)> {
        let mut v: Vec<_> = STATE.with(|s| s.borrow().ops.iter().map(|(k, &(n, t))| (k.clone(), n, t)).collect());
        v.sort_by(|a, b| b.2.cmp(&a.2));
        v
    }

    pub(crate) fn time<R>(name: impl FnOnce() -> String, f: impl FnOnce() -> R) -> R {
        STATE.with(|s| s.borrow_mut().stack.push(Duration::ZERO));
        let t = Instant::now();
        let r = f();
        let dt = t.elapsed();
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let child = s.stack.pop().unwrap_or_default();
            if let Some(top) = s.stack.last_mut() {
                *top += dt;
            }
            let e = s.ops.entry(name()).or_default();
            e.0 += 1;
            e.1 += dt.saturating_sub(child);
        });
        r
    }
}
