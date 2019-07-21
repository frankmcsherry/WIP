use crate::{Node, Diff, NodeArrangement, EdgeArrangement};

use timely::dataflow::Scope;
use timely::order::Product;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::AsCollection;

pub fn connected<G: Scope<Timestamp = ()>>(
    nodes: &NodeArrangement<G>,
    forward: &EdgeArrangement<G>,
    reverse: &EdgeArrangement<G>,
) -> Collection<G, (Node, Node), Diff> {

    nodes.stream.scope().iterative(|scope| {

        // import arrangements, nodes.
        let forward = forward.enter(scope);
        let reverse = reverse.enter(scope);
        let nodes = nodes.enter(scope).as_collection(|&src,&()| (src,src));

        let inner = Variable::new(scope, Product::new(Default::default(), 1));

        let labels = inner.arrange_by_key();
        let f_prop = labels.join_core(&forward, |_k,l,d| Some((*d,*l)));
        let r_prop = labels.join_core(&reverse, |_k,l,d| Some((*d,*l)));

        use timely::dataflow::operators::{Map, Concat, Delay};

        let result =
        nodes
            .inner
            .map_in_place(|dtr| (dtr.1).inner = 256 * ((((::std::mem::size_of::<Node>() * 8) as u32) - (dtr.0).1.leading_zeros())))
            .concat(&inner.filter(|_| false).inner)
            .delay(|dtr,_| dtr.1.clone())
            .as_collection()
            .concat(&f_prop)
            .concat(&r_prop)
            .reduce(|_, s, t| { t.push((*s[0].0, 1)); });

        inner.set(&result);
        result.leave()
    })
}