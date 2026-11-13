use corgi::{Demand, Shape};

#[test]
fn batched_requests_match_individual_requests() {
    let cases = [
        ("let (a,b) = input in (a,(a,b) add)", Shape::Prod(vec![Shape::Int, Shape::Int])),
        ("let (a,b) = input in (a map (x -> x.0),b)", Shape::Prod(vec![Shape::List(Box::new(Shape::Prod(vec![Shape::Int,Shape::Int]))), Shape::Int])),
    ];
    let requests = vec![Demand::Prod(vec![Demand::All,Demand::None]), Demand::Prod(vec![Demand::None,Demand::All]), Demand::All];
    for (text,input) in cases {
        let graph = corgi::compile(corgi::parse_ml(text).unwrap(), &input).unwrap();
        let want: Vec<_> = requests.iter().map(|d| corgi::analyze_demand(graph.graph(), &input, d).unwrap().input_demand().clone()).collect();
        assert_eq!(corgi::analyze_demands(graph.graph(), &input, &requests).unwrap(), want);
    }
}
