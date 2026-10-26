use corgi::{optimize_with_demand, parse_ml};

#[test]
fn invalid_patterns_are_rejected_before_dead_code_is_removed() {
    for source in ["let (a, b) = input in 0", "input map ((a, b) -> 0)",
        "(0, input) fold ((acc, (a, b)) -> acc)"] {
        let g = parse_ml(source).unwrap();
        let shape = if source.starts_with("let") { corgi::Shape::Int }
            else { corgi::Shape::List(Box::new(corgi::Shape::Int)) };
        assert!(optimize_with_demand(&g, &shape).is_err(), "{source}");
    }
}
