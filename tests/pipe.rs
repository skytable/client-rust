use skytable::{query, query::Pipeline};

#[test]
fn compile_add_queries() {
    let mut pipeline: Pipeline = (0..123)
        .map(|num| query!("select * from mymodel where username = ?", num as u64))
        .collect();
    assert_eq!(pipeline.query_count(), 123);
    let query = query!("systemctl report status");
    pipeline.extend(vec![&query]);
    assert_eq!(pipeline.query_count(), 124);
}
