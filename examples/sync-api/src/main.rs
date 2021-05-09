use skytable::sync::{Connection};
use skytable::{DataType, Query, Response};
fn main() {
    let mut query = Query::new(); query.arg("heya");
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    let resp = con.run_simple_query(query).unwrap();
    assert_eq!(resp, Response::Item(DataType::Str("HEY!".to_owned())));
}
