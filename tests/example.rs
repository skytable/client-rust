use skytable::connection::Connection;
use skytable::{DataType, Query, RespCode, Response};

#[ignore]
#[tokio::test]
async fn test_basic_flushdb() {
    // WARNING: You need to have a running database server!
    let mut con = Connection::new("localhost", 2003).await.unwrap();
    let mut query = Query::new();
    query.arg("flushdb");
    match con.run_simple_query(query).await.unwrap() {
        Response::InvalidResponse => panic!("Server sent an invalid response"),
        Response::Item(item) => match item {
            DataType::RespCode(RespCode::Okay) => (),
            x @ _ => panic!("Server sent an unexpected data type: {:?}", x),
        },
        Response::Array(_) => panic!("We didn't expect an array"),
        Response::ParseError => panic!(
            "The server sent data but the client failed to parse it into the appropriate data type"
        ),
        _ => panic!("The server sent some unknown data type")
    }
    let mut query = Query::new();
    query.arg("set");
    query.arg("x");
    query.arg("100");
    assert!(con.run_simple_query(query).await.unwrap() == Response::Item(DataType::RespCode(RespCode::Okay)));
}
