use skytable::connection::Connection;
use skytable::{DataType, Query, RespCode, Response};

#[cfg(test)]
async fn flush_db(con: &mut Connection) {
    let mut query = Query::new();
    query.arg("flushdb");
    let res = con.run_simple_query(query).await.unwrap();
    assert_eq!(res, Response::Item(DataType::RespCode(RespCode::Okay)))
}

#[ignore]
#[tokio::test]
async fn test_basic_set() {
    let mut con = Connection::new("localhost", 2003).await.unwrap();
    flush_db(&mut con).await;
    let mut query = Query::new();
    query.arg("set").arg("x").arg("100");
    assert_eq!(
        con.run_simple_query(query).await.unwrap(),
        Response::Item(DataType::RespCode(RespCode::Okay))
    );
}
