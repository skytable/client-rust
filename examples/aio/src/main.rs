use skytable::actions::AsyncActions;
use skytable::aio::Connection;
use skytable::ConnectionBuilder;

#[tokio::main]
async fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).await.unwrap();
    con.flushdb().await.unwrap();
    con.set("x", "100").await.unwrap();
    // example of getting a custom type
    let get: u8 = con.get("x").await.unwrap();
    assert_eq!(get, 100);

    // doing the same thing using a connection builder
    let _con = ConnectionBuilder::new()
        .set_host("127.0.0.1".to_string())
        .set_port(2003)
        .set_entity("default:default".to_owned())
        .get_async_connection()
        .await
        .unwrap();
}
