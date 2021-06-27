use skytable::actions::AsyncActions;
use skytable::aio::Connection;

#[tokio::main]
async fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).await.unwrap();
    con.flushdb().await.unwrap();
    con.set("x", 100).await.unwrap();
    assert_eq!(con.get("x").await.unwrap(), "100".to_owned());
}
