use skytable::actions::Actions;
use skytable::Connection;
use skytable::ConnectionBuilder;

fn main() {
    // simple example
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    con.set("sayan", "is writing code").unwrap();
    let get: String = con.get("sayan").unwrap();
    assert_eq!(get, "is writing code".to_owned());

    // getting a connection using the connection builder
    let _con = ConnectionBuilder::new()
        .set_host("127.0.0.1".to_string())
        .set_port(2003)
        .set_entity("default:default".to_owned())
        .get_connection()
        .unwrap();
}
