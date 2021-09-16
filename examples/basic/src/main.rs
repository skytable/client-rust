use skytable::actions::Actions;
use skytable::Connection;

fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    con.set("sayan", "is writing code").unwrap();
    let get: String = con.get("sayan").unwrap();
    assert_eq!(get, "is writing code".to_owned());
}
