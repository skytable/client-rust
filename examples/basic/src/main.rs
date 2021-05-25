use skytable::actions::Actions;
use skytable::Connection;

fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    con.set("sayan", "is writing code").unwrap();
    assert_eq!(con.get("sayan").unwrap(), "is writing code".to_owned());
}
