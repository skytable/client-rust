use skytable::{actions::Actions, Connection, Element};

fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    con.flushdb().unwrap();
    assert!(con.mset(vec!["x", "y", "z"], vec![100, 200, 300]).unwrap() == 3);
    let ret = con.mget(["x", "y", "z"]).unwrap();
    assert_eq!(
        vec![
            Element::String("100".to_owned()),
            Element::String("200".to_owned()),
            Element::String("300".to_owned())
        ],
        ret
    );
}
