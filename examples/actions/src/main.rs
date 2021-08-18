use skytable::{actions::Actions, Connection};

fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    con.flushdb().unwrap();
    assert!(
        con.mset(vec!["x", "y", "z"], vec!["100", "200", "300"])
            .unwrap()
            == 3
    );
    let ret = con.mget(["x", "y", "z"]).unwrap();
    let ret = ret.into_string_array().unwrap();
    assert_eq!(
        vec!["100".to_owned(), "200".to_owned(), "300".to_owned()],
        ret
    );
}
