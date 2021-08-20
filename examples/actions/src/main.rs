use skytable::{actions::Actions, types::RawString, Connection};

fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    con.flushdb().unwrap();
    assert!(
        con.mset(vec!["x", "y", "z"], vec!["100", "200", "300"])
            .unwrap()
            == 3
    );
    let ret = con.mget(["x", "y", "z"]).unwrap();
    let ret = ret.try_into_string_array().unwrap();
    assert_eq!(
        vec!["100".to_owned(), "200".to_owned(), "300".to_owned()],
        ret
    );
    let mybinarydata = RawString::from(vec![1, 2, 3, 4]);
    assert!(con.set("mybindata", &mybinarydata).unwrap());
}
