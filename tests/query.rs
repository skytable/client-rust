use skytable::query;

#[test]
fn param_cnt_zero() {
    let q = query!("sysctl report status");
    assert_eq!(q.param_cnt(), 0);
    assert_eq!(q.query_str(), "sysctl report status");
}

#[test]
fn param_cnt() {
    let q = query!("insert into myspace.mymodel(?, ?)", "username", "password");
    assert_eq!(q.param_cnt(), 2);
    assert_eq!(q.query_str(), "insert into myspace.mymodel(?, ?)");
}
