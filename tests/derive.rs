use skytable::{query, Query, Response};

#[derive(Query, Response)]
struct User {
    username: String,
    password: String,
    email: Option<String>,
}

impl User {
    fn new(username: String, password: String, email: Option<String>) -> Self {
        Self {
            username,
            password,
            email,
        }
    }
}

#[test]
fn test_params() {
    let q = query!(
        "insert into myspace.mymodel(?, ?, ?)",
        User::new("sayan".into(), "pass".into(), None)
    );
    assert_eq!(q.param_cnt(), 3);
}
