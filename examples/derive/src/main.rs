use serde::{Deserialize, Serialize};
use skytable::actions::Actions;
use skytable::derive;
use skytable::Connection;

#[derive(Serialize, Deserialize, derive::json)]
pub struct User {
    name: String,
    verified: bool,
    email: String,
}

impl User {
    pub fn new(name: impl ToString, email: impl ToString, verified: bool) -> Self {
        Self {
            name: name.to_string(),
            verified,
            email: email.to_string(),
        }
    }
}

fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    let user = User::new("sayan", "ohsayan@outlook.com", true);
    con.set("sayan", &user).unwrap();
    let x: User = con.get("sayan").unwrap();
    assert_eq!(x.name, "sayan");
    assert_eq!(x.email, "ohsayan@outlook.com");
    assert!(x.verified);
}
