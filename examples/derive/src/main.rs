use serde::{Deserialize, Serialize};
use skytable::actions::Actions;
use skytable::derive;
use skytable::Connection;

// this will save the object as JSON on the server side
#[derive(Serialize, Deserialize, derive::Skyjson)]
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

// this will save the object as binary data on the server side
#[derive(Serialize, Deserialize, derive::Skybin)]
pub struct Binuser {
    name: String,
    verified: bool,
    email: String,
}

impl Binuser {
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

    // try the JSON version
    let user = User::new("sayan", "ohsayan@outlook.com", true);
    con.set("sayan", &user).unwrap();
    let x: User = con.get("sayan").unwrap();
    assert_eq!(x.name, "sayan");
    assert_eq!(x.email, "ohsayan@outlook.com");
    assert!(x.verified);

    // try the binary version
    let user = Binuser::new("sayan", "ohsayan@outlook.com", true);
    con.set("sayan", &user).unwrap();
    let x: Binuser = con.get("sayan").unwrap();
    assert_eq!(x.name, "sayan");
    assert_eq!(x.email, "ohsayan@outlook.com");
    assert!(x.verified);
}
