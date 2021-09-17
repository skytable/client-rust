use serde::{Deserialize, Serialize};
use skytable::actions::Actions;
use skytable::error::Error;
use skytable::sync::Connection;
use skytable::types::FromSkyhashBytes;
use skytable::types::IntoSkyhashBytes;
use skytable::Element;
use skytable::SkyRawResult;

/// Our custom user type
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct User {
    name: String,
    email: String,
    verified: bool,
}

impl User {
    pub fn new(name: String, email: String, verified: bool) -> Self {
        Self {
            name,
            email,
            verified,
        }
    }
}

// Implement this for our type so that we can directly add it to queries
impl IntoSkyhashBytes for &User {
    fn as_bytes(&self) -> Vec<u8> {
        serde_json::to_string(self).unwrap().into_bytes()
    }
}

// Implement this for our type so that we can directly use it with actions/queries
impl FromSkyhashBytes for User {
    fn from_element(e: Element) -> SkyRawResult<Self> {
        // we want our JSON as a string
        let my_value_as_string: String = e.try_element_into()?;
        // now let us convert it into our struct
        match serde_json::from_str(&my_value_as_string) {
            // good, we got it
            Ok(v) => Ok(v),
            // nah, something bad happened. We'll turn the error into a string
            // and return it
            Err(e) => Err(Error::ParseError(e.to_string())),
        }
    }
}

fn main() {
    // let's create our user
    let sayan = User::new("Sayan".to_string(), "ohsayan@outlook.com".to_string(), true);
    // now connect to the server
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    // save our user in the database
    con.set("sayan", &sayan).unwrap();
    // now get the user
    let my_user: User = con.get("sayan").unwrap();
    // it'll be the same as our `sayan` variable!
    assert_eq!(my_user, sayan);
}
