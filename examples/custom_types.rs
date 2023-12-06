use skytable::{
    query,
    query::SQParam,
    response::{FromResponse, Response},
    ClientResult, Config,
};

#[derive(Debug, PartialEq, Clone)]
struct User {
    username: String,
    password: String,
    followers: u64,
    email: Option<String>,
}

impl User {
    fn new(username: String, password: String, followers: u64, email: Option<String>) -> Self {
        Self {
            username,
            password,
            followers,
            email,
        }
    }
}

impl SQParam for User {
    fn append_param(&self, q: &mut Vec<u8>) -> usize {
        self.username.append_param(q)
            + self.password.append_param(q)
            + self.followers.append_param(q)
            + self.email.append_param(q)
    }
}

impl FromResponse for User {
    fn from_response(resp: Response) -> ClientResult<Self> {
        let (username, password, followers, email) = FromResponse::from_response(resp)?;
        Ok(Self::new(username, password, followers, email))
    }
}

fn main() {
    let mut db = Config::new_default("username", "password")
        .connect()
        .unwrap();

    // set up schema
    // create space
    db.query_parse::<()>(&query!("create space myspace"))
        .unwrap();
    // create model
    db.query_parse::<()>(&query!(
        "create model myspace.mymodel(username: string, password: string, followers: uint64, null email: string"
    ))
    .unwrap();

    // insert data
    let our_user = User::new("myuser".into(), "pass123".into(), 0, None);
    db.query_parse::<()>(&query!(
        "insert into myspace.mymodel(?, ?, ?, ?)",
        our_user.clone()
    ))
    .unwrap();

    // select data
    let ret_user: User = db
        .query_parse(&query!(
            "select * from myspace.mymodel WHERE username = ?",
            &our_user.username
        ))
        .unwrap();

    assert_eq!(our_user, ret_user);
}
