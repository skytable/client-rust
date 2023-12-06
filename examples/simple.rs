use skytable::{query, Config};

/// a dummy function to fetch username and password from a request
fn dummy_web_fetch_username_password() -> (String, String) {
    ("rickastley".into(), "rick123".into())
}

fn dummy_respond_to_request(_followers: u64) {}

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
        "create model myspace.mymodel(username: string, password: string, followers: uint64"
    ))
    .unwrap();

    // manipulate data

    let (form_username, form_pass) = dummy_web_fetch_username_password();
    // insert some data
    db.query_parse::<()>(&query!(
        "insert into myspace.mymodel(?, ?, ?)",
        &form_username,
        form_pass,
        100_000_000u64
    ))
    .unwrap();

    // get it back
    let (password, followers): (String, u64) = db
        .query_parse(&query!(
            "select password, followers FROM myspace.mymodel WHERE username = ?",
            &form_username
        ))
        .unwrap();
    assert_eq!(password, "rick123", "password changed!");
    // send to our client
    dummy_respond_to_request(followers);

    // update followers to account for huge numbers who were angry after being rickrolled
    db.query_parse::<()>(&query!(
        "update myspace.mymodel SET followers -= ? WHERE username = ?",
        50_000_000u64,
        &form_username
    ))
    .unwrap();

    // alright, everyone is tired from being rickrolled so we'll have to ban rick's account
    db.query_parse::<()>(&query!(
        "delete from myspace.mymodel where username = ?",
        &form_username
    ))
    .unwrap();
}
