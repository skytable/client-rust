use skytable::Connection;
use skytable::{DataType, Query, Response};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // First let's establish a connection
    let mut con = Connection::new("localhost", 2003).await?;
    // Now let's run a query to set a couple of items
    let mut query = Query::new();
    query
        .arg("mset")
        .arg("x") // x : ex
        .arg("ex")
        .arg("y") // y: why
        .arg("why")
        .arg("z") // z: zee
        .arg("zee");
    // Check if the command run was 3; we set 3 keys, so the server should return 3
    if let Response::Item(DataType::UnsignedInt(3)) = con.run_simple_query(query).await? {
        println!("Done setting the keys! Now let's get the values!");
    } else {
        panic!("Something else happened!");
    }
    // Now let's get those keys
    let mut get_keys = Query::new();
    get_keys.arg("mget").arg("x").arg("y").arg("z");
    let ret = con.run_simple_query(get_keys).await?;
    if let Response::Array(ret) = ret {
        for value in ret {
            // Now print them
            println!("{:?}", value);
            // You should see something like Str("ex"), Str("why") and Str("zee") printed in newlines
        }
    } else {
        panic!("Oh no, we expected an array but got something else!");
    }
    Ok(())
}
