use skytable::types::Array;
use skytable::{Connection, Element, Query};

fn main() {
    // example of running and handling a custom query

    // initiate the connection
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    // create your query
    let mut query = Query::from("MGET");
    query.push("x");
    query.push("y");
    query.push("z");
    // run it: you will either get a binary array if you declared your table
    // to have the `binstr` type for values or you'll get a string array if
    // you declared your table to have the `str` type for values
    match con.run_simple_query(&query) {
        Ok(Element::Array(Array::Bin(binarr))) => {
            println!("Got a binary array!");
            for element in binarr {
                match element {
                    Some(item) => println!("Got a blob: {:?}", item),
                    None => println!("Got null!"),
                }
            }
        }
        Ok(Element::Array(Array::Str(strarr))) => {
            println!("Got a string array!");
            for element in strarr {
                match element {
                    Some(item) => println!("Got a string: {}", item),
                    None => println!("Got null!"),
                }
            }
        }
        Ok(_) => eprintln!("Oh no, the server returned something bad!"),
        Err(e) => eprintln!("Oh no, an error occurred: {}", e),
    }
}
