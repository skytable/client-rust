use skytable::ddl::{Ddl, Keymap, KeymapType};
use skytable::Connection;

fn main() {
    // initialize a connection
    let mut con = Connection::new("127.0.0.1", 2003).unwrap();

    // let's create the table
    // our table is a key/value table has the type (str,binstr)
    let mytable = Keymap::new("default:mytbl")
        .set_ktype(KeymapType::Str)
        .set_vtype(KeymapType::Binstr);
    con.create_table(mytable).unwrap();

    // now let's switch to the table
    con.switch("default:mytbl").unwrap();

    // now let's drop the table
    con.drop_table("default:mytbl").unwrap();
}
