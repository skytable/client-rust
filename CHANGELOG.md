# Changelog
All changes in this project will be noted in this file.

## Version 0.3.0
> Breaking changes

* Dropped support for Terrapipe 1.0
* Added support for Skyhash 1.0
* Response variants have changed according to the protocol
* Added `sync` and `async` features with `sync` as a default feature and `async` as an optional feature
* Queries are now constructed like:
    ```rust
    let q = Query::new("set").arg("x").arg("100");
    ```
    instead of using `.arg(...)` multiple times across multiple lines
* `run_simple_query` now takes a reference to a `Query` instead of taking ownership of it
* Actions can now be run by importing `skytable::actions::Actions` (or `skytable::actions::AsyncActions` for the `async` API).  
For example:
    ```rust
    use skytable::{Connection, actions::Actions};
    fn main() {
        let mut con = Connection::new("127.0.0.1", 2003).unwrap();
        con.set("x", "100").unwrap();
        assert_eq!(con.get("x").unwrap(), "100".to_owned());
    }
    ```

## Version 0.2.0
> Breaking changes

* Fixed `Response` variant returning `Vec<DataGroup>` instead of just `DataGroup`
* Implemented `IntoIterator` for `DataGroup`

## Version 0.1.0
> This release has been yanked because it returned the incorrect type in the `Response` enum

Initial release