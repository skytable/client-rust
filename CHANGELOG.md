# Changelog

All changes in this project will be noted in this file.

## Version 0.6.0

> Breaking changes

### Added

- Support for DDL queries
- Support for directly getting types from actions. For example:
  ```rust
  let mut x: u64 = con.get("my integer key").unwrap();
  ```
  All errors resulting from this parse are simply propagated into the `Error::ParseError`
  variant
- Support for retrieval of custom types through the use of the `FromSkyhashBytes` trait

### Broken

- Errors have been unified into a single error type
- Some types have been removed to aid simplicity:
  - `types::Str`
  - `types::SimpleArray`

## Version 0.5.0

> Breaking changes

- Added support for Skyhash 1.1
- Changed `Query` type to `AnyArray` as in the latest protocol

## Version 0.4.0

> Breaking changes

- Added backward compatibility for Rust versions < 1.51 (
  people who need const-generics just add `const-gen` to their feature list)
- Added support for the `POP` query type
- `into_string` is now called `as_string` to follow naming conventions (trait `IntoSkyhashBytes`)
- New SSL features:
  - `ssl`: For sync TLS
  - `sslv`: For sync TLS (vendored)
  - `aio-ssl`: For async TLS
  - `aio-sslv`: For async TLS (vendored)

## Version 0.3.0

> Breaking changes

- Dropped support for Terrapipe 1.0
- Added support for Skyhash 1.0
- Response variants have changed according to the protocol
- Added `sync` and `async` features with `sync` as a default feature and `async` as an optional feature
- Queries constructed using `Query::arg()` now follow the builder pattern
- Queries can be constructed by taking references using `Query::push`
- `run_simple_query` now takes a reference to a `Query` instead of taking ownership of it
- Actions can now be run by importing `skytable::actions::Actions` (or `skytable::actions::AsyncActions` for the `async` API).  
  For example:
  `rust use skytable::{Connection, actions::Actions}; fn main() { let mut con = Connection::new("127.0.0.1", 2003).unwrap(); con.set("x", "100").unwrap(); assert_eq!(con.get("x").unwrap(), "100".to_owned()); } `
- `run_simple_query` (both sync and async) verify whether the query is empty or not. If it is, the function will
  panic. This is a very important check to avoid confusion as the server would return a `PacketError` which might
  create additional confusion
- `Query` objects can now be constructed with the `From` trait on appropriate types (such as single items or
  sequences)

## Version 0.2.0

> Breaking changes

- Fixed `Response` variant returning `Vec<DataGroup>` instead of just `DataGroup`
- Implemented `IntoIterator` for `DataGroup`

## Version 0.1.0

> This release has been yanked because it returned the incorrect type in the `Response` enum

Initial release
