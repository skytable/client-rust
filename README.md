# Skytable client [![Crates.io](https://img.shields.io/crates/v/skytable?style=flat-square)](https://crates.io/crates/skytable) [![docs.rs](https://img.shields.io/docsrs/skytable?style=flat-square)](https://docs.rs/skytable) [![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/skytable/client-rust?include_prereleases&style=flat-square)](https://github.com/skytable/client-rust/releases)

## Introduction

This library is the official client for the free and open-source NoSQL database
[Skytable](https://github.com/skytable/skytable). First, go ahead and install Skytable by
following the instructions [here](https://docs.skytable.io/getting-started). This library supports
all Skytable versions that work with the [Skyhash 1.0 Protocol](https://docs.skytable.io/protocol/skyhash).
This version of the library was tested with the latest Skytable release
(release [0.6](https://github.com/skytable/skytable/releases/v0.6.0)).

## Using this library

This library only ships with the bare minimum that is required for interacting with Skytable. Once you have
Skytable installed and running, you're ready to follow this guide!

We'll start by creating a new binary application and then running actions. Create a new binary application
by running:

```shell
cargo new skyapp
```

**Tip**: You can see a full list of the available actions [here](https://docs.skytable.io/actions-overview).

First add this to your `Cargo.toml` file:

```toml
skytable = "0.3.0"
```

Now open up your `src/main.rs` file and establish a connection to the server while also adding some
imports:

```rust
use skytable::{Connection, Query, Response, Element};
fn main() -> std::io::Result<()> {
    let mut con = Connection::new("127.0.0.1", 2003)?;
    Ok(())
}
```

Now let's run a `Query`! Change the previous code block to:

```rust
use skytable::{Connection, Query, Response, Element};
fn main() -> std::io::Result<()> {
    let mut con = Connection::new("127.0.0.1", 2003)?;
    let query = Query::from("heya");
    let res = con.run_simple_query(&query)?;
    assert_eq!(res, Response::Item(Element::String("HEY!".to_owned())));
    Ok(())
}
```

Way to go &mdash; you're all set! Now go ahead and run more advanced queries!

## Async API

If you need to use an `async` API, just change your import to:

```toml
skytable = { version = "0.3.0", features=["async"], default-features= false }
```

You can now establish a connection by using `skytable::AsyncConnection::new()`, adding `.await`s wherever
necessary. Do note that you'll the [Tokio runtime](https://tokio.rs).

## Using both `sync` and `async` APIs

With this client driver, it is possible to use both sync and `async` APIs **at the same time**. To do
this, simply change your import to:

```toml
skytable = { version="0.3.0", features=["sync", "async"] }
```

## TLS

If you need to use TLS features, this crate will let you do so with OpenSSL.

### Using TLS with sync interfaces

```toml
skytable = { version="0.4.0", features=["sync","ssl"] }
```

You can now use the sync `sync::TlsConnection` object.

### Using TLS with async interfaces

```toml
skytable = { version="0.4.0", features=["async","aio-ssl"], default-features=false }
```

You can now use the async `aio::TlsConnection` object.

### _Packed TLS_ setup

If you want to pack OpenSSL with your crate, then for sync add `sslv` instead of `ssl` or
add `aio-sslv` instead of `aio-ssl` for async. Adding this will statically link OpenSSL
to your crate. Do note that you'll need a C compiler, GNU Make and Perl to compile OpenSSL
and statically link against it.

## MSRV

The MSRV for this crate is Rust 1.39. Need const generics? Add the `const-gen` feature to your
dependency!

## Contributing

Open-source, and contributions ... &mdash; they're always welcome! For ideas and suggestions,
[create an issue on GitHub](https://github.com/skytable/client-rust/issues/new) and for patches,
fork and open those pull requests [here](https://github.com/skytable/client-rust)!

## License

This client library is distributed under the permissive
[Apache-2.0 License](https://github.com/skytable/client-rust/blob/next/LICENSE). Now go build great apps!
