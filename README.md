# Skytable client [![Crates.io](https://img.shields.io/crates/v/skytable?style=flat-square)](https://crates.io/crates/skytable) [![docs.rs](https://img.shields.io/docsrs/skytable?style=flat-square)](https://docs.rs/skytable) [![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/skytable/client-rust?include_prereleases&style=flat-square)](https://github.com/skytable/client-rust/releases)

## Introduction

This library is the official client for the free and open-source NoSQL database
[Skytable](https://github.com/skytable/skytable). First, go ahead and install Skytable by
following the instructions [here](https://docs.skytable.io/getting-started).

## Using this library

This library only ships with the bare minimum that is required for interacting with Skytable. Once you have
Skytable installed and running, you're ready to follow this guide!

We'll start by creating a new binary application and then running actions. Create a new binary application
by running:
```sh
cargo new skyapp
```
**Tip**: You can see a full list of the available actions [here](https://docs.skytable.io/actions-overview).

First add this to your `Cargo.toml` file:
```toml
skytable = "0.2.3"
```
Now open up your `src/main.rs` file and establish a connection to the server:
```rust
use skytable::{Connection};
async fn main() {
    let mut con = Connection::new("127.0.0.1", 2003).await.unwrap();
}
```

We get an error stating that `main()` cannot be `async`! Now `Connection` itself is an `async` connection
and hence needs to `await`. This is when you'll need a runtime like [Tokio](https://tokio.rs). The Skytable
database itself uses Tokio as its asynchronous runtime! So let's add `tokio` to our `Cargo.toml` and also add
the `#[tokio::main]` macro on top of our main function:

In `Cargo.toml`, add:
```toml
tokio = {version="1.5.0", features=["full"]}
```
And your `main.rs` should now look like:
```rust
use skytable::{Connection, Query, Response, RespCode, DataType};
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut con = Connection::new("127.0.0.1", 2003).await?;
}
```

Now let's run a `Query`! Add this below the previous line:
```rust
let mut query = Query::new();
query.arg("heya");
let res = con.run_simple_query(query).await?;
assert_eq!(res, Response::Item(DataType::Str("HEY!".to_owned())));
```

Way to go &mdash; you're all set! Now go ahead and run more advanced queries!

## Supported database versions

This library supports
all Skytable versions that work with the [Terrapipe 1.0 Protocol](https://docs.skytable.io/Protocol/terrapipe).
This version of the library was tested with the latest Skytable release
(release [0.5.1](https://github.com/skytable/skytable/releases/v0.5.1)).

## Contributing

Open-source, and contributions ... &mdash; they're always welcome! For ideas and suggestions,
[create an issue on GitHub](https://github.com/skytable/client-rust/issues/new) and for patches,
fork and open those pull requests [here](https://github.com/skytable/client-rust)!

## License
This client library is distributed under the permissive 
[Apache-2.0 License](https://github.com/skytable/client-rust/blob/next/LICENSE). Now go build great apps!
