# Skytable client [![Crates.io](https://img.shields.io/crates/v/skytable?style=flat-square)](https://crates.io/crates/skytable) [![Test](https://github.com/skytable/client-rust/actions/workflows/test.yml/badge.svg)](https://github.com/skytable/client-rust/actions/workflows/test.yml) [![docs.rs](https://img.shields.io/docsrs/skytable?style=flat-square)](https://docs.rs/skytable) [![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/skytable/client-rust?include_prereleases&style=flat-square)](https://github.com/skytable/client-rust/releases)

## Introduction

This library is the official client for the free and open-source NoSQL database [Skytable](https://github.com/skytable/skytable). First, go ahead and install Skytable by following the instructions [here](https://docs.skytable.io/getting-started). This library supports all Skytable versions that work with the [Skyhash 2 Protocol](https://docs.skytable.io/protocol/overview). This version of the library was tested with the latest Skytable release (release [0.8.4](https://github.com/skytable/skytable/releases/v0.8.4)). [Read more about supported versions here](#version-support).

**📁 You can [find some usage examples in this folder here](/examples)**.

## Definitive example

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
skytable = "0.8"
```

You're ready to go!

```rust
use skytable::{
    Query, Response, Config, query,
    response::Rows,
};

#[derive(Query, Response)]
#[derive(Clone, PartialEq, Debug)] // we just do these for the assert (they are not needed)
struct User {
    userid: String,
    pass: String,
    followers: u64,
    email: Option<String>
}

let our_user = User { userid: "user".into(), pass: "pass".into(), followers: 120, email: None };

let mut db = Config::new_default("username", "password").connect().unwrap();

// insert data
let q = query!("insert into myspace.mymodel(?, ?, ?, ?)", our_user.clone());
db.query_parse::<()>(&q).unwrap();

// select data
let user: User = db.query_parse(&query!("select * from myspace.mymodel where username = ?", &our_user.userid)).unwrap();
assert_eq!(user, our_user);

// select multiple rows
let users: Rows<User> = db.query_parse(&query!("select all * from myspace.mymodel limit ?", 1000u64)).unwrap();
assert_eq!(users[0].userid, "user");
```

> **Read [docs here to learn BlueQL](https://docs.skytable.io/)**


## Version support

- Minimum Supported Rust Version (MSRV): 1.75.0
- Minimum Supported Skytable Version: 0.8.0

## Features

- Sync API
- Async API
- TLS in both sync/async APIs
- Connection pooling for sync/async
- Use both sync/async APIs at the same time
- Always up-to-date

## Contributing

Contributions are always welcome. To submit patches please fork this repository and submit a pull request. If you find any bugs, [please open an issue here](https://github.com/skytable/client-rust/issues/new).

## License

This library is distributed under the [Apache-2.0 License](https://github.com/skytable/client-rust/blob/next/LICENSE).
