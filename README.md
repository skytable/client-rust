# Skytable client [![Crates.io](https://img.shields.io/crates/v/skytable?style=flat-square)](https://crates.io/crates/skytable) [![Test](https://github.com/skytable/client-rust/actions/workflows/test.yml/badge.svg)](https://github.com/skytable/client-rust/actions/workflows/test.yml) [![docs.rs](https://img.shields.io/docsrs/skytable?style=flat-square)](https://docs.rs/skytable) [![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/skytable/client-rust?include_prereleases&style=flat-square)](https://github.com/skytable/client-rust/releases)

## Introduction

This library is the official client for the free and open-source NoSQL database
[Skytable](https://github.com/skytable/skytable). First, go ahead and install Skytable by
following the instructions [here](https://docs.skytable.io/getting-started). This library supports
all Skytable versions that work with the [Skyhash 1.1 Protocol](https://docs.skytable.io/protocol/skyhash).
This version of the library was tested with the latest Skytable release
(release [0.7.5](https://github.com/skytable/skytable/releases/v0.7.5)).

## Features

- Sync API
- Async API
- TLS in both sync/async APIs
- Connection pooling for sync/async
- Use both sync/async APIs at the same time
- Always up-to-date

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
skytable = "0.8.0-beta.1"
```

You're ready to go!

```rust
use skytable::{Config, query};

let mut db = Config::new_default("username", "password").connect().unwrap();
let query = query!("select username, password from myspace.mytbl WHERE username = ?", your_fn_to_get_user());
let (username, password) = db.query_parse::<(String, String)>(&query).unwrap();
```

> **Read [docs here to learn BlueQL](https://docs.skytable.io/)**

## Contributing

Open-source, and contributions ... &mdash; they're always welcome! For ideas and suggestions,
[create an issue on GitHub](https://github.com/skytable/client-rust/issues/new) and for patches,
fork and open those pull requests [here](https://github.com/skytable/client-rust)!

## License

This client library is distributed under the permissive
[Apache-2.0 License](https://github.com/skytable/client-rust/blob/next/LICENSE). Now go build great apps!
