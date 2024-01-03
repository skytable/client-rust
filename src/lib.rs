/*
 * Copyright 2023, Sayan Nandan <nandansayan@outlook.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

//! # Skytable driver
//! [![Crates.io](https://img.shields.io/crates/v/skytable?style=flat-square)](https://crates.io/crates/skytable) [![Test](https://github.com/skytable/client-rust/actions/workflows/test.yml/badge.svg)](https://github.com/skytable/client-rust/actions/workflows/test.yml) [![docs.rs](https://img.shields.io/docsrs/skytable?style=flat-square)](https://docs.rs/skytable) [![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/skytable/client-rust?include_prereleases&style=flat-square)](https://github.com/skytable/client-rust/releases)
//!
//! This is [Skytable](https://github.com/skytable/skytable)'s official client driver for Rust that you can use to build applications.
//! The client-driver is distributed under the liberal [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0) and hence
//! you can use it in your applications without any licensing issues.
//!
//! ## Definitive example
//!
//! ```no_run
//! use skytable::{Query, Response, Config, query};
//!
//! #[derive(Query, Response)]
//! #[derive(Clone, PartialEq, Debug)] // we just do these for the assert (they are not needed)
//! struct User {
//!     userid: String,
//!     pass: String,
//!     followers: u64,
//!     email: Option<String>
//! }
//!
//! let our_user = User { userid: "user".into(), pass: "pass".into(), followers: 120, email: None };
//!
//! let mut db = Config::new_default("username", "password").connect().unwrap();
//!
//! // insert data
//! let q = query!("insert into myspace.mymodel(?, ?, ?, ?)", our_user.clone());
//! db.query_parse::<()>(&q).unwrap();
//!
//! // select data
//! let user: User = db.query_parse(&query!("select * from myspace.mymodel where username = ?", &our_user.userid)).unwrap();
//! assert_eq!(user, our_user);
//!
//! ```
//!
//! ## Getting started
//!
//! Make sure you have Skytable set up. If not, follow the [official installation guide here](https://docs.skytable.io/installation)
//! and then come back here.
//!
//! Let's run our first query. Connections are set up using the [`Config`] object and then we establish a connection by calling
//! [`Config::connect`] or use other functions for different connection configurations (for TLS or async).
//!
//! ```no_run
//! use skytable::{Config, query};
//! let mut db = Config::new_default("username", "password").connect().unwrap();
//! // the query `sysctl report status` returns a `Response::Empty` indicating that everything is okay. This is equivalent to
//! // rust's unit type `()` so that's what the driver uses
//! db.query_parse::<()>(&query!("sysctl report status")).unwrap();
//! ```
//!
//! We used the [`query!`] macro above which allows us to conveniently create queries when we don't need to handle complex
//! cases and we have a fixed number of arguments.
//!
//! ## Diving in
//!
//! Now let's say that we have a model `create model myspace.mymodel(username: string, password: string, followers: uint64)`
//! and we want to do some DML operations. Here's how we do it.
//!
//! ```no_run
//! use skytable::{Config, query};
//! let mut db = Config::new_default("username", "password").connect().unwrap();
//!
//! let insert_query = query!("insert into myspace.mymodel(?, ?, ?)", "sayan", "pass123", 1_500_000_u64);
//! db.query_parse::<()>(&insert_query).unwrap(); // insert will return empty on success
//!
//! let select_query = query!("select password, followers FROM myspace.mymodel WHERE username = ?", "sayan");
//! let (pass, followers): (String, u64) = db.query_parse(&select_query).unwrap();
//! assert_eq!(pass, "pass123");
//! assert_eq!(followers, 1_500_000_u64);
//!
//! let update_query = query!("update myspace.mymodel set followers += ? where username = ?", 1u64, "sayan");
//! db.query_parse::<()>(&update_query).unwrap(); // update will return empty on success
//! ```
//!
//! Now you're ready to run your own queries!
//!
//! ## Going advanced
//!
//! You can use the [`macro@Query`] and [`macro@Response`] derive macros to directly pass complex types as parameters
//! and read as responses. This should cover most of the general use-cases (otherwise you can manually implement them).
//!
//! - Custom [`mod@query`] generation
//! - Custom [`response`] parsing
//! - [`Connection pooling`](pool)
//!
//! ## Need help? Get help!
//!
//! Jump into [Skytable's official Discord server](https://discord.com/invite/QptWFdx) where maintainers, developers and fellow
//! users help each other out.
//!

// internal modules
#[macro_use]
mod macros;
mod protocol;
// public modules
pub mod aio;
pub mod config;
pub mod error;
pub mod pool;
pub mod query;
pub mod response;
pub mod syncio;
/// The `Query` derive macro enables you to directly pass complex types as parameters into queries
pub use sky_derive::Query;
/// The `Response` derive macro enables you to directly pass complex types as parameters into queries
pub use sky_derive::Response;
// re-exports
pub use {
    aio::{ConnectionAsync, ConnectionTlsAsync},
    config::Config,
    error::ClientResult,
    query::Query,
    syncio::{Connection, ConnectionTls},
};

/// we use a 8KB read buffer by default; allow this to be changed
const BUFSIZE: usize = 8 * 1024;
