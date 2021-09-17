/*
 * Created on Wed May 05 2021
 *
 * Copyright (c) 2021 Sayan Nandan <nandansayan@outlook.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

//! # Skytable client
//!
//! This library is the official client for the free and open-source NoSQL database
//! [Skytable](https://github.com/skytable/skytable). First, go ahead and install Skytable by
//! following the instructions [here](https://docs.skytable.io/getting-started). This library supports
//! all Skytable versions that work with the [Skyhash 1.1 Protocol](https://docs.skytable.io/protocol/skyhash).
//! This version of the library was tested with the latest Skytable release
//! (release [0.7](https://github.com/skytable/skytable/releases/v0.7.0)).
//!
//! ## Using this library
//!
//! This library only ships with the bare minimum that is required for interacting with Skytable. Once you have
//! Skytable installed and running, you're ready to follow this guide!
//!
//! We'll start by creating a new binary application and then running actions. Create a new binary application
//! by running:
//! ```shell
//! cargo new skyapp
//! ```
//! **Tip**: You can see a full list of the available actions [here](https://docs.skytable.io/actions-overview).
//!
//! First add this to your `Cargo.toml` file:
//! ```toml
//! skytable = "0.6.0-alpha.2"
//! ```
//! Now open up your `src/main.rs` file and establish a connection to the server while also adding some
//! imports:
//! ```no_run
//! use skytable::{Connection, Query, Element};
//! fn main() -> std::io::Result<()> {
//!     let mut con = Connection::new("127.0.0.1", 2003)?;
//!     Ok(())
//! }
//! ```
//!
//! Now let's run a [`Query`]! Change the previous code block to:
//! ```no_run
//! use skytable::{error, Connection, Query, Element};
//! fn main() -> Result<(), error::Error> {
//!     let mut con = Connection::new("127.0.0.1", 2003)?;
//!     let query = Query::from("heya");
//!     let res = con.run_simple_query(&query)?;
//!     assert_eq!(res, Element::String("HEY".to_owned()));
//!     Ok(())
//! }
//! ```
//!
//! ## Running actions
//!
//! As noted [below](#binary-data), the default table is a key/value table with a binary key
//! type and a binary value type. Let's go ahead and run some actions (we're assuming you're
//! using the sync API; for async, simply change the import to `use skytable::actions::AsyncActions`).
//!
//! ### `SET`ting a key
//!
//! ```no_run
//! use skytable::actions::Actions;
//! use skytable::sync::Connection;
//!
//! let mut con = Connection::new("127.0.0.1", 2003).unwrap();
//! con.set("hello", "world").unwrap();
//! ```
//!
//! This will set the value of the key `hello` to `world` in the `default:default` entity.
//!
//! ### `GET`ting a key
//!
//! ```no_run
//! use skytable::actions::Actions;
//! use skytable::sync::Connection;
//!
//! let mut con = Connection::new("127.0.0.1", 2003).unwrap();
//! let x: String = con.get("hello").unwrap();
//! assert_eq!(x, "world");
//! ```
//!
//! Way to go &mdash; you're all set! Now go ahead and run more advanced queries!
//!
//! ## Binary data
//!
//! The `default:default` keyspace has the following declaration:
//! ```json
//! Keymap { data:(binstr,binstr), volatile:false }
//! ```
//!
//! This means that the default keyspace is ready to store binary data. Let's say
//! you wanted to `SET` the value of a key called `bindata` to some binary data stored
//! in a `Vec<u8>`. You can achieve this with the [`RawString`](types::RawString) type:
//!
//! ```no_run
//! use skytable::actions::Actions;
//! use skytable::sync::Connection;
//! use skytable::types::RawString;
//!
//! let mut con = Connection::new("127.0.0.1", 2003).unwrap();
//! let mybinarydata = RawString::from(vec![1, 2, 3, 4]);
//! assert!(con.set("bindata", mybinarydata).unwrap());
//! ```
//!
//! ## Going advanced
//!
//! Now that you know how you can run basic queries, check out the [`actions`] module documentation for learning
//! to use actions and the [`types`] module documentation for implementing your own Skyhash serializable
//! types. Need to meddle with DDL queries like creating and dropping tables? Check out the [`ddl`] module.
//! You can also find the [latest examples here](https://github.com/skytable/client-rust/tree/next/examples)
//!
//! ## Async API
//!
//! If you need to use an `async` API, just change your import to:
//! ```toml
//! skytable = { version = "0.6.0-alpha.2", features=["async"], default-features=false }
//! ```
//! You can now establish a connection by using `skytable::AsyncConnection::new()`, adding `.await`s wherever
//! necessary. Do note that you'll the [Tokio runtime](https://tokio.rs).
//!
//! ## Using both `sync` and `async` APIs
//!
//! With this client driver, it is possible to use both sync and `async` APIs **at the same time**. To do
//! this, simply change your import to:
//! ```toml
//! skytable = { version="0.6.0-alpha.2", features=["sync", "async"] }
//! ```
//!
//! ## TLS
//!
//! If you need to use TLS features, this crate will let you do so with OpenSSL.
//!
//! ### Using TLS with sync interfaces
//! ```toml
//! skytable = { version="0.6.0-alpha.2", features=["sync","ssl"] }
//! ```
//! You can now use the async [TlsConnection](`sync::TlsConnection`) object.
//!
//! ### Using TLS with async interfaces
//! ```toml
//! skytable = { version="0.6.0-alpha.2", features=["async","aio-ssl"], default-features=false }
//! ```
//! You can now use the async [TlsConnection](`aio::TlsConnection`) object.
//!
//! ### _Packed TLS_ setup
//!
//! If you want to pack OpenSSL with your crate, then for sync add `sslv` instead of `ssl` or
//! add `aio-sslv` instead of `aio-ssl` for async. Adding this will statically link OpenSSL
//! to your crate. Do note that you'll need a C compiler, GNU Make and Perl to compile OpenSSL
//! and statically link against it.
//!
//! ## Contributing
//!
//! Open-source, and contributions ... &mdash; they're always welcome! For ideas and suggestions,
//! [create an issue on GitHub](https://github.com/skytable/client-rust/issues/new) and for patches,
//! fork and open those pull requests [here](https://github.com/skytable/client-rust)!
//!
//! ## License
//! This client library is distributed under the permissive
//! [Apache-2.0 License](https://github.com/skytable/client-rust/blob/next/LICENSE). Now go build great apps!
//!

#![cfg_attr(docsrs, feature(doc_cfg))]
// macro mods
#[macro_use]
mod util;
// endof macro mods
// public mods
pub mod actions;
pub mod ddl;
pub mod error;
pub mod types;
// endof public mods
// private mods
mod deserializer;
mod respcode;
// endof private mods
use crate::types::GetIterator;
pub use deserializer::Element;
pub use respcode::RespCode;
pub(crate) use std::io::Result as IoResult;
use types::IntoSkyhashAction;
use types::IntoSkyhashBytes;

/// The default host address
pub const DEFAULT_HOSTADDR: &str = "127.0.0.1";
/// The default port
pub const DEFAULT_PORT: u16 = 2003;
/// The default entity
pub const DEFAULT_ENTITY: &str = "default:default";

cfg_async!(
    pub mod aio;
    pub use aio::Connection as AsyncConnection;
    use tokio::io::AsyncWriteExt;
);
cfg_sync!(
    pub mod sync;
    pub use sync::Connection;
);

/// A generic result type
pub type SkyRawResult<T> = Result<T, self::error::Error>;
/// A specialized error type for queries
pub type SkyResult = SkyRawResult<Element>;

#[derive(Debug, Clone)]
/// A connection builder for easily building connections
///
/// ## Example (sync)
/// ```no_run
/// use skytable::ConnectionBuilder;
/// let con =
///     ConnectionBuilder::new()
///     .set_host("127.0.0.1".to_string())
///     .set_port(2003)
///     .set_entity("mykeyspace:mytable".to_string())
///     .get_connection()
///     .unwrap();
/// ```
///
/// ## Example (async)
/// ```no_test
/// use skytable::ConnectionBuilder;
/// async fn main() {
///     let con =
///         ConnectionBuilder::new()
///         .set_host("127.0.0.1")
///         .set_port(2003)
///         .set_entity("mykeyspace:mytable".to_string())
///         .get_async_connection()
///         .await
///         .unwrap();
/// }
/// ```
pub struct ConnectionBuilder {
    port: Option<u16>,
    host: Option<String>,
    entity: Option<String>,
}

impl Default for ConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionBuilder {
    /// Create an empty connection builder
    pub fn new() -> Self {
        Self {
            port: None,
            host: None,
            entity: None,
        }
    }
    /// Set the port (defaults to `2003`)
    pub fn set_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
    /// Set the host (defaults to `localhost`)
    pub fn set_host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }
    /// Set the entity (defaults to `default:default`)
    pub fn set_entity(mut self, entity: String) -> Self {
        self.entity = Some(entity);
        self
    }
    cfg_sync! {
        /// Get a [sync connection](sync::Connection) to the database
        pub fn get_connection(&self) -> SkyRawResult<sync::Connection> {
            use crate::ddl::Ddl;
            let mut con =
                sync::Connection::new(self.host.as_ref().unwrap_or(&DEFAULT_HOSTADDR.to_owned()), self.port.unwrap_or(2003))?;
            con.switch(self.entity.as_ref().unwrap_or(&DEFAULT_ENTITY.to_owned()))?;
            Ok(con)
        }
        cfg_sync_ssl_any! {
            /// Get a [sync TLS connection](sync::TlsConnection) to the database
            pub fn get_tls_connection(
                &self,
                sslcert: String,
            ) -> SkyRawResult<sync::TlsConnection> {
                use crate::ddl::Ddl;
                let mut con = sync::TlsConnection::new(
                    self.host.as_ref().unwrap_or(&DEFAULT_HOSTADDR.to_owned()),
                    self.port.unwrap_or(2003),
                    &sslcert,
                )?;
                con.switch(self.entity.as_ref().unwrap_or(&DEFAULT_ENTITY.to_owned()))?;
                Ok(con)
            }
        }
    }
    cfg_async! {
        /// Get an [async connection](aio::Connection) to the database
        pub async fn get_async_connection(&self) -> SkyRawResult<aio::Connection> {
            use crate::ddl::AsyncDdl;
            let mut con = aio::Connection::new(self.host.as_ref().unwrap_or(&DEFAULT_HOSTADDR.to_owned()), self.port.unwrap_or(2003))
                .await?;
            con.switch(self.entity.as_ref().unwrap_or(&DEFAULT_ENTITY.to_owned())).await?;
            Ok(con)
        }
        cfg_async_ssl_any! {
            /// Get an [async TLS connection](aio::TlsConnection) to the database
            pub async fn get_async_tls_connection(
                &self,
                sslcert: String,
            ) -> SkyRawResult<aio::TlsConnection> {
                use crate::ddl::AsyncDdl;
                let mut con = aio::TlsConnection::new(
                    self.host.as_ref().unwrap_or(&DEFAULT_HOSTADDR.to_owned()),
                    self.port.unwrap_or(2003),
                    &sslcert,
                )
                .await?;
                con.switch(self.entity.as_ref().unwrap_or(&DEFAULT_ENTITY.to_owned())).await?;
                Ok(con)
            }
        }
    }
}

#[macro_export]
/// A macro that can be used to easily create queries with _almost_ variadic properties.
/// Where you'd normally create queries like this:
/// ```
/// use skytable::Query;
/// let q = Query::new().arg("mset").arg("x").arg("100").arg("y").arg("200");
/// ```
/// with this macro, you can just do this:
/// ```
/// use skytable::query;
/// let q = query!("mset", "x", "100", "y", "200");
/// ```
macro_rules! query {
    ($($arg:expr),+) => {
        $crate::Query::new()$(.arg($arg))*
    };
}

#[derive(Debug, PartialEq)]
/// This struct represents a single simple query as defined by the Skyhash protocol
///
/// A simple query is serialized into a flat string array which is nothing but a Skyhash serialized equivalent
/// of an array of [`String`] items. To construct a query like `SET x 100`, one needs to:
/// ```
/// use skytable::Query;
/// let q = Query::new().arg("set").arg("x").arg("100");
///
/// ```
/// You can now run this with a [`Connection`] or an `AsyncConnection`. You can also created queries [`From`]
/// objects that can be turned into actions. For example, these are completely valid constructions:
/// ```
/// use skytable::Query;
///
/// let q1 = Query::from(["mget", "x", "y", "z"]);
/// let q2 = Query::from(vec!["mset", "x", "100", "y", "200", "z", "300"]);
/// let q3 = Query::from("get").arg("x");
/// ```
/// **Important:** You should use the [`RawString`](types::RawString) type if you're willing to directly add things like
/// `Vec<u8>` to your query.
///
/// Finally, queries can also be created by taking references. For example:
/// ```
/// use skytable::Query;
///
/// let my_keys = vec!["key1", "key2", "key3"];
/// let mut q = Query::new();
/// for key in my_keys {
///     q.push(key);
/// }
/// ```
///
pub struct Query {
    size_count: usize,
    data: Vec<u8>,
}

impl<T> From<T> for Query
where
    T: IntoSkyhashAction,
{
    fn from(action: T) -> Self {
        Query::new().arg(action)
    }
}

impl Default for Query {
    fn default() -> Self {
        Query {
            size_count: 0,
            data: Vec::new(),
        }
    }
}

impl Query {
    /// Create a new empty query with no arguments
    pub fn new() -> Self {
        Query::default()
    }
    /// Add an argument to a query returning a [`Query`]. This can be used for queries built using the
    /// builder pattern. If you need to add items, by reference, consider using [`Query::push`]
    ///
    /// ## Panics
    /// This method will panic if the passed `arg` is empty
    pub fn arg(mut self, arg: impl IntoSkyhashAction) -> Self {
        arg.push_into_query(&mut self);
        self
    }
    pub(in crate) fn _push_arg(&mut self, arg: Vec<u8>) {
        if arg.is_empty() {
            panic!("Argument cannot be empty")
        }
        // A data element will look like:
        // `<bytes_in_next_line>\n<data>`
        let bytes_in_next_line = arg.len().to_string().into_bytes();
        self.data.extend(bytes_in_next_line);
        // add the LF char
        self.data.push(b'\n');
        // Add the data itself, which is `arg`
        self.data.extend(arg);
        self.data.push(b'\n'); // add the LF char
        self.size_count += 1;
    }
    /// Add an argument to a query taking a reference to it
    ///
    /// This is useful if you are adding queries in a loop than building it using the builder
    /// pattern (to use the builder-pattern, use [`Query::arg`])
    ///
    /// ## Panics
    /// This method will panic if the passed `arg` is empty
    pub fn push(&mut self, arg: impl IntoSkyhashAction) {
        assert!(
            arg.incr_len_by() != 0,
            "An argument passed to a query cannot be empty!"
        );
        arg.push_into_query(self);
    }
    pub(in crate) fn _push_alt_iter<T, U>(
        mut self,
        v1: impl GetIterator<T>,
        v2: impl GetIterator<U>,
    ) -> Query
    where
        T: IntoSkyhashBytes,
        U: IntoSkyhashBytes,
    {
        v1.get_iter().zip(v2.get_iter()).for_each(|(a, b)| {
            self._push_arg(a.as_bytes());
            self._push_arg(b.as_bytes());
        });
        self
    }
    /// Returns the number of arguments in this query
    pub fn len(&self) -> usize {
        self.size_count
    }
    /// Check if the query is empty
    pub fn is_empty(&self) -> bool {
        self.size_count == 0
    }
    fn get_holding_buffer(&self) -> &[u8] {
        &self.data
    }
    cfg_async!(
        /// Write a query to a given stream
        async fn write_query_to<T>(&self, stream: &mut T) -> IoResult<()>
        where
            T: tokio::io::AsyncWrite + Unpin,
        {
            // Write the metaframe
            stream.write_all(b"*1\n").await?;
            // Add the dataframe
            let number_of_items_in_datagroup = self.len().to_string().into_bytes();
            stream.write_all(&[b'~']).await?;
            stream.write_all(&number_of_items_in_datagroup).await?;
            stream.write_all(&[b'\n']).await?;
            stream.write_all(self.get_holding_buffer()).await?;
            stream.flush().await?;
            Ok(())
        }
    );
    cfg_sync!(
        /// Write a query to a given stream
        fn write_query_to_sync<T>(&self, stream: &mut T) -> IoResult<()>
        where
            T: std::io::Write,
        {
            // Write the metaframe
            stream.write_all(b"*1\n")?;
            // Add the dataframe
            let number_of_items_in_datagroup = self.len().to_string().into_bytes();
            stream.write_all(&[b'~'])?;
            stream.write_all(&number_of_items_in_datagroup)?;
            stream.write_all(&[b'\n'])?;
            stream.write_all(self.get_holding_buffer())?;
            stream.flush()?;
            Ok(())
        }
    );
    cfg_dbg!(
        /// Get the raw bytes of a query
        ///
        /// This is a function that is **not intended for daily use** but is for developers working to improve/debug
        /// or extend the Skyhash protocol. [Skytable](https://github.com/skytable/skytable) itself uses this function
        /// to generate raw queries. Once you're done passing the arguments to a query, running this function will
        /// return the raw query that would be written to the stream, serialized using the Skyhash serialization protocol
        pub fn into_raw_query(self) -> Vec<u8> {
            let mut v = Vec::with_capacity(self.data.len());
            v.extend(b"*1\n~");
            v.extend(self.len().to_string().into_bytes());
            v.extend(b"\n");
            v.extend(self.get_holding_buffer());
            v
        }
        /// Returns the expected size of a packet for the given lengths of the query
        /// This is not a _standard feature_ but is intended for developers working
        /// on Skytable
        pub fn array_packet_size_hint(element_lengths: impl AsRef<[usize]>) -> usize {
            let element_lengths = element_lengths.as_ref();
            let mut len = 0_usize;
            // *1\n_
            len += 4;
            let dig_count = |dig| -> usize {
                let dig_count = (dig as f32).log(10.0_f32).floor() + 1_f32;
                dig_count as usize
            };
            // the array size byte count
            len += dig_count(element_lengths.len());
            // the newline
            len += 1;
            element_lengths.iter().for_each(|elem| {
                // the digit length
                len += dig_count(*elem);
                // the newline
                len += 1;
                // the element's own length
                len += elem;
                // the final newline
                len += 1;
            });
            len
        }
    );
}

cfg_dbg!(
    #[test]
    fn my_query() {
        let q = vec!["set", "x", "100"];
        let ma_query_len = Query::from(&q).into_raw_query().len();
        let q_len =
            Query::array_packet_size_hint(q.iter().map(|v| v.len()).collect::<Vec<usize>>());
        assert_eq!(ma_query_len, q_len);
    }
);
