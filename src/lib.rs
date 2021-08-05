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
//! all Skytable versions that work with the [Skyhash 1.0 Protocol](https://docs.skytable.io/protocol/skyhash).
//! This version of the library was tested with the latest Skytable release
//! (release [0.6](https://github.com/skytable/skytable/releases/v0.6.0)).
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
//! skytable = "0.4.0"
//! ```
//! Now open up your `src/main.rs` file and establish a connection to the server while also adding some
//! imports:
//! ```no_run
//! use skytable::{Connection, Query, Response, Element};
//! fn main() -> std::io::Result<()> {
//!     let mut con = Connection::new("127.0.0.1", 2003)?;
//!     Ok(())
//! }
//! ```
//!
//! Now let's run a [`Query`]! Change the previous code block to:
//! ```no_run
//! use skytable::{Connection, Query, Response, Element};
//! fn main() -> std::io::Result<()> {
//!     let mut con = Connection::new("127.0.0.1", 2003)?;
//!     let query = Query::from("heya");
//!     let res = con.run_simple_query(&query)?;
//!     assert_eq!(res, Response::Item(Element::String(Vec::from("HEY"))));
//!     Ok(())
//! }
//! ```
//!
//! **And why was our string a [`Vec`]?**
//! That's because the server sends a binary string with arbitrary bytes. The returned value may
//! or may not be unicode, and this depends on the data type you set for your table.
//!
//! Way to go &mdash; you're all set! Now go ahead and run more advanced queries!
//!
//! ## Going advanced
//!
//! Now that you know how you can run basic queries, check out the [`actions`] module documentation for learning
//! to use actions and the [`types`] module documentation for implementing your own Skyhash serializable
//! types.
//!
//! ## Async API
//!
//! If you need to use an `async` API, just change your import to:
//! ```toml
//! skytable = { version = "0.4.0", features=["async"], default-features=false }
//! ```
//! You can now establish a connection by using `skytable::AsyncConnection::new()`, adding `.await`s wherever
//! necessary. Do note that you'll the [Tokio runtime](https://tokio.rs).
//!
//! ## Using both `sync` and `async` APIs
//!
//! With this client driver, it is possible to use both sync and `async` APIs **at the same time**. To do
//! this, simply change your import to:
//! ```toml
//! skytable = { version="0.4.0", features=["sync", "async"] }
//! ```
//!
//! ## TLS
//!
//! If you need to use TLS features, this crate will let you do so with OpenSSL.
//!
//! ### Using TLS with sync interfaces
//! ```toml
//! skytable = { version="0.4.0", features=["sync","ssl"] }
//! ```
//! You can now use the async `sync::TlsConnection` object.
//!
//! ### Using TLS with async interfaces
//! ```toml
//! skytable = { version="0.4.0", features=["async","aio-ssl"], default-features=false }
//! ```
//! You can now use the async `aio::TlsConnection` object.
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
#[macro_use]
mod util;
pub mod actions;
mod deserializer;
mod respcode;
pub mod types;
use crate::types::GetIterator;
pub use deserializer::Element;
pub use respcode::RespCode;
pub(crate) use std::io::Result as IoResult;
use types::IntoSkyhashAction;
use types::IntoSkyhashBytes;

cfg_async!(
    pub mod aio;
    pub use aio::Connection as AsyncConnection;
    use tokio::io::AsyncWriteExt;
);
cfg_sync!(
    pub mod sync;
    pub use sync::Connection;
);

#[derive(Debug)]
/// A connection builder for easily building connections
///
/// ## Example (sync)
/// ```no_run
/// let con =
///     ConnectionBuilder::new()
///     .set_host("127.0.0.1")
///     .set_port(2003)
///     .get_connection()
///     .unwrap();
/// ```
///
/// ## Example (async)
/// ```no_run
/// let con =
///     ConnectionBuilder::new()
///     .set_host("127.0.0.1")
///     .set_port(2003)
///     .get_async_connection()
///     .unwrap();
/// ```
pub struct ConnectionBuilder<'a> {
    port: Option<u16>,
    host: Option<&'a str>,
}

impl<'a> Default for ConnectionBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

pub type ConnectionBuilderResult<T> = Result<T, error::Error>;

impl<'a> ConnectionBuilder<'a> {
    /// Create an empty connection builder
    pub fn new() -> Self {
        Self {
            port: None,
            host: None,
        }
    }
    /// Set the port
    pub fn set_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
    /// Set the host
    pub fn set_host(mut self, host: &'a str) -> Self {
        self.host = Some(host);
        self
    }
    cfg_sync! {
        /// Get a [sync connection](sync::Connection) to the database
        pub fn get_connection(&self) -> ConnectionBuilderResult<sync::Connection> {
            let con =
                sync::Connection::new(self.host.unwrap_or("127.0.0.1"), self.port.unwrap_or(2003))?;
            Ok(con)
        }
        cfg_sync_ssl_any! {
            /// Get a [sync TLS connection](sync::TlsConnection) to the database
            pub fn get_tls_connection(
                &self,
                sslcert: String,
            ) -> ConnectionBuilderResult<sync::TlsConnection> {
                let con = sync::TlsConnection::new(
                    self.host.unwrap_or("127.0.0.1"),
                    self.port.unwrap_or(2003),
                    &sslcert,
                )?;
                Ok(con)
            }
        }
    }
    cfg_async! {
        /// Get an [async connection](aio::Connection) to the database
        pub async fn get_async_connection(&self) -> ConnectionBuilderResult<aio::Connection> {
            let con = aio::Connection::new(self.host.unwrap_or("127.0.0.1"), self.port.unwrap_or(2003))
                .await?;
            Ok(con)
        }
        cfg_async_ssl_any! {
            /// Get an [async TLS connection](aio::TlsConnection) to the database
            pub async fn get_async_tls_connection(
                &self,
                sslcert: String,
            ) -> ConnectionBuilderResult<aio::TlsConnection> {
                let con = aio::TlsConnection::new(
                    self.host.unwrap_or("127.0.0.1"),
                    self.port.unwrap_or(2003),
                    &sslcert,
                )
                .await?;
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
        skytable::Query::new()$(.arg($arg))*
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
    pub(in crate) fn _push_arg(&mut self, arg: impl IntoSkyhashBytes) {
        let arg = arg.as_string();
        if arg.is_empty() {
            panic!("Argument cannot be empty")
        }
        // A data element will look like:
        // `+<bytes_in_next_line>\n<data>`
        self.data.push(b'+');
        let bytes_in_next_line = arg.len().to_string().into_bytes();
        self.data.extend(bytes_in_next_line);
        // add the LF char
        self.data.push(b'\n');
        // Add the data itself, which is `arg`
        self.data.extend(arg.into_bytes());
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
            self.push(a.as_string());
            self.push(b.as_string());
        });
        self
    }
    /// Number of items in the datagroup
    pub(crate) fn __len(&self) -> usize {
        self.size_count
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
            let number_of_items_in_datagroup = self.__len().to_string().into_bytes();
            stream.write_all(&[b'_']).await?;
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
            let number_of_items_in_datagroup = self.__len().to_string().into_bytes();
            stream.write_all(&[b'_'])?;
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
            v.extend(b"*1\n_");
            v.extend(self.__len().to_string().into_bytes());
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
                // the tsymbol
                len += 1;
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

/// # Responses
///
/// This enum represents responses returned by the server. This can either be an array (or bulk), a single item
/// or can be a parse error if the server returned some data but it couldn't be parsed into the expected type
/// or it can be an invalid response in the event the server sent some invalid data.
/// This enum is `#[non_exhaustive]` as more types of responses can be added in the future.
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum Response {
    /// The server sent an invalid response
    InvalidResponse,
    /// The server responded with _something_. This can be any of the [`Element`] variants
    Item(Element),
    /// We failed to parse data
    ParseError,
    /// The server sent some data of a type that this client doesn't support
    UnsupportedDataType,
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

pub mod error {
    //! Errors
    cfg_ssl_any!(
        use std::fmt;
        /// Errors that may occur while initiating an [async TLS connection](crate::aio::TlsConnection)
        /// or a [sync TLS connection](crate::sync::TlsConnection)
        #[derive(Debug)]
        pub enum SslError {
            /// An [I/O Error](std::io::Error) occurred
            IoError(std::io::Error),
            /// An [SSL Error](openssl::error::Error) occurred
            SslError(openssl::ssl::Error),
        }

        impl From<openssl::ssl::Error> for SslError {
            fn from(e: openssl::ssl::Error) -> Self {
                Self::SslError(e)
            }
        }

        impl From<std::io::Error> for SslError {
            fn from(e: std::io::Error) -> Self {
                Self::IoError(e)
            }
        }

        impl From<openssl::error::ErrorStack> for SslError {
            fn from(e: openssl::error::ErrorStack) -> Self {
                Self::SslError(e.into())
            }
        }

        impl fmt::Display for SslError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                match self {
                    Self::IoError(e) => write!(f, "{}", e),
                    Self::SslError(e) => write!(f, "{}", e),
                }
            }
        }
    );
    #[derive(Debug)]
    /// An error originating from the Skyhash protocol
    pub enum SkyhashError {
        /// The server sent an invalid response
        InvalidResponse,
        /// The server sent a response but it could not be parsed
        ParseError,
        /// The server sent a data type not supported by this client version
        UnsupportedDataType,
    }

    #[derive(Debug)]
    /// A standard error type for the client driver
    pub enum Error {
        /// An I/O error occurred
        IoError(std::io::Error),
        #[cfg(any(
            feature = "ssl",
            feature = "sslv",
            feature = "aio-ssl",
            feature = "aio-sslv"
        ))]
        #[cfg_attr(
            docsrs,
            doc(cfg(any(
                feature = "ssl",
                feature = "sslv",
                feature = "aio-ssl",
                feature = "aio-sslv"
            )))
        )]
        /// An SSL error occurred
        SslError(openssl::ssl::Error),
        /// A Skyhash error occurred
        SkyError(SkyhashError),
        /// An application level parse error occurred
        ParseError,
    }

    cfg_ssl_any! {
        impl From<openssl::ssl::Error> for Error {
            fn from(err: openssl::ssl::Error) -> Self {
                Self::SslError(err)
            }
        }
    }

    impl From<std::io::Error> for Error {
        fn from(err: std::io::Error) -> Self {
            Self::IoError(err)
        }
    }

    cfg_ssl_any! {
        impl From<SslError> for Error {
            fn from(err: SslError) -> Self {
                match err {
                    SslError::IoError(ioerr) => Self::IoError(ioerr),
                    SslError::SslError(sslerr) => Self::SslError(sslerr),
                }
            }
        }
    }

    impl From<SkyhashError> for Error {
        fn from(err: SkyhashError) -> Self {
            Self::SkyError(err)
        }
    }
}
