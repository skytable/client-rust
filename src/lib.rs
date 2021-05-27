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
//! skytable = "0.3.0-alpha.4"
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
//!     assert_eq!(res, Response::Item(Element::String("HEY!".to_owned())));
//!     Ok(())
//! }
//! ```
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
//! skytable = { version = "0.3.0-alpha.4", features=["async"], default-features=false }
//! ```
//! You can now establish a connection by using `skytable::AsyncConnection::new()`, adding `.await`s wherever
//! necessary. Do note that you'll the [Tokio runtime](https://tokio.rs).
//!
//! ## Using both `sync` and `async` APIs
//!
//! With this client driver, it is possible to use both sync and `async` APIs **at the same time**. To do
//! this, simply change your import to:
//! ```toml
//! skytable = { version="0.3.0-alpha.4", features=["sync", "async"] }
//! ```
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
pub mod actions;
mod deserializer;
mod respcode;
pub mod types;
use crate::types::GetIterator;
use std::io::Result as IoResult;
use types::IntoSkyhashAction;
use types::IntoSkyhashBytes;
// async imports
#[cfg(feature = "async")]
mod async_con;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use async_con::Connection as AsyncConnection;
#[cfg(feature = "async")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "async")]
use tokio::net::TcpStream;
// default imports
pub use deserializer::Element;
pub use respcode::RespCode;
// sync imports
#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
mod sync;
#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
pub use sync::Connection;

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
/// fn main() {
///     let q = Query::new().arg("set").arg("x").arg("100");
/// }
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

impl Query {
    /// Create a new empty query with no arguments
    pub fn new() -> Self {
        Query {
            size_count: 0,
            data: Vec::new(),
        }
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
        let arg = arg.into_string();
        if arg.len() == 0 {
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
            self.push(a.into_string());
            self.push(b.into_string());
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
    #[cfg(feature = "async")]
    /// Write a query to a given stream
    async fn write_query_to(&self, stream: &mut tokio::io::BufWriter<TcpStream>) -> IoResult<()> {
        // Write the metaframe
        stream.write_all(b"*1\n").await?;
        // Add the dataframe
        let number_of_items_in_datagroup = self.__len().to_string().into_bytes();
        stream.write_all(&[b'_']).await?;
        stream.write_all(&number_of_items_in_datagroup).await?;
        stream.write_all(&[b'\n']).await?;
        stream.write_all(self.get_holding_buffer()).await?;
        Ok(())
    }
    #[cfg(feature = "sync")]
    /// Write a query to a given stream
    fn write_query_to_sync(&self, stream: &mut std::net::TcpStream) -> IoResult<()> {
        use std::io::Write;
        // Write the metaframe
        stream.write_all(b"*1\n")?;
        // Add the dataframe
        let number_of_items_in_datagroup = self.__len().to_string().into_bytes();
        stream.write_all(&[b'_'])?;
        stream.write_all(&number_of_items_in_datagroup)?;
        stream.write_all(&[b'\n'])?;
        stream.write_all(self.get_holding_buffer())?;
        Ok(())
    }
    #[cfg(feature = "dbg")]
    #[cfg_attr(docsrs, doc(cfg(feature = "dbg")))]
    /// Get the raw bytes of a query
    ///
    /// This is a function that is **not intended for daily use** but is for developers working to improve/debug
    /// or extend the Skyhash protocol. [Skytable](https://github.com/skytable/skytable) itself uses this function
    /// to generate raw queries. Once you're done passing the arguments to a query, running this function will
    /// return the raw query that would be written to the stream, serialized using the Skyhash serialization protocol
    pub fn into_raw_query(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.data.len());
        v.extend(b"*1\n");
        v.extend(b"_");
        v.extend(self.__len().to_string().into_bytes());
        v.extend(b"\n");
        v.extend(self.get_holding_buffer());
        v
    }
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
