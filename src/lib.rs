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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
// macro mods
#[macro_use]
mod util;
// endof macro mods
// public mods
pub mod actions;
pub mod ddl;
pub mod error;
#[cfg(any(
    feature = "sync",
    feature = "pool",
    feature = "aio",
    feature = "aio-pool"
))]
pub mod pool;
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
    use core::{future::Future, pin::Pin};
    pub mod aio;
    pub use aio::Connection as AsyncConnection;
    use tokio::io::AsyncWriteExt;
    /// A special result that is returned when running actions (async)
    pub type AsyncResult<'s, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 's>>;
);

cfg_sync!(
    pub mod sync;
    pub use sync::Connection;
);

/// A generic result type
pub type SkyResult<T> = Result<T, self::error::Error>;
/// A result type for queries
pub type SkyQueryResult = SkyResult<Element>;

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
/// ```no_run
/// use skytable::ConnectionBuilder;
/// async fn run() {
///     let con =
///         ConnectionBuilder::new()
///         .set_host("127.0.0.1".to_string())
///         .set_port(2003)
///         .set_entity("mykeyspace:mytable".to_string())
///         .get_async_connection()
///         .await
///         .unwrap();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ConnectionBuilder {
    port: u16,
    host: String,
    entity: String,
}

impl Default for ConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionBuilder {
    /// Create an empty connection builder
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the port (defaults to `2003`)
    pub fn set_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    /// Set the host (defaults to `localhost`)
    pub fn set_host(mut self, host: String) -> Self {
        self.host = host;
        self
    }
    /// Set the entity (defaults to `default:default`)
    pub fn set_entity(mut self, entity: String) -> Self {
        self.entity = entity;
        self
    }
    cfg_sync! {
        /// Get a [sync connection](sync::Connection) to the database
        pub fn get_connection(&self) -> SkyResult<sync::Connection> {
            use crate::ddl::Ddl;
            let mut con =
                sync::Connection::new(&self.host, self.port)?;
            con.switch(&self.entity)?;
            Ok(con)
        }
        cfg_sync_ssl_any! {
            /// Get a [sync TLS connection](sync::TlsConnection) to the database
            pub fn get_tls_connection(
                &self,
                sslcert: String,
            ) -> SkyResult<sync::TlsConnection> {
                use crate::ddl::Ddl;
                let mut con = sync::TlsConnection::new(
                    &self.host,
                    self.port,
                    &sslcert,
                )?;
                con.switch(&self.entity)?;
                Ok(con)
            }
        }
    }
    cfg_async! {
        /// Get an [async connection](aio::Connection) to the database
        pub async fn get_async_connection(&self) -> SkyResult<aio::Connection> {
            use crate::ddl::AsyncDdl;
            let mut con = aio::Connection::new(&self.host, self.port)
                .await?;
            con.switch(&self.entity).await?;
            Ok(con)
        }
        cfg_async_ssl_any! {
            /// Get an [async TLS connection](aio::TlsConnection) to the database
            pub async fn get_async_tls_connection(
                &self,
                sslcert: String,
            ) -> SkyResult<aio::TlsConnection> {
                use crate::ddl::AsyncDdl;
                let mut con = aio::TlsConnection::new(
                    &self.host,
                    self.port,
                    &sslcert,
                )
                .await?;
                con.switch(&self.entity).await?;
                Ok(con)
            }
        }
    }
}

cfg_sync! {
    trait WriteQuerySync {
        fn write_sync(&self, b: &mut impl std::io::Write) -> IoResult<()>;
    }

    impl WriteQuerySync for Query {
        fn write_sync(&self, stream: &mut impl std::io::Write) -> IoResult<()> {
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
    }

    impl WriteQuerySync for Pipeline {
        fn write_sync(&self, stream: &mut impl std::io::Write) -> IoResult<()> {
            let len = self.len.to_string().into_bytes();
            stream.write_all(b"*")?;
            stream.write_all(&len)?;
            stream.write_all(b"\n")?;
            stream.write_all(&self.chain)
        }
    }
}

cfg_async! {
    use tokio::io::AsyncWrite;
    type FutureRet<'s> = Pin<Box<dyn Future<Output = IoResult<()>> + Send + Sync + 's>>;
    trait WriteQueryAsync<T: AsyncWrite + Unpin + Send + Sync>: Unpin + Sync + Send {
        fn write_async<'s>(&'s self, b: &'s mut T) -> FutureRet<'s>;
    }
    impl<T: AsyncWrite + Unpin + Send + Sync> WriteQueryAsync<T> for Query {
        fn write_async<'s>(&'s self, stream: &'s mut T) -> FutureRet {
            Box::pin(async move {
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
            })
        }
    }
    impl<T: AsyncWrite + Unpin + Send + Sync> WriteQueryAsync<T> for Pipeline {
        fn write_async<'s>(&'s self, stream: &'s mut T) -> FutureRet {
            Box::pin(async move {
                let len = self.len.to_string().into_bytes();
                stream.write_all(b"*").await?;
                stream.write_all(&len).await?;
                stream.write_all(b"\n").await?;
                stream.write_all(&self.chain).await
            })
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
    fn write_query_to_writable(&self, buffer: &mut Vec<u8>) {
        // Add the dataframe element
        let number_of_items_in_datagroup = self.len().to_string().into_bytes();
        buffer.extend([b'~']);
        buffer.extend(&number_of_items_in_datagroup);
        buffer.extend([b'\n']);
        buffer.extend(self.get_holding_buffer());
    }
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

/// # Pipeline
///
/// A pipeline is a way of queing up multiple queries, sending them to the server at once instead of sending them individually, avoiding
/// round-trip-times while also simplifying usage in several places. Responses are returned in the order they are sent.
///
/// ## Example with the sync API
///
/// ```no_run
/// use skytable::{query, Pipeline, Element, RespCode};
/// use skytable::sync::Connection;
///
/// let mut con = Connection::new("127.0.0.1", 2003).unwrap();
/// let pipe = Pipeline::new()
///     .add(query!("set", "x", "100"))
///     .add(query!("heya", "echo me!"));
///
/// let ret = con.run_pipeline(pipe).unwrap();
/// assert_eq!(
///     ret,
///     vec![
///         Element::RespCode(RespCode::Okay),
///         Element::String("echo me!".to_owned())
///     ]
/// );
/// ```
///
/// ## Example with the async API
///
/// ```no_run
/// use skytable::{query, Pipeline, Element, RespCode};
/// use skytable::aio::Connection;
///
/// async fn run() {
///     let mut con = Connection::new("127.0.0.1", 2003).await.unwrap();
///     let pipe = Pipeline::new()
///         .add(query!("set", "x", "100"))
///         .add(query!("heya", "echo me!"));
///
///     let ret = con.run_pipeline(pipe).await.unwrap();
///     assert_eq!(
///         ret,
///         vec![
///             Element::RespCode(RespCode::Okay),
///             Element::String("echo me!".to_owned())
///         ]
///     );
/// }
/// ```
///
pub struct Pipeline {
    len: usize,
    chain: Vec<u8>,
}

impl Pipeline {
    /// Initializes a new empty pipeline
    pub fn new() -> Self {
        Self {
            len: 0usize,
            chain: Vec::new(),
        }
    }
    /// Append a query (builder pattern)
    pub fn add(mut self, query: Query) -> Self {
        self.len += 1;
        query.write_query_to_writable(&mut self.chain);
        self
    }
    /// Append a query by taking reference
    pub fn push(&mut self, query: Query) {
        self.len += 1;
        query.write_query_to_writable(&mut self.chain);
    }
    /// Returns the number of queries in the pipeline
    pub fn len(&self) -> usize {
        self.len
    }
    cfg_dbg! {
        /// Returns the query packet representation of this pipeline
        ///
        /// ## Panics
        ///
        /// This function will panic if the query is empty
        pub fn into_raw_query(self) -> Vec<u8> {
            if self.len == 0 {
                panic!("The pipeline is empty")
            } else {
                let mut v = Vec::with_capacity(self.chain.len() + 4);
                v.push(b'*');
                v.extend(self.len.to_string().as_bytes());
                v.push(b'\n');
                v.extend(self.chain);
                v
            }
        }
    }
}

cfg_dbg! {
#[test]
    fn test_pipeline_dbg() {
        let bytes = b"*2\n~1\n5\nhello\n~1\n5\nworld\n";
        let pipe = Pipeline::new().add(query!("hello")).add(query!("world"));
        assert_eq!(pipe.into_raw_query(), bytes);
    }
}
