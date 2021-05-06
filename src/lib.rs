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
//! all Skytable versions that work with the [Terrapipe 1.0 Protocol](https://docs.skytable.io/Protocol/terrapipe).
//! This version of the library was tested with the latest Skytable release
//! (release [0.5.1](https://github.com/skytable/skytable/releases/v0.5.1)).
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
//! skytable = "0.2.3"
//! ```
//! Now open up your `src/main.rs` file and establish a connection to the server:
//! ```ignore
//! use skytable::{Connection};
//! async fn main() -> std::io::Result<()> {
//!     let mut con = Connection::new("127.0.0.1", 2003).await?;
//! }
//! ```
//!
//! We get an error stating that `main()` cannot be `async`! Now [`Connection`] itself is an `async` connection
//! and hence needs to `await`. This is when you'll need a runtime like [Tokio](https://tokio.rs). The Skytable
//! database itself uses Tokio as its asynchronous runtime! So let's add `tokio` to our `Cargo.toml` and also add
//! the `#[tokio::main]` macro on top of our main function:
//!
//! In `Cargo.toml`, add:
//! ```toml
//! tokio = {version="1.5.0", features=["full"]}
//! ```
//! And your `main.rs` should now look like:
//! ```no_run
//! use skytable::{Connection, Query, Response, RespCode, DataType};
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let mut con = Connection::new("127.0.0.1", 2003).await?;
//!     Ok(())
//! }
//! ```
//!
//! Now let's run a [`Query`]! Add this below the previous line:
//! ```ignore
//! let mut query = Query::new();
//! query.arg("heya");
//! let res = con.run_simple_query(query).await?;
//! assert_eq!(res, Response::Item(DataType::Str("HEY!".to_owned())));
//! ```
//!
//! Way to go &mdash; you're all set! Now go ahead and run more advanced queries!
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

pub mod connection;
mod deserializer;
mod terrapipe;

use crate::connection::IoResult;
use crate::deserializer::DataGroup;
pub use crate::deserializer::DataType;
pub use connection::Connection;
pub use terrapipe::RespCode;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug, PartialEq)]
/// This struct represents a single simple query as defined by the Terrapipe protocol
pub struct Query {
    size_count: usize,
    data: Vec<u8>,
}

impl Query {
    /// Create an empty query
    pub fn new() -> Self {
        Query {
            size_count: 0,
            data: Vec::new(),
        }
    }
    /// Add an argument to a query
    ///
    /// ## Panics
    /// This method will panic if the passed `arg` is empty
    pub fn arg(&mut self, arg: impl ToString) -> &mut Self {
        let arg = arg.to_string();
        if arg.len() == 0 {
            panic!("Argument cannot be empty")
        }
        self.size_count += 1;
        // A data element will look like:
        // `#<bytes_in_next_line>\n<data>`
        self.data.push(b'#');
        let bytes_in_next_line = arg.len().to_string().into_bytes();
        self.data.extend(bytes_in_next_line);
        // add the LF char
        self.data.push(b'\n');
        // Add the data itself, which is `arg`
        self.data.extend(arg.into_bytes());
        self.data.push(b'\n'); // add the LF char
        self
    }
    /// Number of items in the datagroup
    fn __len(&self) -> usize {
        self.size_count
    }
    fn get_holding_buffer(&self) -> &[u8] {
        &self.data
    }
    /// Write a query to a given stream
    async fn write_query_to(&mut self, stream: &mut TcpStream) -> IoResult<()> {
        // Write the metaline
        stream.write(b"#2\n*1\n").await?;
        // Add the dataframe layout
        // The dataframe layout looks like: #<number_of_items_in_datagroup_len_for_sizeline>\n&<number_of_items_in_datagroup>\n
        let number_of_items_in_datagroup = self.__len().to_string().into_bytes();
        let number_of_items_in_datagroup_len_for_sizeline = (number_of_items_in_datagroup.len()
            + 1)
        .to_string()
        .into_bytes();
        // Now write the dataframe layout
        stream.write(&[b'#']).await?;
        stream
            .write(&number_of_items_in_datagroup_len_for_sizeline)
            .await?;
        stream.write(&[b'\n', b'&']).await?;
        stream.write(&number_of_items_in_datagroup).await?;
        stream.write(&[b'\n']).await?;
        stream.write(self.get_holding_buffer()).await?;
        // Clear out the holding buffer for running other commands
        {
            self.data.clear()
        }
        Ok(())
    }
}

/// # Responses
///
/// This enum represents responses returned by the server. This can either be an array (or bulk), a single item
/// or can be a parse error if the server returned some data but it couldn't be parsed into the expected type
/// or it can be an invalid response in the event the server sent some invalid data.
///
/// ## Notes
/// - This enum is `#[non_exhaustive]` as more types of responses can be added in the future
/// - The `Response::Item` field is just a simple abstraction provided by this client library; Skytable's Terrapipe
/// protocol (as of 1.0) doesn't discriminate between single and multiple elements returned in a data group, That is
/// to say if an action like `GET x` returns (and will return) a single element in a datagroup, then it is passed
/// into this variant; Terrapipe 1.0 always sends arrays
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum Response {
    /// The server sent an invalid response
    InvalidResponse,
    /// An array of items
    /// 
    /// The server has responded with an array of items in a single datagroup. This variant wraps around
    /// `Vec<DataType>`
    Array(DataGroup),
    /// A single item
    ///
    /// This is a client abstraction for a datagroup that only has one element
    Item(DataType),
    /// We failed to parse data
    ParseError,
}
