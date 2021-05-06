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

//! # Database connections
//!
//! This crate provides a [`Connection`] object that can be used to connect to a Skytable database instance
//! and write/read queries/responses to/from it

use crate::deserializer::{self, ClientResult};
use crate::{Query, Response};
use bytes::{Buf, BytesMut};
pub use std::io::Result as IoResult;
use std::io::{Error, ErrorKind};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// 4 KB Read Buffer
const BUF_CAP: usize = 4096;

/// A `Connection` is a wrapper around a`TcpStream` and a read buffer
pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    /// Create a new connection to a Skytable instance hosted on `host` and running on `port`
    pub async fn new(host: &str, port: u16) -> IoResult<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        Ok(Connection {
            stream: stream,
            buffer: BytesMut::with_capacity(BUF_CAP),
        })
    }
    /// This function will write a [`Query`] to the stream and read the response from the
    /// server. It will then determine if the returned response is complete or incomplete
    /// or invalid and return an appropriate variant of [`Response`] wrapped in [`IoResult`]
    /// for any I/O errors that may occur
    pub async fn run_simple_query(&mut self, mut query: Query) -> IoResult<Response> {
        match query.write_query_to(&mut self.stream).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("ERROR: Couldn't write data to socket");
                return Err(e);
            }
        };
        loop {
            match self.stream.read_buf(&mut self.buffer).await {
                Ok(_) => (),
                Err(e) => return Err(e),
            }
            match self.try_response().await {
                ClientResult::Empty => break Err(Error::from(ErrorKind::ConnectionReset)),
                ClientResult::Incomplete => {
                    continue;
                }
                ClientResult::SimpleResponse(r, f) => {
                    self.buffer.advance(f);
                    break Ok(Response::Array(r));
                }
                ClientResult::ResponseItem(r, f) => {
                    self.buffer.advance(f);
                    break Ok(Response::Item(r));
                }
                ClientResult::InvalidResponse => {
                    self.buffer.clear();
                    break Ok(Response::InvalidResponse);
                }
                ClientResult::ParseError => {
                    self.buffer.clear();
                    break Ok(Response::ParseError);
                }
                ClientResult::PipelinedResponse(_, _) => {
                    todo!("Pipelined queries haven't been implemented yet!")
                }
            }
        }
    }
    /// This function is a subroutine of `run_query` used to parse the response packet
    async fn try_response(&mut self) -> ClientResult {
        if self.buffer.is_empty() {
            // The connection was possibly reset
            return ClientResult::Empty;
        }
        deserializer::parse(&self.buffer)
    }
}
