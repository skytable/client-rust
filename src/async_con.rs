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

use crate::deserializer::{ParseError, Parser, RawResponse};
use crate::{Query, Response};
use bytes::{Buf, BytesMut};
pub use std::io::Result as IoResult;
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// 4 KB Read Buffer
const BUF_CAP: usize = 4096;

#[derive(Debug)]
/// An async connection object that wraps around a`TcpStream` and a read buffer
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    /// Create a new connection to a Skytable instance hosted on `host` and running on `port`
    pub async fn new(host: &str, port: u16) -> IoResult<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        Ok(Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(BUF_CAP),
        })
    }
    /// This function will write a [`Query`] to the stream and read the response from the
    /// server. It will then determine if the returned response is complete or incomplete
    /// or invalid and return an appropriate variant of [`Response`] wrapped in [`IoResult`]
    /// for any I/O errors that may occur
    pub async fn run_simple_query(&mut self, query: &Query) -> IoResult<Response> {
        query.write_query_to(&mut self.stream).await?;
        self.stream.flush().await?;
        loop {
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                return Err(Error::from(ErrorKind::ConnectionReset));
            }
            match self.try_response() {
                Ok((query, forward_by)) => {
                    self.buffer.advance(forward_by);
                    match query {
                        RawResponse::SimpleQuery(s) => return Ok(Response::Item(s)),
                        RawResponse::PipelinedQuery(_) => {
                            unimplemented!("Pipelined queries aren't implemented yet")
                        }
                    }
                }
                Err(e) => match e {
                    ParseError::NotEnough => (),
                    ParseError::BadPacket | ParseError::UnexpectedByte => {
                        self.buffer.clear();
                        return Ok(Response::InvalidResponse);
                    }
                    ParseError::DataTypeParseError => return Ok(Response::ParseError),
                    ParseError::Empty => return Err(Error::from(ErrorKind::ConnectionReset)),
                    ParseError::UnknownDatatype => return Ok(Response::UnsupportedDataType),
                },
            }
        }
    }
    /// This function is a subroutine of `run_query` used to parse the response packet
    fn try_response(&mut self) -> Result<(RawResponse, usize), ParseError> {
        if self.buffer.is_empty() {
            // The connection was possibly reset
            return Err(ParseError::Empty);
        }
        Parser::new(&self.buffer).parse()
    }
}

impl crate::actions::AsyncSocket for crate::async_con::Connection {
    fn run(&mut self, q: Query) -> crate::actions::AsyncResult<std::io::Result<Response>> {
        Box::pin(async move { self.run_simple_query(&q).await })
    }
}
