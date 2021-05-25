/*
 * Created on Wed May 12 2021
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
pub use std::io::Result as IoResult;
use std::io::{Error, ErrorKind, Read, Write};
use std::net::TcpStream;

/// 4 KB Read Buffer
const BUF_CAP: usize = 4096;

#[derive(Debug)]
/// A `Connection` is a wrapper around a`TcpStream` and a read buffer
pub struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Connection {
    /// Create a new connection to a Skytable instance hosted on `host` and running on `port`
    pub fn new(host: &str, port: u16) -> IoResult<Self> {
        let stream = TcpStream::connect((host, port))?;
        Ok(Connection {
            stream: stream,
            buffer: Vec::with_capacity(BUF_CAP),
        })
    }
    /// This function will write a [`Query`] to the stream and read the response from the
    /// server. It will then determine if the returned response is complete or incomplete
    /// or invalid and return an appropriate variant of [`Response`] wrapped in [`IoResult`]
    /// for any I/O errors that may occur
    pub fn run_simple_query(&mut self, query: &Query) -> IoResult<Response> {
        query.write_query_to_sync(&mut self.stream)?;
        self.stream.flush()?;
        loop {
            let mut buffer = [0u8; 1024];
            match self.stream.read(&mut buffer) {
                Ok(0) => return Err(Error::from(ErrorKind::ConnectionReset)),
                Ok(read) => {
                    self.buffer.extend(&buffer[..read]);
                }
                Err(e) => return Err(e),
            }
            match self.try_response() {
                Ok((query, forward_by)) => {
                    self.buffer.drain(..forward_by);
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

impl crate::actions::SyncConnection for crate::sync::Connection {
    fn run(&mut self, q: Query) -> std::result::Result<Response, std::io::Error> {
        self.run_simple_query(&q)
    }
}
