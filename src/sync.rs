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
#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
use openssl::ssl::{Ssl, SslContext, SslMethod, SslStream};
pub use std::io::Result as IoResult;
use std::io::{Error, ErrorKind, Read, Write};
use std::net::TcpStream;

macro_rules! impl_sync_methods {
    ($ty:ty) => {
        impl $ty {
            /// This function will write a [`Query`] to the stream and read the response from the
            /// server. It will then determine if the returned response is complete or incomplete
            /// or invalid and return an appropriate variant of [`Response`] wrapped in [`IoResult`]
            /// for any I/O errors that may occur
            ///
            /// ## Panics
            /// This method will panic if the [`Query`] supplied is empty (i.e has no arguments)
            /// This function is a subroutine of `run_query` used to parse the response packet
            pub fn run_simple_query(&mut self, query: &Query) -> IoResult<Response> {
                assert!(query.__len() != 0, "A `Query` cannot be of zero length!");
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
                            ParseError::Empty => {
                                return Err(Error::from(ErrorKind::ConnectionReset))
                            }
                            ParseError::UnknownDatatype => {
                                return Ok(Response::UnsupportedDataType)
                            }
                        },
                    }
                }
            }
            fn try_response(&mut self) -> Result<(RawResponse, usize), ParseError> {
                if self.buffer.is_empty() {
                    // The connection was possibly reset
                    return Err(ParseError::Empty);
                }
                Parser::new(&self.buffer).parse()
            }
        }
        impl crate::actions::SyncSocket for $ty {
            fn run(&mut self, q: Query) -> std::result::Result<Response, std::io::Error> {
                self.run_simple_query(&q)
            }
        }
    };
}

/// 4 KB Read Buffer
const BUF_CAP: usize = 4096;

#[derive(Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
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
            stream,
            buffer: Vec::with_capacity(BUF_CAP),
        })
    }
}

impl_sync_methods!(Connection);

#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "ssl", feature = "sslv"))))]
pub enum SslError {
    IoError(std::io::Error),
    SslError(openssl::ssl::Error),
}

#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
impl From<openssl::ssl::Error> for SslError {
    fn from(e: openssl::ssl::Error) -> Self {
        Self::SslError(e)
    }
}

#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
impl From<std::io::Error> for SslError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
impl From<openssl::error::ErrorStack> for SslError {
    fn from(e: openssl::error::ErrorStack) -> Self {
        Self::SslError(e.into())
    }
}

#[derive(Debug)]
#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "ssl", feature = "sslv"))))]
pub struct SslConnection {
    stream: SslStream<TcpStream>,
    buffer: Vec<u8>,
}

#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "ssl", feature = "sslv"))))]
impl SslConnection {
    pub fn new(host: &str, port: u16, ssl_certificate: &str) -> Result<Self, SslError> {
        let mut ctx = SslContext::builder(SslMethod::tls_client())?;
        ctx.set_ca_file(ssl_certificate)?;
        let ssl = Ssl::new(&ctx.build())?;
        let stream = TcpStream::connect((host, port))?;
        let mut stream = SslStream::new(ssl, stream).map_err(|e| SslError::SslError(e.into()))?;
        stream.connect()?;
        Ok(Self {
            stream,
            buffer: Vec::with_capacity(BUF_CAP),
        })
    }
}

#[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "ssl", feature = "sslv"))))]
impl_sync_methods!(SslConnection);
