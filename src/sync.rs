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

//! # Synchronous database connections
//!
//! This module provides sync interfaces for database connections. There are two versions:
//! - The [`Connection`]: a connection to the database over Skyhash/TCP
//! - The [`TlsConnection`]: a connection to the database over Skyhash/TLS
//!
//! All the [actions][crate::actions::Actions] can be used on both the connection types
//!

use crate::deserializer::{ParseError, Parser, RawResponse};
use crate::error::SkyhashError;
use crate::Element;
use crate::IoResult;
use crate::Pipeline;
use crate::Query;
use crate::SkyQueryResult;
use crate::SkyResult;
use crate::WriteQuerySync;
use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::net::TcpStream;

macro_rules! impl_sync_methods {
    ($ty:ty) => {
        impl $ty {
            /// This function will write a [`Query`] to the stream and read the response from the
            /// server. It will then determine if the returned response is complete or incomplete
            /// or invalid and return an appropriate variant of [`Error`](crate::error::Error) wrapped in [`IoResult`]
            /// for any I/O errors that may occur
            ///
            /// ## Panics
            /// This method will panic:
            /// - if the [`Query`] supplied is empty (i.e has no arguments)
            /// This function is a subroutine of `run_query` used to parse the response packet
            pub fn run_simple_query(&mut self, query: &Query) -> SkyQueryResult {
                assert!(query.len() != 0, "A `Query` cannot be of zero length!");
                match self._run_query(query)? {
                    RawResponse::SimpleQuery(sq) => Ok(sq),
                    RawResponse::PipelinedQuery(_) => Err(SkyhashError::InvalidResponse.into()),
                }
            }
            /// Runs a pipelined query. See the [`Pipeline`](Pipeline) documentation for a guide on
            /// usage
            pub fn run_pipeline(&mut self, pipeline: Pipeline) -> SkyResult<Vec<Element>> {
                assert!(pipeline.len() != 0, "A `Pipeline` cannot be empty!");
                match self._run_query(&pipeline)? {
                    RawResponse::PipelinedQuery(pq) => Ok(pq),
                    RawResponse::SimpleQuery(_) => Err(SkyhashError::InvalidResponse.into()),
                }
            }
            fn _run_query<T: WriteQuerySync>(&mut self, query: &T) -> SkyResult<RawResponse> {
                query.write_sync(&mut self.stream)?;
                self.stream.flush()?;
                loop {
                    let mut buffer = [0u8; 1024];
                    match self.stream.read(&mut buffer) {
                        Ok(0) => return Err(IoError::from(ErrorKind::ConnectionReset).into()),
                        Ok(read) => {
                            self.buffer.extend(&buffer[..read]);
                        }
                        Err(e) => return Err(e.into()),
                    }
                    match self.try_response() {
                        Ok((query, forward_by)) => {
                            self.buffer.drain(..forward_by);
                            return Ok(query);
                        }
                        Err(e) => match e {
                            ParseError::NotEnough => (),
                            ParseError::BadPacket | ParseError::UnexpectedByte => {
                                self.buffer.clear();
                                return Err(SkyhashError::InvalidResponse.into());
                            }
                            ParseError::DataTypeError => {
                                return Err(SkyhashError::ParseError.into())
                            }
                            ParseError::Empty => {
                                return Err(IoError::from(ErrorKind::ConnectionReset).into())
                            }
                            ParseError::UnknownDatatype => {
                                return Err(SkyhashError::UnknownDataType.into())
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
            fn run(&mut self, q: Query) -> SkyQueryResult {
                self.run_simple_query(&q)
            }
        }
    };
}

cfg_sync!(
    /// 4 KB Read Buffer
    const BUF_CAP: usize = 4096;

    #[derive(Debug)]
    /// A database connection over Skyhash/TCP
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

);

cfg_sync_ssl_any!(
    use openssl::ssl::{Ssl, SslContext, SslMethod, SslStream};
    use crate::error::Error;
    #[derive(Debug)]
    /// A database connection over Skyhash/TLS
    pub struct TlsConnection {
        stream: SslStream<TcpStream>,
        buffer: Vec<u8>,
    }

    impl TlsConnection {
        /// Pass the `host` and `port` and the path to the CA certificate to use for TLS
        pub fn new(host: &str, port: u16, ssl_certificate: &str) -> Result<Self, Error> {
            let mut ctx = SslContext::builder(SslMethod::tls_client())?;
            ctx.set_ca_file(ssl_certificate)?;
            let ssl = Ssl::new(&ctx.build())?;
            let stream = TcpStream::connect((host, port))?;
            let mut stream = SslStream::new(ssl, stream)?;
            stream.connect()?;
            Ok(Self {
                stream,
                buffer: Vec::with_capacity(BUF_CAP),
            })
        }
    }

    impl_sync_methods!(TlsConnection);
);
