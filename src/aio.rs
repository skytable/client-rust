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

//! # Asynchronous database connections
//!
//! This module provides async interfaces for database connections. There are two versions:
//! - The [`Connection`]: a connection to the database over Skyhash/TCP
//! - The [`TlsConnection`]: a connection to the database over Skyhash/TLS
//!
//! All the [async actions][crate::actions::AsyncActions] can be used on both the connection types
//!

use crate::deserializer::{ParseError, Parser, RawResponse};
use crate::error::Error;
use crate::error::SkyhashError;
use crate::Element;
use crate::IoResult;
use crate::Pipeline;
use crate::Query;
use crate::SkyRawResult;
use crate::SkyResult;
use crate::WriteQueryAsync;
use bytes::{Buf, BytesMut};
use std::io::{Error as IoError, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// 4 KB Read Buffer
const BUF_CAP: usize = 4096;

macro_rules! impl_async_methods {
    ($ty:ty, $inner:ty) => {
        impl $ty {
            /// This function will write a [`Query`] to the stream and read the response from the
            /// server. It will then determine if the returned response is complete or incomplete
            /// or invalid and return an appropriate variant of [`Error`](crate::error::Error) wrapped in [`IoResult`]
            /// for any I/O errors that may occur
            ///
            /// ## Panics
            /// This method will panic if the [`Query`] supplied is empty (i.e has no arguments)
            pub async fn run_simple_query(&mut self, query: &Query) -> SkyResult {
                match self._run_query(query).await? {
                    RawResponse::SimpleQuery(sq) => Ok(sq),
                    RawResponse::PipelinedQuery(_) => Err(SkyhashError::InvalidResponse.into()),
                }
            }
            pub async fn run_pipeline(&mut self, pipeline: Pipeline) -> SkyRawResult<Vec<Element>> {
                match self._run_query(&pipeline).await? {
                    RawResponse::PipelinedQuery(pq) => Ok(pq),
                    RawResponse::SimpleQuery(_) => Err(SkyhashError::InvalidResponse.into()),
                }
            }
            async fn _run_query<Q: WriteQueryAsync<$inner>>(
                &mut self,
                query: &Q,
            ) -> SkyRawResult<RawResponse> {
                query.write_async(&mut self.stream).await?;
                self.stream.flush().await?;
                loop {
                    if 0usize == self.stream.read_buf(&mut self.buffer).await? {
                        return Err(IoError::from(ErrorKind::ConnectionReset).into());
                    }
                    match self.try_response() {
                        Ok((query, forward_by)) => {
                            self.buffer.advance(forward_by);
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
            /// This function is a subroutine of `run_query` used to parse the response packet
            fn try_response(&mut self) -> Result<(RawResponse, usize), ParseError> {
                if self.buffer.is_empty() {
                    // The connection was possibly reset
                    return Err(ParseError::Empty);
                }
                Parser::new(&self.buffer).parse()
            }
        }
        impl crate::actions::AsyncSocket for $ty {
            fn run(&mut self, q: Query) -> crate::actions::AsyncResult<SkyResult> {
                Box::pin(async move { self.run_simple_query(&q).await })
            }
        }
    };
}

cfg_async!(
    /// An asynchronous database connection over Skyhash/TCP
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
    }
    impl_async_methods!(Connection, BufWriter<TcpStream>);
);

cfg_async_ssl_any!(
    use tokio_openssl::SslStream;
    use openssl::ssl::{SslContext, SslMethod, Ssl};
    use core::pin::Pin;

    /// An asynchronous database connection over Skyhash/TLS
    pub struct TlsConnection {
        stream: SslStream<TcpStream>,
        buffer: BytesMut
    }

    impl TlsConnection {
        /// Pass the `host` and `port` and the path to the CA certificate to use for TLS
        pub async fn new(host: &str, port: u16, sslcert: &str) -> Result<Self, Error> {
            let mut ctx = SslContext::builder(SslMethod::tls_client())?;
            ctx.set_ca_file(sslcert)?;
            let ssl = Ssl::new(&ctx.build())?;
            let stream = TcpStream::connect((host, port)).await?;
            let mut stream = SslStream::new(ssl, stream)?;
            Pin::new(&mut stream).connect().await?;
            Ok(Self {
                stream,
                buffer: BytesMut::with_capacity(BUF_CAP),
            })
        }
    }
    impl_async_methods!(TlsConnection, SslStream<TcpStream>);
);
