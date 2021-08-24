/*
 * Created on Wed Aug 18 2021
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

//! # Errors
//!
//! This module contains error types that the client returns in different cases

use crate::RespCode;
use core::fmt;

#[derive(Debug)]
#[non_exhaustive]
/// An error originating from the Skyhash protocol
pub enum SkyhashError {
    /// The server sent data but we failed to parse it
    ParseError,
    /// The server sent an unexpected data type for this action
    UnexpectedDataType,
    /// The server sent data, that is valid, however, for this specific query, it
    /// was unexpected. This indicates a bug in the server
    UnexpectedResponse,
    /// The server sent an unknown data type that we cannot parse
    UnknownDataType,
    /// The server sent an invalid response
    InvalidResponse,
    /// The server returned a response code **other than the one that should have been returned
    /// for this action** (if any)
    Code(RespCode),
}

#[derive(Debug)]
#[non_exhaustive]
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(eio) => write!(f, "{}", eio),
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
            Self::SslError(essl) => write!(f, "{}", essl),
            Self::ParseError => write!(f, "Skyhash parse error"),
            Self::SkyError(eproto) => match eproto {
                SkyhashError::Code(rcode) => write!(f, "{}", rcode),
                SkyhashError::InvalidResponse => {
                    write!(f, "Invalid Skyhash response received from server")
                }
                SkyhashError::UnexpectedResponse => {
                    write!(f, "Unexpected response from server")
                }
                SkyhashError::ParseError => write!(f, "Client-side datatype parse error"),
                SkyhashError::UnexpectedDataType => write!(f, "Wrong type sent by server"),
                SkyhashError::UnknownDataType => {
                    write!(f, "Server sent unknown data type for this client version")
                }
            },
        }
    }
}

cfg_ssl_any! {
    impl From<openssl::ssl::Error> for Error {
        fn from(err: openssl::ssl::Error) -> Self {
            Self::SslError(err)
        }
    }
    impl From<openssl::error::ErrorStack> for Error {
        fn from(e: openssl::error::ErrorStack) -> Self {
            let e: openssl::ssl::Error = e.into();
            Self::SslError(e)
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<SkyhashError> for Error {
    fn from(err: SkyhashError) -> Self {
        Self::SkyError(err)
    }
}
