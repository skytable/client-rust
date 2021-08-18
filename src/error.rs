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
use std::io::ErrorKind;
cfg_ssl_any!(
    use std::fmt;
    /// Errors that may occur while initiating an [async TLS connection](crate::aio::TlsConnection)
    /// or a [sync TLS connection](crate::sync::TlsConnection)
    #[derive(Debug)]
    pub enum SslError {
        /// An [I/O Error](std::io::Error) occurred
        IoError(std::io::Error),
        /// An [SSL Error](openssl::error::Error) occurred
        SslError(openssl::ssl::Error),
    }

    impl From<openssl::ssl::Error> for SslError {
        fn from(e: openssl::ssl::Error) -> Self {
            Self::SslError(e)
        }
    }

    impl From<std::io::Error> for SslError {
        fn from(e: std::io::Error) -> Self {
            Self::IoError(e)
        }
    }

    impl From<openssl::error::ErrorStack> for SslError {
        fn from(e: openssl::error::ErrorStack) -> Self {
            Self::SslError(e.into())
        }
    }

    impl fmt::Display for SslError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
            match self {
                Self::IoError(e) => write!(f, "{}", e),
                Self::SslError(e) => write!(f, "{}", e),
            }
        }
    }
);
#[derive(Debug)]
#[non_exhaustive]
/// An error originating from the Skyhash protocol
pub enum SkyhashError {
    /// The server sent data but we failed to parse it
    ParseError,
    /// The server sent an unexpected data type for this action
    UnexpectedDataType,
    /// The server sent an unknown data type that we cannot parse
    UnknownDataType,
    /// The server sent an invalid response
    InvalidResponse,
    /// An I/O error occurred while running this action
    IoError(ErrorKind),
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

cfg_ssl_any! {
    impl From<openssl::ssl::Error> for Error {
        fn from(err: openssl::ssl::Error) -> Self {
            Self::SslError(err)
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

cfg_ssl_any! {
    impl From<SslError> for Error {
        fn from(err: SslError) -> Self {
            match err {
                SslError::IoError(ioerr) => Self::IoError(ioerr),
                SslError::SslError(sslerr) => Self::SslError(sslerr),
            }
        }
    }
}

impl From<SkyhashError> for Error {
    fn from(err: SkyhashError) -> Self {
        Self::SkyError(err)
    }
}
