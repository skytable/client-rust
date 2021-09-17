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

#[derive(Debug, PartialEq)]
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

pub mod errorstring {
    //! # Error strings
    //!
    //! This module contains a collection of constants that represent [error strings](https://docs.skytable.io/protocol/errors)
    //! returned by the server
    //!
    /// The default container was not set
    pub const DEFAULT_CONTAINER_UNSET: &str = "default-container-unset";
    /// The container was not found
    pub const CONTAINER_NOT_FOUND: &str = "container-not-found";
    /// The container is still in use
    pub const STILL_IN_USE: &str = "still-in-use";
    /// The object is a protected object and is not user accessible
    pub const ERR_PROTECTED_OBJECT: &str = "err-protected-object";
    /// The container already exists
    pub const ERR_ALREADY_EXISTS: &str = "err-already-exists";
    /// The container is not ready
    pub const ERR_NOT_READY: &str = "not-ready";
    /// The error string returned when the snapshot engine is busy
    pub const ERR_SNAPSHOT_BUSY: &str = "err-snapshot-busy";
    /// The error string returned when periodic snapshots are busy
    pub const ERR_SNAPSHOT_DISABLED: &str = "err-snapshot-disabled";
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
    ParseError(String),
}

impl PartialEq for Error {
    fn eq(&self, oth: &Error) -> bool {
        use Error::*;
        match (self, oth) {
            (IoError(a), IoError(b)) => a.kind().eq(&b.kind()),
            (SkyError(a), SkyError(b)) => a == b,
            (ParseError(a), ParseError(b)) => a == b,
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
            (SslError(a), SslError(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
    }
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
            Self::ParseError(apperr) => {
                write!(f, "custom type parse error: {}", apperr)
            }
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

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Self::ParseError(e.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(e: std::num::ParseFloatError) -> Self {
        Self::ParseError(e.to_string())
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(e: std::num::TryFromIntError) -> Self {
        Self::ParseError(e.to_string())
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        unsafe { core::hint::unreachable_unchecked() }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::ParseError(e.to_string())
    }
}
