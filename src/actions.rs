/*
 * Created on Mon May 24 2021
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

//! Actions
//!
//! This module contains macros and other methods for running actions (and generating the code for them)

use crate::RespCode;
#[cfg(feature = "async")]
use core::{future::Future, pin::Pin};
use std::io::ErrorKind;

/// Errors while running actions
#[derive(Debug)]
pub enum ActionError {
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

type ActionResultInner<T> = Result<T, ActionError>;
#[cfg(feature = "async")]
/// A special result that is returned when running actions
pub type ActionResult<'s, T> =
    Pin<Box<dyn Future<Output = ActionResultInner<T>> + Send + Sync + 's>>;
#[cfg(feature = "sync")]
/// A special result that is returned when running actions
pub type ActionResult<T> = ActionResultInner<T>;

macro_rules! gen_match {
    ($queryret:expr, $mtch:pat, $ret:expr) => {
        match $queryret {
            Ok($mtch) => $ret,
            Ok(Response::InvalidResponse) => Err(ActionError::InvalidResponse),
            Ok(Response::ParseError) => Err(ActionError::ParseError),
            Ok(Response::UnsupportedDataType) => Err(ActionError::UnknownDataType),
            Ok(Response::Item(Element::RespCode(code))) => Err(ActionError::Code(code)),
            Ok(_) => Err(ActionError::UnexpectedDataType),
            Err(e) => Err(ActionError::IoError(e.kind())),
        };
    };
}

macro_rules! gen_return {
    ($con:expr, $query:ident, $mtch:pat => $ret:expr) => {
        #[cfg(feature = "async")]
        return Box::pin(
            async move { gen_match!($con.run_simple_query(&$query).await, $mtch, $ret) },
        );
        #[cfg(feature = "sync")]
        return gen_match!($con.run_simple_query(&$query), $mtch, $ret);
    };
}

macro_rules! impl_actions {
    ($contype:ty) => {
        use crate::{Element, Response};
        type _Result = Result<Response, std::io::Error>;
        impl $contype {
            /// Get a `key`
            pub fn get<'s>(&'s mut self, key: impl ToString) -> ActionResult<String> {
                let q = crate::Query::new("get").arg(key.to_string());
                gen_return!(self, q, Response::Item(Element::String(st)) => Ok(st));
            }
            /// Set a `key` to a `value`
            pub fn set<'s>(
                &'s mut self,
                key: impl ToString,
                value: impl ToString,
            ) -> ActionResult<()> {
                let q = crate::Query::new("set")
                    .arg(key.to_string())
                    .arg(value.to_string());
                gen_return!(self, q, Response::Item(Element::RespCode(RespCode::Okay)) => Ok(()));
            }
        }
    };
}

#[cfg(all(feature = "sync", not(feature = "async")))]
impl_actions!(crate::Connection);
#[cfg(all(feature = "async", not(feature = "sync")))]
impl_actions!(crate::AsyncConnection);
