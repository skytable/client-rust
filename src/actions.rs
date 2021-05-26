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

//! # Actions
//!
//! This module contains macros and other methods for running actions. To run actions:
//! - For the `sync` feature, add this import:
//!     ```
//!     use skytable::actions::Actions;
//!     ```
//! - For the `async` feature, add this import:
//!     ```
//!     use skytable::actions::AsyncActions;
//!     ```
//! ## Running actions
//!
//! Once you have imported the required traits, you can now run the actions! For example:
//! ```no_run
//! use skytable::{actions::Actions, Connection};
//! fn main() {
//!     let mut con = Connection::new("127.0.0.1", 2003).unwrap();
//!     con.set("x", "100").unwrap();
//!     assert_eq!(con.get("x").unwrap(), "100".to_owned());
//! }
//! ```

use crate::types::SnapshotResult;
use crate::Element;
use crate::IntoSkyhashAction;
use crate::IntoSkyhashBytes;
use crate::Query;
use crate::RespCode;
use crate::Response;
#[cfg(feature = "async")]
use core::{future::Future, pin::Pin};
use std::io::ErrorKind;

pub const ERR_SNAPSHOT_BUSY: &str = "err-snapshot-busy";
pub const ERR_SNAPSHOT_DISABLED: &str = "err-snapshot-disabled";

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

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
/// A special result that is returned when running actions (async)
pub type AsyncResult<'s, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 's>>;
/// A special result that is returned when running actions
pub type ActionResult<T> = Result<T, ActionError>;

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
#[doc(hidden)]
pub trait SyncSocket {
    fn run(&mut self, q: Query) -> std::io::Result<Response>;
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
#[doc(hidden)]
pub trait AsyncSocket: Send + Sync {
    fn run(&mut self, q: Query) -> AsyncResult<std::io::Result<Response>>;
}

macro_rules! gen_match {
    ($ret:expr, $($($mtch:pat)+ $(if $exp:expr)*, $expect:expr),*) => {
        match $ret {
            $($(Ok($mtch))|* $(if $exp:expr)* => Ok($expect),)*
            Ok(Response::InvalidResponse) => Err(ActionError::InvalidResponse),
            Ok(Response::ParseError) => Err(ActionError::ParseError),
            Ok(Response::UnsupportedDataType) => Err(ActionError::UnknownDataType),
            Ok(Response::Item(Element::RespCode(code))) => Err(ActionError::Code(code)),
            Ok(Response::Item(_)) => Err(ActionError::UnexpectedDataType),
            Err(e) => Err(ActionError::IoError(e.kind())),
        }
    };
}

macro_rules! implement_actions {
    (
        $(
            $(#[$attr:meta])+
            fn $name:ident(
                $($argname:ident: $argty:ty),*) -> $ret:ty {
                    $($block:block)*
                    $($($mtch:pat)|+ => $expect:expr),+
                }
        )*
    ) => {
        #[cfg(feature = "sync")]
        #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
        /// Actions that can be run on a [`SyncSocket`] connection
        pub trait Actions: SyncSocket {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<'s>(&'s mut self $(, $argname: $argty)*) -> ActionResult<$ret> {
                    $($block)*
                    let q = crate::Query::new(stringify!($name))$(.arg($argname))*;
                    gen_match!(self.run(q), $($($mtch)+, $expect),*)
                }
            )*
        }
        #[cfg(feature = "async")]
        #[cfg_attr(docsrs, doc(cfg(feature = "async")))]
        /// Actions that can be run on an [`AsyncSocket`] connection
        pub trait AsyncActions: AsyncSocket {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<'s>(&'s mut self $(, $argname: $argty)*) -> AsyncResult<ActionResult<$ret>> {
                    let q = crate::Query::new(stringify!($name))$(.arg($argname))*;
                    Box::pin(async move {
                        gen_match!(self.run(q).await, $($($mtch)+, $expect),*)
                    })
                }
            )*
        }
    };
}

#[cfg(feature = "sync")]
impl<T> Actions for T where T: SyncSocket {}
#[cfg(feature = "async")]
impl<T> AsyncActions for T where T: AsyncSocket {}

implement_actions!(
    /// Get the number of keys present in the database
    fn dbsize() -> usize {
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Deletes a single or a number of keys
    ///
    /// This will return the number of keys that were deleted
    fn del(key: impl IntoSkyhashAction) -> usize {
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Checks if a key (or keys) exist(s)
    ///
    /// This will return the number of keys that do exist
    fn exists(key: impl IntoSkyhashAction) -> usize {
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Removes all the keys present in the database
    fn flushdb() -> () {
        Response::Item(Element::RespCode(RespCode::Okay)) => {}
    }
    /// Get the value of a key
    fn get(key: impl IntoSkyhashBytes) -> String {
        Response::Item(Element::String(st)) => st
    }
    /// Get the length of a key
    fn keylen(key: impl IntoSkyhashBytes) -> usize {
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Returns a vector of keys
    ///
    /// Do note that the order might be completely meaningless
    fn lskeys(count: usize) -> Vec<String> {
        Response::Item(Element::FlatArray(arr)) => arr
    }
    /// Get multiple keys
    ///
    /// This returns a vector of [`Element`]s which either contain the values
    /// as strings or contains `Not Found (Code: 1)` response codes
    fn mget(keys: impl IntoSkyhashAction) -> Vec<Element> {
        Response::Item(Element::Array(array)) => array
    }
    /// Creates a snapshot
    ///
    /// This returns a [`SnapshotResult`] containing the result. The reason [`SnapshotResult`] is not
    /// an error is because `mksnap` might fail simply because an existing snapshot process was in progress
    /// which is normal behavior and _not an inherent error_
    fn mksnap() -> SnapshotResult {
       Response::Item(Element::RespCode(RespCode::Okay)) => SnapshotResult::Okay,
       Response::Item(Element::RespCode(RespCode::ErrorString(er))) => {
           match er.as_str() {
               ERR_SNAPSHOT_BUSY => SnapshotResult::Busy,
               ERR_SNAPSHOT_DISABLED => SnapshotResult::Disabled,
               _ => return Err(ActionError::InvalidResponse)
           }
       }
    }
    /// Sets the value of multiple keys and values and returns the number of keys that were set
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    fn mset(keys: impl IntoSkyhashAction, values: impl IntoSkyhashAction) -> usize {
        {
            assert!(keys.incr_len_by() == values.incr_len_by(), "The number of keys and values for mset must be equal");
        }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Updates the value of multiple keys and values and returns the number of keys that were updated
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    fn mupdate(keys: impl IntoSkyhashAction, values: impl IntoSkyhashAction) -> usize {
        {
            assert!(keys.incr_len_by() == values.incr_len_by(), "The number of keys and values for mupdate must be equal");
        }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Deletes all the provided keys if they exist or doesn't do anything at all. This method
    /// will return true if all the provided keys were deleted, else it will return false
    fn sdel(keys: impl IntoSkyhashAction) -> bool {
        Response::Item(Element::RespCode(RespCode::Okay)) => true,
        Response::Item(Element::RespCode(RespCode::NotFound)) => false
    }
    /// Set the value of a key
    fn set(key: impl IntoSkyhashBytes, value: impl IntoSkyhashBytes) -> () {
        Response::Item(Element::RespCode(RespCode::Okay)) => {}
    }
    /// Sets the value of all the provided keys or does nothing. This method will return true if all the keys
    /// were set or will return false if none were set
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    fn sset(keys: impl IntoSkyhashAction, values: impl IntoSkyhashAction) -> bool {
        {
            assert!(
                keys.incr_len_by() == values.incr_len_by(),
                "The number of keys and values for sset must be equal"
            );
        }
        Response::Item(Element::RespCode(RespCode::Okay)) => true,
        Response::Item(Element::RespCode(RespCode::OverwriteError)) => false
    }
    /// Updates the value of all the provided keys or does nothing. This method will return true if all the keys
    /// were updated or will return false if none were updated
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    fn supdate(keys: impl IntoSkyhashAction, values: impl IntoSkyhashAction) -> bool {
        {
            assert!(
                keys.incr_len_by() == values.incr_len_by(),
                "The number of keys and values for supdate must be equal"
            );
        }
        Response::Item(Element::RespCode(RespCode::Okay)) => true,
        Response::Item(Element::RespCode(RespCode::NotFound)) => false
    }
    /// Update the value of a key
    fn update(key: impl IntoSkyhashBytes, value: impl IntoSkyhashBytes) -> () {
        Response::Item(Element::RespCode(RespCode::Okay)) => {}
    }
    /// Updates or sets all the provided keys and returns the number of keys that were set
    ///
    /// ## Panics
    /// This method will panic if the number of keys is not equal to the number of values
    fn uset(keys: impl IntoSkyhashAction, values: impl IntoSkyhashAction) -> usize {
        {
            assert!(
                keys.incr_len_by() == values.incr_len_by(),
                "The number of keys and values for uset must be equal"
            );
        }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
);
