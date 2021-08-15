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
//! This module contains traits for running actions. To run actions:
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
//! let mut con = Connection::new("127.0.0.1", 2003).unwrap();
//! con.set("x", "100").unwrap();
//! assert_eq!(con.get("x").unwrap(), "100");
//!
//! ```

use crate::deserializer::FlatElement;
use crate::types::Array;
use crate::types::SnapshotResult;
use crate::types::Str;
use crate::Element;
use crate::GetIterator;
use crate::IntoSkyhashAction;
use crate::IntoSkyhashBytes;
use crate::Query;
use crate::RespCode;
use crate::Response;

cfg_async!(
    use core::{future::Future, pin::Pin};
);
use std::io::ErrorKind;

/// The error string returned when the snapshot engine is busy
pub const ERR_SNAPSHOT_BUSY: &str = "err-snapshot-busy";
/// The error string returned when periodic snapshots are busy
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

cfg_async!(
    /// A special result that is returned when running actions (async)
    pub type AsyncResult<'s, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 's>>;
    #[doc(hidden)]
    pub trait AsyncSocket: Send + Sync {
        fn run(&mut self, q: Query) -> AsyncResult<std::io::Result<Response>>;
    }
    impl<T> AsyncActions for T where T: AsyncSocket {}
);

/// A special result that is returned when running actions
pub type ActionResult<T> = Result<T, ActionError>;

cfg_sync!(
    #[doc(hidden)]
    pub trait SyncSocket {
        fn run(&mut self, q: Query) -> std::io::Result<Response>;
    }
    impl<T> Actions for T where T: SyncSocket {}
);

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
            fn $name:ident$(<$($tyargs:ident : $ty:ident $(+$tye:lifetime)*),*>)?(
                $($argname:ident: $argty:ty),*) -> $ret:ty {
                    $($block:block)?
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
                fn $name<'s, $($($tyargs: $ty $(+$tye)*, )*)?>(&'s mut self $(, $argname: $argty)*) -> ActionResult<$ret> {
                    gen_match!(self.run($($block)?), $($($mtch)+, $expect),*)
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
                fn $name<'s, $($($tyargs: $ty $(+$tye)*, )*)?>(&'s mut self $(, $argname: $argty)*) -> AsyncResult<ActionResult<$ret>> {
                    Box::pin(async move {gen_match!(self.run($($block)?).await, $($($mtch)+, $expect),*)})
                }
            )*
        }
    };
}

implement_actions!(
    /// Get the number of keys present in the database
    fn dbsize() -> usize {
        { Query::from("dbsize") }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Deletes a single or a number of keys
    ///
    /// This will return the number of keys that were deleted
    fn del(key: impl IntoSkyhashAction + 's) -> usize {
        { Query::from("del").arg(key) }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Checks if a key (or keys) exist(s)
    ///
    /// This will return the number of keys that do exist
    fn exists(key: impl IntoSkyhashAction + 's) -> usize {
        { Query::from("exists").arg(key) }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Removes all the keys present in the database
    fn flushdb() -> () {
        { Query::from("flushdb") }
        Response::Item(Element::RespCode(RespCode::Okay)) => {}
    }
    /// Get the value of a key
    fn get(key: impl IntoSkyhashBytes + 's) -> String {
        { Query::from("get").arg(key)}
        Response::Item(Element::Str(st)) => st
    }
    /// Get the length of a key
    fn keylen(key: impl IntoSkyhashBytes + 's) -> usize {
        { Query::from("keylen").arg(key)}
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Returns a vector of keys
    ///
    /// Do note that the order might be completely meaningless
    fn lskeys(count: usize) -> Vec<FlatElement> {
        { Query::from("lskeys").arg(count)}
        Response::Item(Element::FlatArray(arr)) => arr
    }
    /// Get multiple keys
    ///
    /// This returns a vector of [`Element`]s which either contain the values
    /// as strings or contains `Not Found (Code: 1)` response codes
    fn mget(keys: impl IntoSkyhashAction+ 's) -> Array {
        { Query::from("mget").arg(keys)}
        Response::Item(Element::BinArray(brr)) => Array::Bin(brr),
        Response::Item(Element::StrArray(srr)) => Array::Str(srr)
    }
    /// Creates a snapshot
    ///
    /// This returns a [`SnapshotResult`] containing the result. The reason [`SnapshotResult`] is not
    /// an error is because `mksnap` might fail simply because an existing snapshot process was in progress
    /// which is normal behavior and _not an inherent error_
    fn mksnap() -> SnapshotResult {
       { Query::from("mksnap")}
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
    fn mset<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> usize {
        {
            assert!(keys.incr_len_by() == values.incr_len_by(), "The number of keys and values for mset must be equal");
            Query::from("mset")._push_alt_iter(keys, values)
        }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Updates the value of multiple keys and values and returns the number of keys that were updated
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    fn mupdate<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> usize {
        {
            assert!(keys.incr_len_by() == values.incr_len_by(), "The number of keys and values for mupdate must be equal");
            Query::from("mset")._push_alt_iter(keys, values)
        }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
    /// Consumes a key if it exists
    ///
    /// This should return either the corresponding values of the provided keys or `Not Found`
    /// error codes
    fn pop(keys: impl IntoSkyhashBytes + 's) -> Str {
        { Query::from("POP").arg(keys) }
        Response::Item(Element::Str(st)) => Str::Unicode(st),
        Response::Item(Element::Binstr(bstr)) => Str::Binary(bstr)
    }
    /// Consumes the provided keys if they exist
    fn mpop(keys: impl IntoSkyhashAction + 's) -> Array {
        { Query::from("mpop").arg(keys)}
        Response::Item(Element::BinArray(brr)) => Array::Bin(brr),
        Response::Item(Element::StrArray(srr)) => Array::Str(srr)
    }
    /// Deletes all the provided keys if they exist or doesn't do anything at all. This method
    /// will return true if all the provided keys were deleted, else it will return false
    fn sdel(keys: impl IntoSkyhashAction + 's) -> bool {
        { Query::from("sdel").arg(keys) }
        Response::Item(Element::RespCode(RespCode::Okay)) => true,
        Response::Item(Element::RespCode(RespCode::NotFound)) => false
    }
    /// Set the value of a key
    fn set(key: impl IntoSkyhashBytes + 's, value: impl IntoSkyhashBytes + 's) -> () {
        { Query::from("set").arg(key).arg(value) }
        Response::Item(Element::RespCode(RespCode::Okay)) => {}
    }
    /// Sets the value of all the provided keys or does nothing. This method will return true if all the keys
    /// were set or will return false if none were set
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    fn sset<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> bool {
        {
            assert!(
                keys.incr_len_by() == values.incr_len_by(),
                "The number of keys and values for sset must be equal"
            );
            Query::from("sset")._push_alt_iter(keys, values)
        }
        Response::Item(Element::RespCode(RespCode::Okay)) => true,
        Response::Item(Element::RespCode(RespCode::OverwriteError)) => false
    }
    /// Updates the value of all the provided keys or does nothing. This method will return true if all the keys
    /// were updated or will return false if none were updated
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    fn supdate<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> bool {
        {
            assert!(
                keys.incr_len_by() == values.incr_len_by(),
                "The number of keys and values for supdate must be equal"
            );
            Query::from("supdate")._push_alt_iter(keys, values)
        }
        Response::Item(Element::RespCode(RespCode::Okay)) => true,
        Response::Item(Element::RespCode(RespCode::NotFound)) => false
    }
    /// Update the value of a key
    fn update(key: impl IntoSkyhashBytes + 's, value: impl IntoSkyhashBytes + 's) -> () {
        { Query::from("update").arg(key).arg(value) }
        Response::Item(Element::RespCode(RespCode::Okay)) => {}
    }
    /// Updates or sets all the provided keys and returns the number of keys that were set
    ///
    /// ## Panics
    /// This method will panic if the number of keys is not equal to the number of values
    fn uset<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> usize {
        {
            assert!(
                keys.incr_len_by() == values.incr_len_by(),
                "The number of keys and values for uset must be equal"
            );
            Query::from("uset")._push_alt_iter(keys, values)
        }
        Response::Item(Element::UnsignedInt(int)) => int as usize
    }
);
