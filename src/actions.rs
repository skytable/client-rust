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
//! let ret: String = con.get("x").unwrap();
//! assert_eq!(ret, "100");
//! ```
//!

use crate::error::{errorstring, SkyhashError};
use crate::types::Array;
use crate::types::FromSkyhashBytes;
use crate::types::SnapshotResult;
use crate::Element;
use crate::GetIterator;
use crate::IntoSkyhashAction;
use crate::IntoSkyhashBytes;
use crate::Query;
use crate::RespCode;
use crate::SkyQueryResult;
use crate::SkyResult;

cfg_async!(
    use crate::AsyncResult;
);

cfg_async!(
    #[doc(hidden)]
    /// A raw async connection to the database server
    pub trait AsyncSocket: Send + Sync {
        /// Run the query
        fn run(&mut self, q: Query) -> AsyncResult<SkyQueryResult>;
    }
    impl<T> AsyncActions for T where T: AsyncSocket {}
);

cfg_sync!(
    #[doc(hidden)]
    /// A raw synchronous connection to the database server
    pub trait SyncSocket {
        /// Run the query
        fn run(&mut self, q: Query) -> SkyQueryResult;
    }
    impl<T> Actions for T where T: SyncSocket {}
);

macro_rules! implement_actions {
    (
        $(
            $(#[$attr:meta])+
            fn $name:ident$(<$($tyargs:ident $(: $ty:ident $(+$tye:lifetime)?)?),*>)?(
                $($argname:ident: $argty:ty),*) -> $ret:ty {
                    $($block:block)?
                    $($($mtch:pat)|+ => $expect:expr),+
                }
        )*
    ) => {
        #[cfg(feature = "sync")]
        #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
        /// Actions that can be run on a [`SyncSocket`] connection
        ///
        /// ## Example
        /// ```no_run
        /// use skytable::actions::Actions;
        /// use skytable::sync::Connection;
        ///
        /// let mut con = Connection::new("127.0.0.1", 2003).unwrap();
        /// con.set("x", "100").unwrap();
        /// let x: String = con.get("x").unwrap();
        /// assert_eq!(x, "100");
        /// ```
        pub trait Actions: SyncSocket {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<'s, $($($tyargs$(: $ty $(+$tye)*,)* )*)?>(&'s mut self $(, $argname: $argty)*) -> SkyResult<$ret> {
                    gen_match!(self.run($($block)?), $($($mtch)+, $expect),*)
                }
            )*
        }
        #[cfg(feature = "aio")]
        #[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
        /// Actions that can be run on an [`AsyncSocket`] connection
        pub trait AsyncActions: AsyncSocket {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<'s, $($($tyargs$(: $ty $(+$tye)*,)* )*)?>(&'s mut self $(, $argname: $argty)*) -> AsyncResult<SkyResult<$ret>> {
                    Box::pin(async move {gen_match!(self.run($($block)?).await, $($($mtch)+, $expect),*)})
                }
            )*
        }
    };
}

implement_actions! {
    /// Get the number of keys present in the database
    fn dbsize() -> u64 {
        { Query::from("dbsize") }
        Element::UnsignedInt(int) => int
    }
    /// Deletes a single or a number of keys
    ///
    /// This is equivalent to:
    /// ```text
    /// DEL <k1> <k2> <k3> ...
    /// ```
    ///
    /// This will return the number of keys that were deleted
    ///
    fn del(key: impl IntoSkyhashAction + 's) -> u64 {
        { Query::from("del").arg(key) }
        Element::UnsignedInt(int) => int as u64
    }
    /// Checks if a key (or keys) exist(s)
    ///
    /// This is equivalent to:
    /// ```text
    /// EXISTS <k1> <k2> <k3> ...
    /// ```
    ///
    /// This will return the number of keys that do exist
    ///
    fn exists(key: impl IntoSkyhashAction + 's) -> u64 {
        { Query::from("exists").arg(key) }
        Element::UnsignedInt(int) => int as u64
    }
    /// Removes all the keys present in the database
    fn flushdb() -> () {
        { Query::from("flushdb") }
        Element::RespCode(RespCode::Okay) => {}
    }
    /// Get the value of a key
    ///
    /// This is equivalent to:
    /// ```text
    /// GET <key>
    /// ```
    fn get<T: FromSkyhashBytes>(key: impl IntoSkyhashBytes + 's) -> T {
        { Query::from("get").arg(key)}
        x @ Element::String(_) | x @ Element::Binstr(_) => T::from_element(x)?
    }
    /// Get the length of a key
    ///
    /// This is equivalent to:
    /// ```text
    /// KEYLEN <key>
    /// ```
    fn keylen(key: impl IntoSkyhashBytes + 's) -> u64 {
        { Query::from("keylen").arg(key)}
        Element::UnsignedInt(int) => int as u64
    }
    /// Returns a vector of keys
    ///
    /// This is equivalent to:
    /// ```text
    /// LSKEYS <count>
    /// ```
    ///
    /// Do note that the order might be completely meaningless
    fn lskeys<T: FromSkyhashBytes>(count: u64) -> T {
        { Query::from("lskeys").arg(count.to_string())}
        x @ Element::Array(Array::Bin(_)) | x @ Element::Array(Array::Str(_)) => T::from_element(x)?
    }
    /// Get multiple keys
    ///
    /// This is equivalent to:
    /// ```text
    /// MGET <k1> <k2> ...
    /// ```
    ///
    /// **This method expects either:** `[T; N]`, `&[T; N]` or anything that derefs to `&[T]`
    fn mget<T: FromSkyhashBytes>(keys: impl IntoSkyhashAction+ 's) -> T {
        { Query::from("mget").arg(keys)}
        x @ Element::Array(Array::Bin(_)) | x @ Element::Array(Array::Str(_)) => T::from_element(x)?
    }
    /// Creates a snapshot
    ///
    /// This returns a [`SnapshotResult`] containing the result. The reason [`SnapshotResult`] is not
    /// an error is because `mksnap` might fail simply because an existing snapshot process was in progress
    /// which is normal behavior and _not an inherent error_
    fn mksnap() -> SnapshotResult {
       { Query::from("mksnap")}
       Element::RespCode(RespCode::Okay) => SnapshotResult::Okay,
       Element::RespCode(RespCode::ErrorString(er)) => {
           match er.as_str() {
               errorstring::ERR_SNAPSHOT_BUSY => SnapshotResult::Busy,
               errorstring::ERR_SNAPSHOT_DISABLED => SnapshotResult::Disabled,
               _ => return Err(SkyhashError::InvalidResponse.into())
           }
       }
    }

    /// Sets the value of multiple keys and values and returns the number of keys that were set
    ///
    /// This is equivalent to:
    /// ```text
    /// MSET <k1> <v1> <k2> <v2> ...
    /// ```
    /// with the only difference that you have to pass in the keys and values as separate
    /// objects
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    ///
    /// **This method expects either:** `[T; N]`, `&[T; N]` or anything that derefs to `&[T]`
    fn mset<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> u64 {
        {
            assert!(keys.incr_len_by() == values.incr_len_by(), "The number of keys and values for mset must be equal");
            Query::from("mset")._push_alt_iter(keys, values)
        }
        Element::UnsignedInt(int) => int as u64
    }
    /// Updates the value of multiple keys and values and returns the number of keys that were updated
    ///
    /// This is equivalent to:
    /// ```text
    /// MUPDATE <k1> <v1> <k2> <v2> ...
    /// ```
    /// with the only difference that you have to pass in the keys and values as separate
    /// objects
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    ///
    /// **This method expects either:** `[T; N]`, `&[T; N]` or anything that derefs to `&[T]`
    fn mupdate<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> u64 {
        {
            assert!(keys.incr_len_by() == values.incr_len_by(), "The number of keys and values for mupdate must be equal");
            Query::from("mset")._push_alt_iter(keys, values)
        }
        Element::UnsignedInt(int) => int as u64
    }
    /// Consumes a key if it exists
    ///
    /// This will return the corresponding values of the provided key
    /// depending on the type for that table
    ///
    /// This is equivalent to:
    /// ```text
    /// POP <key>
    /// ```
    fn pop<T: FromSkyhashBytes>(keys: impl IntoSkyhashBytes + 's) -> T {
        { Query::from("POP").arg(keys) }
        x @ Element::String(_) | x @ Element::Binstr(_) => T::from_element(x)?
    }
    /// Consumes the provided keys if they exist
    ///
    /// This is equivalent to:
    /// ```text
    /// MPOP <k1> <k2> <k3>
    /// ```
    fn mpop<T: FromSkyhashBytes>(keys: impl IntoSkyhashAction + 's) -> T {
        { Query::from("mpop").arg(keys)}
        x @ Element::Array(Array::Bin(_)) | x @ Element::Array(Array::Str(_)) => T::from_element(x)?
    }
    /// Deletes all the provided keys if they exist or doesn't do anything at all. This method
    /// will return true if all the provided keys were deleted, else it will return false
    ///
    /// This is equivalent to:
    /// ```text
    /// SDEL <k1> <v1> <k2> <v2>
    /// ```
    /// with the only difference that you have to pass in the keys and values as separate
    /// objects
    fn sdel(keys: impl IntoSkyhashAction + 's) -> bool {
        { Query::from("sdel").arg(keys) }
        Element::RespCode(RespCode::Okay) => true,
        Element::RespCode(RespCode::NotFound) => false
    }
    /// Set the value of a key
    ///
    /// This is equivalent to:
    /// ```text
    /// SET <k> <v>
    /// ```
    ///
    fn set(key: impl IntoSkyhashBytes + 's, value: impl IntoSkyhashBytes + 's) -> bool {
        { Query::from("set").arg(key).arg(value) }
        Element::RespCode(RespCode::Okay) => true,
        Element::RespCode(RespCode::OverwriteError) => false
    }
    /// Sets the value of all the provided keys or does nothing. This method will return true if all the keys
    /// were set or will return false if none were set
    ///
    /// This is equivalent to:
    /// ```text
    /// SSET <k1> <v1> <k2> <v2> ...
    /// ```
    /// with the only difference that you have to pass in the keys and values as separate
    /// objects
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    ///
    /// **This method expects either:** `[T; N]`, `&[T; N]` or anything that derefs to `&[T]`
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
        Element::RespCode(RespCode::Okay) => true,
        Element::RespCode(RespCode::OverwriteError) => false
    }
    /// Updates the value of all the provided keys or does nothing. This method will return true if all the keys
    /// were updated or will return false if none were updated.
    ///
    /// This is equivalent to:
    /// ```text
    /// SUPDATE <key1> <value1> <key2> <value2> ...
    /// ```
    /// with the only difference that you have to pass in the keys and values as separate
    /// objects
    ///
    /// ## Panics
    /// This method will panic if the number of keys and values are not equal
    ///
    /// **This method expects either:** `[T; N]`, `&[T; N]` or anything that derefs to `&[T]`
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
        Element::RespCode(RespCode::Okay) => true,
        Element::RespCode(RespCode::NotFound) => false
    }
    /// Update the value of a key
    ///
    /// This is equivalent to:
    /// ```text
    /// UPDATE <key> <value>
    /// ```
    fn update(key: impl IntoSkyhashBytes + 's, value: impl IntoSkyhashBytes + 's) -> () {
        { Query::from("update").arg(key).arg(value) }
        Element::RespCode(RespCode::Okay) => {}
    }
    /// Updates or sets all the provided keys and returns the number of keys that were set
    ///
    /// This is equivalent to:
    /// ```text
    /// USET <key1> <value1> <key2> <value2> ...
    /// ```
    /// with the only difference that you have to pass in the keys and values as separate
    /// objects
    ///
    /// ## Panics
    /// This method will panic if the number of keys is not equal to the number of values
    ///
    /// **This method expects either:** `[T; N]`, `&[T; N]` or anything that derefs to `&[T]`
    fn uset<T: IntoSkyhashBytes + 's , U: IntoSkyhashBytes + 's>
    (
        keys: impl GetIterator<T> + 's,
        values: impl GetIterator<U> + 's
    ) -> u64 {
        {
            assert!(
                keys.incr_len_by() == values.incr_len_by(),
                "The number of keys and values for uset must be equal"
            );
            Query::from("uset")._push_alt_iter(keys, values)
        }
        Element::UnsignedInt(int) => int as u64
    }
}
