/*
 * Created on Tue May 25 2021
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

//! # Types
//!
//! This module contains a collection of enumerations and traits that are used for converting multiple
//! types, either primitve or user-defined, into Skyhash serializable items.
//!
//!
//! ## Implementing a Skyhash serializable type
//! If you have an object that can be turned into a [`String`] or a sequence of [`String`] objects, then
//! your type can be serialized by Skyhash (this might change in the future with more types being supported).
//! Here is a simple example:
//! ```
//! use skytable::actions::Actions;
//! use skytable::types::{IntoSkyhashAction, IntoSkyhashBytes, GetIterator};
//! use skytable::Query;
//!
//! /// Our custom element that adds "cool" to the end of every string when serialized
//! struct CoolString(String);
//!
//! impl IntoSkyhashBytes for CoolString {
//!     fn as_string(&self) -> String {
//!         let mut st = self.0.to_string();
//!         // add cool
//!         st.push_str("cool");
//!         st
//!     }
//! }
//!
//! /// Our custom sequence of `CoolString` objects
//! struct CoolStringCollection(Vec<CoolString>);
//!
//! impl IntoSkyhashAction for CoolStringCollection {
//!     fn push_into_query(&self, query: &mut Query) {
//!         self.0.iter().for_each(|item| query.push(item.as_string()));
//!     }
//!     fn incr_len_by(&self) -> usize {
//!         self.0.len()
//!     }
//! }
//!
//! // And finally implement `GetIterator` for use with some actions that need them
//!
//! impl GetIterator<CoolString> for CoolStringCollection {
//!     fn get_iter(&self) -> std::slice::Iter<'_, CoolString> {
//!         self.0.iter()
//!     }
//! }
//!
//! // You can now directly append your custom element to queries
//!
//! let mut q = Query::new();
//! let cool = CoolString(String::from("sayan is "));
//! q.push(cool);
//! let other_cools = CoolStringCollection(vec![
//!     CoolString("ferris is ".to_owned()),
//!     CoolString("llvm is ".to_owned())
//! ]);
//! q.push(other_cools);
//! assert_eq!(q, Query::from(vec!["sayan is cool", "ferris is cool", "llvm is cool"]));
//!
//! ```

use crate::Query;

/// Anything that implements this trait can be turned into a [`String`]. This trait is implemented
/// for most primitive types by default using [`std`]'s [`ToString`] trait.
///
/// ## Implementing this trait
///
/// A trivial example:
/// ```
/// use skytable::types::{IntoSkyhashBytes};
///
/// struct MyStringWrapper(String);
///
/// impl IntoSkyhashBytes for MyStringWrapper {
///     fn as_string(&self) -> String {
///         self.0.to_string()
///     }
/// }
/// ```
///
pub trait IntoSkyhashBytes: Send + Sync {
    /// Turn `Self` into a [`String`]
    fn as_string(&self) -> String;
}

macro_rules! impl_skyhash_bytes {
    ($($ty:ty),*) => {
        $(
            impl IntoSkyhashBytes for $ty {
                fn as_string(&self) -> String {
                    self.to_string()
                }
            }
        )*
    };
}

impl_skyhash_bytes!(
    u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, f32, f64, bool, char, usize, isize, String,
    &str, &String, str
);

/// Anything that implements this trait can directly add itself to the bytes part of a [`Query`] object
///
/// ## Implementing this trait
///
/// This trait does **not** need to be implemented manually for some basic objects. Once you implement
/// [`IntoSkyhashBytes`], it is implemented automatically. This trait will need to be implemented manually
/// for other objects however. For example:
/// ```
/// use skytable::types::IntoSkyhashAction;
/// use skytable::Query;
///
/// struct MyVecWrapper(Vec<String>);
///
/// impl IntoSkyhashAction for MyVecWrapper {
///     fn push_into_query(&self, q: &mut Query) {
///         self.0.iter().for_each(|element| q.push(element));
///     }
///     fn incr_len_by(&self) -> usize {
///         self.0.len()
///     }
/// }
/// ```
/// ### Unexpected behavior
///
/// As you can see, it is easy enough for someone to miss an element while implementing (maybe not the best example)
/// [`IntoSkyhashAction::push_into_query()`] and also misguide the client driver about the length in the
/// implementation for [`IntoSkyhashAction::incr_len_by()`]. This is why you should _stay on the guard_
/// while implementing it.
///
pub trait IntoSkyhashAction: Send + Sync {
    /// Extend the bytes of a `Vec` of bytes
    fn push_into_query(&self, data: &mut Query);
    /// Returns the number of items `Self` will add to the query
    fn incr_len_by(&self) -> usize;
}

impl<T> IntoSkyhashAction for T
where
    T: IntoSkyhashBytes,
{
    fn push_into_query(&self, q: &mut Query) {
        q._push_arg(self.as_string());
    }
    fn incr_len_by(&self) -> usize {
        1
    }
}

impl<T> IntoSkyhashAction for Vec<T>
where
    T: IntoSkyhashBytes,
{
    fn push_into_query(&self, mut data: &mut Query) {
        self.iter().for_each(|elem| elem.push_into_query(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        self.len()
    }
}

impl<T> IntoSkyhashAction for &Vec<T>
where
    T: IntoSkyhashBytes,
{
    fn push_into_query(&self, mut data: &mut Query) {
        self.iter().for_each(|elem| elem.push_into_query(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        self.len()
    }
}

impl<T> IntoSkyhashAction for &[T]
where
    T: IntoSkyhashBytes,
{
    fn push_into_query(&self, mut data: &mut Query) {
        self.iter().for_each(|elem| elem.push_into_query(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        self.len()
    }
}

#[cfg(feature = "const-gen")]
impl<T: IntoSkyhashBytes, const N: usize> IntoSkyhashAction for [T; N] {
    fn push_into_query(&self, mut data: &mut Query) {
        self.iter().for_each(|elem| elem.push_into_query(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        self.len()
    }
}

#[cfg(feature = "const-gen")]
impl<T: IntoSkyhashBytes, const N: usize> IntoSkyhashAction for &'static [T; N] {
    fn push_into_query(&self, mut data: &mut Query) {
        self.iter().for_each(|elem| elem.push_into_query(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        N
    }
}

/// Result of an `mksnap` action
#[non_exhaustive]
pub enum SnapshotResult {
    /// The snapshot was created successfully
    Okay,
    /// Periodic snapshots are disabled on the server side
    Disabled,
    /// A snapshot is already in progress
    Busy,
}

/// Implement this trait for methods in [`actions`](crate::actions) that need them. See the
/// [module level documentation](crate::types) for more information
pub trait GetIterator<T: IntoSkyhashBytes>: IntoSkyhashAction {
    fn get_iter(&self) -> std::slice::Iter<T>;
}

#[cfg(feature = "const-gen")]
impl<T: IntoSkyhashBytes, const N: usize> GetIterator<T> for [T; N] {
    fn get_iter(&self) -> std::slice::Iter<'_, T> {
        self.iter()
    }
}

#[cfg(feature = "const-gen")]
impl<T: IntoSkyhashBytes, const N: usize> GetIterator<T> for &'static [T; N] {
    fn get_iter(&self) -> std::slice::Iter<'_, T> {
        self.iter()
    }
}

impl<T: IntoSkyhashBytes> GetIterator<T> for &[T] {
    fn get_iter(&self) -> std::slice::Iter<'_, T> {
        self.iter()
    }
}

impl<T: IntoSkyhashBytes> GetIterator<T> for Vec<T> {
    fn get_iter(&self) -> std::slice::Iter<'_, T> {
        self.iter()
    }
}

impl<T: IntoSkyhashBytes> GetIterator<T> for &Vec<T> {
    fn get_iter(&self) -> std::slice::Iter<'_, T> {
        self.iter()
    }
}

/// Array types
pub enum Array {
    Bin(Vec<Option<Vec<u8>>>),
    Str(Vec<Option<String>>),
}

/// String types
pub enum Str {
    Unicode(String),
    Binary(Vec<u8>),
}
