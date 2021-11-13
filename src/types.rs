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
//! If you have an object that can be turned into a [`Vec<u8>`] or a sequence of [`Vec<u8>`] objects, then
//! your type can be serialized by Skyhash (this might change in the future with more types being supported).
//!
//! Here is a simple example:
//! ```
//! use skytable::actions::Actions;
//! use skytable::types::{IntoSkyhashAction, IntoSkyhashBytes, GetIterator, RawString};
//! use skytable::Query;
//!
//! /// Our custom element that adds "cool" to the end of every string when serialized
//! struct CoolString(String);
//!
//! impl IntoSkyhashBytes for CoolString {
//!     fn as_bytes(&self) -> Vec<u8> {
//!         let mut st = self.0.to_string();
//!         // add cool
//!         st.push_str("cool");
//!         st.into_bytes()
//!     }
//! }
//!
//! /// Our custom sequence of `CoolString` objects
//! struct CoolStringCollection(Vec<CoolString>);
//!
//! impl IntoSkyhashAction for CoolStringCollection {
//!     fn push_into_query(&self, query: &mut Query) {
//!         self.0.iter().for_each(|item| {
//!             query.push(RawString::from(item.as_bytes()))
//! });
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
//!
//! ## Implementing Skyhash deserializable types
//!
//! See the guide [in `FromSkyhashBytes`](FromSkyhashBytes) to see how you can implement
//! Skyhash deserializable types.
//!

use crate::error::Error;
use crate::Element;
use crate::Query;
use crate::RespCode;
use crate::SkyRawResult;
use core::convert::TryInto;
use core::ops::Deref;
use core::ops::DerefMut;

const BAD_ELEMENT: &str = "Bad element type for parsing into custom type";
const HAS_NULL_ELEMENTS: &str = "Array has null elements";

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
///     fn as_bytes(&self) -> Vec<u8> {
///         self.0.as_bytes().to_owned()
///     }
/// }
/// ```
///
pub trait IntoSkyhashBytes: Send + Sync {
    /// Return the byte representation of Self
    fn as_bytes(&self) -> Vec<u8>;
}

macro_rules! impl_skyhash_bytes {
    ($($ty:ty),*) => {
        $(
            impl IntoSkyhashBytes for $ty {
                fn as_bytes(&self) -> Vec<u8> {
                    self.to_string().into_bytes()
                }
            }
        )*
    };
}

impl_skyhash_bytes!(String, &str, &String, str);

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
        q._push_arg(self.as_bytes());
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
    /// Returns an iterator for self
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

impl<'a, T: IntoSkyhashBytes> GetIterator<T> for &'a [T] {
    fn get_iter(&self) -> std::slice::Iter<'_, T> {
        self.iter()
    }
}

impl<T: IntoSkyhashBytes> GetIterator<T> for Vec<T> {
    fn get_iter(&self) -> std::slice::Iter<'_, T> {
        self.iter()
    }
}

/// Array types
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum Array {
    /// A binary array with nullable elements(typed array tsymbol `?`, `@` base tsymbol)
    Bin(Vec<Option<Vec<u8>>>),
    /// A non-null binary array
    NonNullBin(Vec<Vec<u8>>),
    /// An unicode string array with nullable elements (typed array tsymbol `+`, `@` base tsymbol)
    Str(Vec<Option<String>>),
    /// A non-null string array
    NonNullStr(Vec<String>),
    /// A non-recursive 'flat' array (tsymbol `_`)
    Flat(Vec<FlatElement>),
    /// A recursive array (tsymbol `&`)
    Recursive(Vec<Element>),
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
/// A _flat_ element. This corresponds to the types that can be present
/// in a flat array as defined by the Skyhash protocol
pub enum FlatElement {
    /// An unicode string
    String(String),
    /// A binary string (blob)
    Binstr(Vec<u8>),
    /// A response code
    RespCode(RespCode),
    /// An unsigned integer
    UnsignedInt(u64),
}

/// A raw string
///
/// Use this type when you need to directly send raw data (i.e a byte sequence) instead of converting
/// each element into a Skyhash binary string.
/// This type allows you to send already assembled binary data like this:
/// ```
/// use skytable::query;
/// use skytable::types::RawString;
///
/// let mut mybin = RawString::from(vec![1, 2, 3, 4]);
/// let x = query!("SET", "mybindata", mybin);
///
/// ```
/// You can also use the RawString as a standard `Vec<u8>`:
/// ```
/// use skytable::types::RawString;
/// let mut mybin = RawString::new();
/// mybin.push(1);
/// mybin.push(2);
/// mybin.push(3);
///
/// assert_eq!(mybin, vec![1, 2, 3]);
/// ```
#[derive(Debug, PartialEq)]
pub struct RawString(Vec<u8>);

impl Default for RawString {
    fn default() -> Self {
        Self::new()
    }
}

impl RawString {
    /// Create a new [`RawString`] with the provided capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }
    /// Create a new [`RawString`] with the default capacity
    pub fn new() -> Self {
        Self(Vec::new())
    }
}

impl Deref for RawString {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RawString {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }
}

impl From<Vec<u8>> for RawString {
    fn from(oth: Vec<u8>) -> Self {
        Self(oth)
    }
}

impl PartialEq<Vec<u8>> for RawString {
    fn eq(&self, oth: &Vec<u8>) -> bool {
        self.0.eq(oth)
    }
}

impl Eq for RawString {}

impl IntoSkyhashBytes for RawString {
    fn as_bytes(&self) -> Vec<u8> {
        self.0.to_owned()
    }
}

impl<'a> IntoSkyhashBytes for &'a RawString {
    fn as_bytes(&self) -> Vec<u8> {
        self.0.to_owned()
    }
}

/// Implementing this trait enables Skyhash [elements](crate::Element) to be converted
/// into Rust types. This trait is already implemented for most primitive types, but for
/// your own custom types, you'll need to implement it yourself.
///
/// ## Example implementation
///
/// Say we have a key/value table that stores `(str, str)`. The value however contains comma
/// separated values (CSV). Let's say it looks like: `(Name, E-mail, State)` with types
/// `String`, `String` and `String` respectively. We'd represent our Rust struct and implement
/// this trait like this:
/// ```no_run
/// use skytable::types::FromSkyhashBytes;
/// use skytable::error::Error;
/// use skytable::{SkyRawResult, Element};
///
/// pub struct MyCSV {
///     name: String,
///     email: String,
///     state: String,
/// }
///
/// // Implement it
/// impl FromSkyhashBytes for MyCSV {
///     fn from_element(element: Element) -> SkyRawResult<Self> {
///         if let Element::String(st) = element {
///             let splits: Vec<&str> = st.split(",").collect();
///             Ok(
///                 MyCSV {
///                     name: splits[0].to_string(),
///                     email: splits[1].to_string(),
///                     state: splits[2].to_string(),
///                 }
///             )
///         } else {
///             Err(Error::ParseError("Bad element type".to_string()))
///         }
///     }
/// }
///
/// // Now use it with actions!
/// use skytable::sync::Connection;
/// use skytable::actions::Actions;
///
/// let mut con = Connection::new("127.0.0.1", 2003).unwrap();
/// let mycsv: MyCSV = con.get("mycsv").unwrap();
/// ```
///
/// Now, you can use this as you like to turn [`Element`]s into your own (or primitive) types or
/// with actions (as shown above)!
///
pub trait FromSkyhashBytes: Sized {
    /// Attempt to convert an element to the target type, returning errors if they occur
    fn from_element(element: Element) -> SkyRawResult<Self>;
}

macro_rules! impl_from_skyhash {
    ($($ty:ty),* $(,)?) => {
        $(impl FromSkyhashBytes for $ty {
            fn from_element(element: Element) -> SkyRawResult<$ty> {
                let ret = match element {
                    Element::Binstr(bstr) => String::from_utf8_lossy(&bstr).parse::<$ty>()?,
                    Element::String(st) => st.parse::<$ty>()?,
                    Element::UnsignedInt(int) => int.try_into()?,
                    _ => return Err(Error::ParseError(BAD_ELEMENT.to_owned())),
                };
                Ok(ret)
            }
        })*
    };
}

impl_from_skyhash!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, usize, isize);

impl FromSkyhashBytes for String {
    fn from_element(element: Element) -> SkyRawResult<String> {
        let e = match element {
            Element::Binstr(bstr) => std::string::String::from_utf8(bstr)?,
            Element::String(st) => st,
            Element::UnsignedInt(int) => int.to_string(),
            _ => return Err(Error::ParseError(BAD_ELEMENT.to_owned())),
        };
        Ok(e)
    }
}

impl FromSkyhashBytes for Vec<String> {
    fn from_element(element: Element) -> SkyRawResult<Self> {
        let e = match element {
            Element::Array(Array::Bin(binarr)) => {
                let mut new_arr = Vec::with_capacity(binarr.len());
                for item in binarr {
                    if let Some(item) = item {
                        new_arr.push(String::from_utf8(item)?);
                    } else {
                        return Err(Error::ParseError(HAS_NULL_ELEMENTS.to_owned()));
                    }
                }
                new_arr
            }
            Element::Array(Array::Str(strarr)) => {
                let mut new_arr = Vec::with_capacity(strarr.len());
                for item in strarr {
                    if let Some(item) = item {
                        new_arr.push(item);
                    } else {
                        return Err(Error::ParseError(HAS_NULL_ELEMENTS.to_owned()));
                    }
                }
                new_arr
            }
            _ => return Err(Error::ParseError(BAD_ELEMENT.to_owned())),
        };
        Ok(e)
    }
}

impl FromSkyhashBytes for Vec<Vec<u8>> {
    fn from_element(e: Element) -> SkyRawResult<Self> {
        let e = match e {
            Element::Array(Array::Bin(brr)) => {
                let mut newarr = Vec::with_capacity(brr.len());
                for item in brr {
                    if let Some(item) = item {
                        newarr.push(item);
                    } else {
                        return Err(Error::ParseError(HAS_NULL_ELEMENTS.to_owned()));
                    }
                }
                newarr
            }
            Element::Array(Array::Str(srr)) => {
                let mut newarr = Vec::with_capacity(srr.len());
                for item in srr {
                    if let Some(item) = item {
                        newarr.push(item.into_bytes());
                    } else {
                        return Err(Error::ParseError(HAS_NULL_ELEMENTS.to_owned()));
                    }
                }
                newarr
            }
            _ => return Err(Error::ParseError(BAD_ELEMENT.to_owned())),
        };
        Ok(e)
    }
}

impl FromSkyhashBytes for Element {
    fn from_element(e: Element) -> SkyRawResult<Element> {
        Ok(e)
    }
}

#[test]
fn test_arr_from_str_to_vecstr() {
    let arr = Element::Array(Array::Str(vec![
        Some("hello1".to_owned()),
        Some("world1".to_owned()),
        Some("hello2".to_owned()),
        Some("world2".to_owned()),
    ]));
    let arr: Vec<String> = arr.try_element_into().unwrap();
    assert_eq!(
        arr,
        vec![
            "hello1".to_owned(),
            "world1".to_owned(),
            "hello2".to_owned(),
            "world2".to_owned()
        ]
    );
}

#[test]
fn test_arr_from_str_with_null_to_vecstr() {
    let arr = Element::Array(Array::Str(vec![
        Some("hello1".to_owned()),
        Some("world1".to_owned()),
        None,
        Some("world2".to_owned()),
    ]));
    assert_eq!(
        arr.try_element_into::<Vec<String>>().unwrap_err(),
        Error::ParseError(HAS_NULL_ELEMENTS.to_string())
    )
}
