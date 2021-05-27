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
///     fn into_string(&self) -> String {
///         self.0.to_string()
///     }
/// }
/// ```
///
pub trait IntoSkyhashBytes: Send + Sync {
    /// Turn `Self` into a [`String`]
    fn into_string(&self) -> String;
}

macro_rules! impl_skyhash_bytes {
    ($($ty:ty),*) => {
        $(
            impl IntoSkyhashBytes for $ty {
                fn into_string(&self) -> String {
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
        q._push_arg(self.into_string());
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
        self.into_iter()
            .for_each(|elem| elem.push_into_query(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        self.len()
    }
}

impl<T: IntoSkyhashBytes, const N: usize> IntoSkyhashAction for [T; N] {
    fn push_into_query(&self, mut data: &mut Query) {
        self.into_iter()
            .for_each(|elem| elem.push_into_query(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        self.len()
    }
}

impl<T: IntoSkyhashBytes, const N: usize> IntoSkyhashAction for &'static [T; N] {
    fn push_into_query(&self, mut data: &mut Query) {
        self.into_iter()
            .for_each(|elem| elem.push_into_query(&mut data));
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
