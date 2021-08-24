/*
 * Created on Mon Aug 23 2021
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

//! # Data definition Language (DDL) Queries
//!
//! This module contains other modules, types, traits and functions to use the DDL
//! abilities of Skytable efficiently.
//!

use crate::error::SkyhashError;
use crate::Element;
use crate::IntoSkyhashBytes;
use crate::Query;
use crate::RespCode;
use crate::SkyRawResult;

cfg_async! {
    use crate::actions::AsyncResult;
    use crate::actions::AsyncSocket;
}

cfg_sync! {
    use crate::actions::SyncSocket;
}

pub mod errors {
    //! A module of errors for DDL queries
    pub const DEFAULT_CONTAINER_UNSET: &str = "default-container-unset";
    pub const CONTAINER_NOT_FOUND: &str = "container-not-found";
    pub const STILL_IN_USE: &str = "still-in-use";
    pub const ERR_PROTECTED_OBJECT: &str = "err-protected-object";
    pub const ERR_ALREADY_EXISTS: &str = "err-already-exists";
    pub const ERR_NOT_READY: &str = "not-ready";
}

/// Result of switching entities
#[non_exhaustive]
#[derive(Debug, PartialEq)]
pub enum SwitchEntityResult {
    ContainerNotFound,
    ProtectedObject,
    NotReady,
    Okay,
}

macro_rules! implement_ddl {
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
        /// [DDL queries](https://docs.skytable.io/ddl) that can be run on a sync socket
        /// connections
        pub trait Ddl: SyncSocket {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<'s, $($($tyargs: $ty $(+$tye)*, )*)?>(&'s mut self $(, $argname: $argty)*) -> SkyRawResult<$ret> {
                    gen_match!(self.run($($block)?), $($($mtch)+, $expect),*)
                }
            )*
        }
        #[cfg(feature = "async")]
        #[cfg_attr(docsrs, doc(cfg(feature = "async")))]
        /// [DDL queries](https://docs.skytable.io/ddl) that can be run on async socket
        /// connections
        pub trait AsyncDdl: AsyncSocket {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<'s, $($($tyargs: $ty $(+$tye)*, )*)?>(&'s mut self $(, $argname: $argty)*) -> AsyncResult<SkyRawResult<$ret>> {
                    Box::pin(async move {gen_match!(self.run($($block)?).await, $($($mtch)+, $expect),*)})
                }
            )*
        }
    };
}

cfg_async! {
    impl<T> AsyncDdl for T where T: AsyncSocket {}
}

cfg_sync! {
    impl<T> Ddl for T where T: SyncSocket {}
}

implement_ddl! {
    /// This function switches to the provided entity. A [`SwitchEntityResult`] is returned
    /// if the entity could not be switched.
    ///
    /// This is equivalent to:
    /// ```text
    /// USE <entity>
    /// ```
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use skytable::ddl::{Ddl, SwitchEntityResult};
    /// use skytable::sync::Connection;
    ///
    /// let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    /// match con.switch("mykeyspace:mytable").unwrap() {
    ///     SwitchEntityResult::Okay => {},
    ///     SwitchEntityResult::ContainerNotFound => println!("oops, couldn't find the container"),
    ///     SwitchEntityResult::NotReady => println!("oops, the container is not ready!"),
    ///     SwitchEntityResult::ProtectedObject => println!("Oh no, protected object"),
    ///     _ => panic!("uh oh, something bad happened")
    /// }
    /// ```
    ///
    fn switch<T: IntoSkyhashBytes + 's>(entity: T) -> SwitchEntityResult {
        { Query::from("use").arg(entity) }
        Element::RespCode(RespCode::Okay) => SwitchEntityResult::Okay,
        Element::RespCode(RespCode::ErrorString(estr)) => {
            match estr.as_str() {
                errors::CONTAINER_NOT_FOUND => SwitchEntityResult::ContainerNotFound,
                errors::ERR_NOT_READY => SwitchEntityResult::NotReady,
                errors::ERR_PROTECTED_OBJECT => SwitchEntityResult::ProtectedObject,
                _ => return Err(SkyhashError::UnexpectedDataType.into())
            }
        }
    }
}
