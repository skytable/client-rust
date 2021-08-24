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
//! ## Example: creating tables
//!
//! ```no_run
//! use skytable::ddl::{CreateTableResult, Ddl, Keymap};
//! use skytable::sync::Connection;
//! fn main() {
//!     let mut con = Connection::new("127.0.0.1", 2003).unwrap();
//!     let table = Keymap::new("mykeyspace:mytable")
//!         .set_ktype("str")
//!         .set_vtype("binstr");
//!     assert_eq!(
//!        con.create_table(table).unwrap(),
//!        CreateTableResult::Okay
//!     );
//! }
//! ```
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

#[derive(Debug, PartialEq)]
/// A Keymap Model Table
///
pub struct Keymap {
    entity: Option<String>,
    ktype: Option<String>,
    vtype: Option<String>,
    volatile: bool,
}

impl Keymap {
    /// Create a new Keymap model with the provided entity and default types: `(binstr,binstr)`
    /// and the default volatility (by default a table is **not** volatile)
    pub fn new(entity: impl AsRef<str>) -> Self {
        Self {
            entity: Some(entity.as_ref().to_owned()),
            ktype: None,
            vtype: None,
            volatile: false,
        }
    }
    /// Set the key type
    pub fn set_ktype(mut self, ktype: impl AsRef<str>) -> Self {
        self.ktype = Some(ktype.as_ref().to_owned());
        self
    }
    /// Set the value type
    pub fn set_vtype(mut self, vtype: impl AsRef<str>) -> Self {
        self.vtype = Some(vtype.as_ref().to_owned());
        self
    }
    /// Make the table volatile
    pub fn set_volatile(mut self) -> Self {
        self.volatile = true;
        self
    }
}

/// Any object that represents a table and that can be turned into a query
pub trait CreateTableIntoQuery: Send + Sync {
    /// Turns self into a query
    fn into_query(self) -> Query;
}

impl CreateTableIntoQuery for Keymap {
    fn into_query(self) -> Query {
        let arg = format!(
            "keymap({ktype},{vtype})",
            ktype = self.ktype.unwrap_or_else(|| "binstr".to_owned()),
            vtype = self.vtype.unwrap_or_else(|| "binstr".to_owned()),
        );
        let q = Query::from("CREATE").arg("TABLE").arg(arg);
        if self.volatile {
            q.arg("volatile")
        } else {
            q
        }
    }
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

#[derive(Debug, PartialEq)]
#[non_exhaustive]
/// Result of creating tables
pub enum CreateTableResult {
    Okay,
    AlreadyExists,
    DefaultContainerUnset,
    ProtectedObject,
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
    /// Create the provided keyspace
    ///
    /// This is equivalent to:
    /// ```text
    /// CREATE KEYSPACE <ksname>
    /// ```
    /// This will return true if the keyspace was created or false if the keyspace
    /// already exists
    fn create_keyspace(ks: impl IntoSkyhashBytes + 's) -> bool {
        { Query::from("CREATE").arg("KEYSPACE").arg(ks) }
        Element::RespCode(RespCode::Okay) => true,
        Element::RespCode(RespCode::ErrorString(estr)) => match_estr! {
            estr,
            errors::ERR_ALREADY_EXISTS => false
        }
    }
    /// Create a table from the provided configuration
    fn create_table(table: impl CreateTableIntoQuery + 's) -> CreateTableResult {
        { table.into_query() }
        Element::RespCode(RespCode::Okay) => CreateTableResult::Okay,
        Element::RespCode(RespCode::ErrorString(estr)) => {
            match_estr! {
                estr,
                errors::ERR_ALREADY_EXISTS => CreateTableResult::AlreadyExists,
                errors::DEFAULT_CONTAINER_UNSET => CreateTableResult::DefaultContainerUnset,
                errors::ERR_PROTECTED_OBJECT => CreateTableResult::ProtectedObject
            }
        }
    }
    /// Drop the provided table
    ///
    /// This returns true if the table was removed for false if the table didn't exist
    fn drop_table(table: impl IntoSkyhashBytes + 's) -> bool {
        { Query::from("DROP").arg("TABLE").arg(table) }
        Element::RespCode(RespCode::Okay) => true,
        Element::RespCode(RespCode::ErrorString(estr)) => match_estr! {
            estr,
            errors::CONTAINER_NOT_FOUND => false
        }
    }
    /// Drop the provided keyspace
    ///
    fn drop_keyspace(keyspace: impl IntoSkyhashBytes + 's, force: bool) -> () {
        {
            let q = Query::from("DROP").arg("KEYSPACE").arg(keyspace);
            if force {
                q.arg("force")
            } else {
                q
            }
        }
        Element::RespCode(RespCode::Okay) => {}
    }
}
