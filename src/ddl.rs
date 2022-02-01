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
//! use skytable::ddl::{Ddl, Keymap, KeymapType};
//! use skytable::sync::Connection;
//!
//! let mut con = Connection::new("127.0.0.1", 2003).unwrap();
//! let table = Keymap::new("mykeyspace:mytable")
//!     .set_ktype(KeymapType::Str)
//!     .set_vtype(KeymapType::Binstr);
//! con.create_table(table).unwrap();
//! ```
//!

use crate::error::{errorstring, SkyhashError};
use crate::types::{Array, FlatElement};
use crate::Element;
use crate::IntoSkyhashBytes;
use crate::Query;
use crate::RespCode;
use crate::SkyResult;

cfg_async! {
    use crate::AsyncResult;
    use crate::actions::AsyncSocket;
}

cfg_sync! {
    use crate::actions::SyncSocket;
}

#[non_exhaustive]
#[derive(Debug, PartialEq)]
/// Data types for the Keymap data model
pub enum KeymapType {
    /// An unicode string
    Str,
    /// A binary string
    Binstr,
    /// A custom type
    Other(String),
}

/// A convenient representation for the `whereami` action
pub enum WhereAmI {
    /// The ID of the keyspace
    Keyspace(String),
    /// The ID of the keyspace (element 0) and the ID of the table (element 1)
    Table(String, String),
}

impl WhereAmI {
    /// Returns an entity group formatted string for this instance. For example:
    /// - if your connection level entity is just a keyspace, say `default`, then you'll get a string with "default"
    /// - Now, if you're in a table, say table `default` in the `default` keyspace, then you'll get a string with "default:default"
    pub fn into_entity_repr(self) -> String {
        match self {
            Self::Keyspace(ks) => ks,
            Self::Table(ks, tbl) => {
                format!("{ks}:{tbl}", ks = ks, tbl = tbl)
            }
        }
    }
}

impl KeymapType {
    fn priv_to_string(&self) -> String {
        match self {
            Self::Str => "str".to_owned(),
            Self::Binstr => "binstr".to_owned(),
            Self::Other(oth) => oth.clone(),
        }
    }
}

#[derive(Debug, PartialEq)]
/// A Keymap Model Table
///
pub struct Keymap {
    entity: String,
    ktype: Option<KeymapType>,
    vtype: Option<KeymapType>,
    volatile: bool,
}

impl Keymap {
    /// Create a new Keymap model with the provided entity and default types: `(binstr,binstr)`
    /// and the default volatility (by default a table is **not** volatile)
    pub fn new(entity: impl AsRef<str>) -> Self {
        Self {
            entity: entity.as_ref().to_owned(),
            ktype: None,
            vtype: None,
            volatile: false,
        }
    }
    /// Set the key type (defaults to `binstr`)
    pub fn set_ktype(mut self, ktype: KeymapType) -> Self {
        self.ktype = Some(ktype);
        self
    }
    /// Set the value type (defaults to `binstr`)
    pub fn set_vtype(mut self, vtype: KeymapType) -> Self {
        self.vtype = Some(vtype);
        self
    }
    /// Make the table volatile (defaults to `false`)
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
            ktype = self
                .ktype
                .as_ref()
                .unwrap_or(&KeymapType::Binstr)
                .priv_to_string(),
            vtype = self
                .vtype
                .as_ref()
                .unwrap_or(&KeymapType::Binstr)
                .priv_to_string(),
        );
        let q = Query::from("CREATE").arg("TABLE").arg(self.entity).arg(arg);
        if self.volatile {
            q.arg("volatile")
        } else {
            q
        }
    }
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
                fn $name<'s, $($($tyargs: $ty $(+$tye)*, )*)?>(&'s mut self $(, $argname: $argty)*) -> SkyResult<$ret> {
                    gen_match!(self.run($($block)?), $($($mtch)+, $expect),*)
                }
            )*
        }
        #[cfg(feature = "aio")]
        #[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
        /// [DDL queries](https://docs.skytable.io/ddl) that can be run on async socket
        /// connections
        pub trait AsyncDdl: AsyncSocket {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<'s, $($($tyargs: $ty $(+$tye)*, )*)?>(&'s mut self $(, $argname: $argty)*) -> AsyncResult<SkyResult<$ret>> {
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
    /// This function switches to the provided entity.
    ///
    /// This is equivalent to:
    /// ```text
    /// USE <entity>
    /// ```
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use skytable::ddl::Ddl;
    /// use skytable::sync::Connection;
    ///
    /// let mut con = Connection::new("127.0.0.1", 2003).unwrap();
    /// con.switch("mykeyspace:mytable").unwrap();
    /// ```
    ///
    fn switch<T: IntoSkyhashBytes + 's>(entity: T) -> () {
        { Query::from("use").arg(entity) }
        Element::RespCode(RespCode::Okay) => ()
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
            errorstring::ERR_ALREADY_EXISTS => false
        }
    }
    /// Create a table from the provided configuration
    fn create_table(table: impl CreateTableIntoQuery + 's) -> () {
        { table.into_query() }
        Element::RespCode(RespCode::Okay) => ()
    }
    /// Drop the provided table
    ///
    /// This returns true if the table was removed for false if the table didn't exist
    fn drop_table(table: impl IntoSkyhashBytes + 's) -> bool {
        { Query::from("DROP").arg("TABLE").arg(table) }
        Element::RespCode(RespCode::Okay) => true,
        Element::RespCode(RespCode::ErrorString(estr)) => match_estr! {
            estr,
            errorstring::CONTAINER_NOT_FOUND => false
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
    /// Check what keyspace this connection is currently connected to
    fn whereami() -> WhereAmI {
        {
            Query::from("whereami")
        }
        Element::Array(
            Array::Flat(mut frr)
        ) => {
            if frr.iter().all(|v| matches!(v, FlatElement::String(_))) {
                return Err(SkyhashError::InvalidResponse.into());
            }
            match frr.len() {
                1 => WhereAmI::Keyspace(match frr.swap_remove(0) {
                    FlatElement::String(st) => st,
                    _ => unsafe {
                        core::hint::unreachable_unchecked()
                    }
                }),
                2 => {
                    let (ks, tbl) = match (frr.swap_remove(0), frr.swap_remove(1)) {
                        (FlatElement::String(ks),FlatElement::String(tbl)) => (ks, tbl),
                        _ => unsafe {
                            core::hint::unreachable_unchecked()
                        }
                    };
                    WhereAmI::Table(ks, tbl)
                }
                _ => return Err(SkyhashError::InvalidResponse.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CreateTableIntoQuery, Keymap, KeymapType};
    #[test]
    fn test_query_generation_from_keymap() {
        let qgen = Keymap::new("mytbl")
            .set_ktype(KeymapType::Str)
            .set_vtype(KeymapType::Binstr)
            .into_query();
        let qman = crate::query!("CREATE", "TABLE", "mytbl", "keymap(str,binstr)");
        assert_eq!(qgen, qman);
    }
}
