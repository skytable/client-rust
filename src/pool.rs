/*
 * Copyright 2022, Sayan Nandan <nandansayan@outlook.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

// re-exports
#[cfg(any(feature = "sync", feature = "pool"))]
pub use self::sync_impls::{ConnectionManager, Pool, TlsPool};

use crate::error::Error;
use core::fmt;

#[derive(Debug)]
pub enum PoolError {
    PoolError(String),
    Other(Error),
}

impl From<Error> for PoolError {
    fn from(e: Error) -> Self {
        Self::Other(e)
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PoolError(e) => write!(f, "Pool error: {}", e),
            Self::Other(e) => write!(f, "Pool connection error: {}", e),
        }
    }
}

impl std::error::Error for PoolError {}

#[derive(Debug)]
pub enum ConnectionProfile {
    NoTls {
        host: String,
        port: u16,
    },
    Tls {
        host: String,
        port: u16,
        cert: String,
    },
}

#[cfg(any(feature = "sync", feature = "pool"))]
mod sync_impls {
    use super::{ConnectionProfile, PoolError};
    use crate::sync::{Connection as SyncConnection, TlsConnection as SyncTlsConnection};
    use crate::{
        error::{Error, SkyhashError},
        Element, Query, SkyQueryResult, SkyResult,
    };
    use core::marker::PhantomData;
    use r2d2::ManageConnection;

    pub type Pool = r2d2::Pool<ConnectionManager<SyncConnection>>;
    pub type TlsPool = r2d2::Pool<ConnectionManager<SyncTlsConnection>>;

    #[derive(Debug)]
    pub struct ConnectionManager<C> {
        profile: ConnectionProfile,
        _m: PhantomData<C>,
    }

    impl<C> ConnectionManager<C> {
        fn _new(profile: ConnectionProfile) -> Self {
            Self {
                profile,
                _m: PhantomData,
            }
        }
    }

    impl ConnectionManager<SyncConnection> {
        pub fn new(host: String, port: u16) -> Self {
            Self::_new(ConnectionProfile::NoTls { host, port })
        }
    }

    impl ConnectionManager<SyncTlsConnection> {
        pub fn new_tls(host: String, port: u16, cert: String) -> Self {
            Self::_new(ConnectionProfile::Tls { host, port, cert })
        }
    }

    pub trait PoolableConnection: Send + Sync + Sized {
        fn get_connection(profile: &ConnectionProfile) -> SkyResult<Self>;
        fn run_query(&mut self, q: Query) -> SkyQueryResult;
    }

    impl PoolableConnection for SyncConnection {
        fn get_connection(profile: &ConnectionProfile) -> SkyResult<Self> {
            if let ConnectionProfile::NoTls { host, port } = profile {
                let c = Self::new(host, *port)?;
                Ok(c)
            } else {
                Err(Error::ConfigurationError(
                    "Connection profile is TLS. Expected TCP",
                ))
            }
        }
        fn run_query(&mut self, q: Query) -> SkyQueryResult {
            self.run_simple_query(&q)
        }
    }

    impl PoolableConnection for SyncTlsConnection {
        fn get_connection(profile: &ConnectionProfile) -> SkyResult<Self> {
            if let ConnectionProfile::Tls { host, port, cert } = profile {
                let c = Self::new(host, *port, cert)?;
                Ok(c)
            } else {
                Err(Error::ConfigurationError(
                    "Connection profile is TCP. Expected TLS",
                ))
            }
        }
        fn run_query(&mut self, q: Query) -> SkyQueryResult {
            self.run_simple_query(&q)
        }
    }
    impl<C: PoolableConnection + 'static> ManageConnection for ConnectionManager<C> {
        type Error = PoolError;
        type Connection = C;
        fn connect(&self) -> Result<Self::Connection, Self::Error> {
            C::get_connection(&self.profile).map_err(|e| Self::Error::Other(e))
        }
        fn is_valid(&self, con: &mut Self::Connection) -> Result<(), Self::Error> {
            let q = crate::query!("HEYA");
            match con.run_query(q)? {
                Element::String(st) if st.eq("HEY!") => Ok(()),
                _ => Err(PoolError::Other(Error::SkyError(
                    SkyhashError::UnexpectedResponse,
                ))),
            }
        }
        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }
}
