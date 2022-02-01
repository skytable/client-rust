/*
 * Created on Fri Jun 25 2021
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

#![allow(unused_macros)] // This is done just to avoid unnecessary complications

macro_rules! gen_match {
    ($ret:expr, $($($mtch:pat)+ $(if $exp:expr)*, $expect:expr),*) => {
        match $ret {
            $($(Ok($mtch))|* $(if $exp:expr)* => Ok($expect),)*
            // IMPORTANT: Translate respcodes into errors!
            Ok($crate::Element::RespCode(rc)) => Err($crate::error::SkyhashError::Code(rc).into()),
            Ok(_) => Err($crate::error::SkyhashError::UnexpectedDataType.into()),
            Err(e) => Err(e),
        }
    };
}

macro_rules! match_estr {
    (
        $ret:expr,
        $($mtch:pat => $expect:expr),*
    ) => {
        match $ret.as_str() {
            $($mtch => $expect,)*
            _ => return Err($crate::error::SkyhashError::UnexpectedResponse.into())
        }
    };
}

macro_rules! cfg_sync_ssl_any {
    ($($body:item)*) => {
        $(
            #[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
            #[cfg_attr(docsrs, doc(cfg(all(feature="sync", any(feature = "ssl", feature = "sslv")))))]
            $body
        )*
    };
}

macro_rules! cfg_ssl_any {
    ($($body:item)*) => {
        $(
            #[cfg(any(feature = "ssl", feature = "sslv", feature="aio-ssl", feature="aio-sslv"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "ssl", feature = "sslv", feature="aio-ssl", feature="aio-sslv"))))]
            $body
        )*
    };
}

macro_rules! cfg_async_ssl_any {
    ($($body:item)*) => {
        $(
            #[cfg(all(feature = "aio", any(feature = "aio-ssl", feature = "aio-sslv")))]
            #[cfg_attr(docsrs, doc(cfg(all(feature="aio", any(feature = "aio-ssl", feature = "aio-sslv")))))]
            $body
        )*
    };
}

macro_rules! cfg_sync {
    ($($body:item)*) => {
        $(
            #[cfg(feature = "sync")]
            #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
            $body
        )*
    };
}

macro_rules! cfg_sync_pool {
    ($($body:item)*) => {
        $(
            #[cfg(all(feature = "sync", feature= "pool"))]
            #[cfg_attr(docsrs, doc(cfg(all(feature = "sync", feature = "pool"))))]
            $body
        )*
    };
}

macro_rules! cfg_async {
    ($($body:item)*) => {
        $(
            #[cfg(feature = "aio")]
            #[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
            $body
        )*
    };
}

macro_rules! cfg_async_pool {
    ($($body:item)*) => {
        $(
            #[cfg(all(feature = "aio", feature= "aio-pool"))]
            #[cfg_attr(docsrs, doc(cfg(all(feature = "aio", feature = "aio-pool"))))]
            $body
        )*
    };
}

macro_rules! cfg_dbg {
    ($($body:item)*) => {
        $(
            #[cfg(feature = "dbg")]
            #[cfg_attr(docsrs, doc(cfg(feature = "dbg")))]
            $body
        )*
    };
}

macro_rules! cfg_pool_any {
    ($($body:item)*) => {
        $(
            #[cfg(any(
                feature = "sync",
                feature = "pool",
                feature = "aio",
                feature = "aio-pool"
            ))]
            #[cfg_attr(
                docsrs,
                doc(cfg(any(
                    feature = "sync",
                    feature = "pool",
                    feature = "aio",
                    feature = "aio-pool"
                )))
            )]
            $body
        )*
    };
}
