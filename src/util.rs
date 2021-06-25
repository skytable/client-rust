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

#[macro_export]
macro_rules! cfg_any_ssl {
    ($($body:item)*) => {
        $(
            #[cfg(all(feature = "sync", any(feature = "ssl", feature = "sslv")))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "ssl", feature = "sslv"))))]
            $body
        )*
    };
}

#[macro_export]
macro_rules! cfg_sync {
    ($($body:item)*) => {
        $(
            #[cfg(feature = "sync")]
            #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
            $body
        )*
    };
}

#[macro_export]
macro_rules! cfg_async {
    ($($body:item)*) => {
        $(
            #[cfg(feature = "async")]
            #[cfg_attr(docsrs, doc(cfg(feature = "async")))]
            $body
        )*
    };
}

#[macro_export]
macro_rules! cfg_dbg {
    ($($body:item)*) => {
        $(
            #[cfg(feature = "dbg")]
            #[cfg_attr(docsrs, doc(cfg(feature = "dbg")))]
            $body
        )*
    };
}
