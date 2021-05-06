/*
 * Created on Wed May 05 2021
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

//! # The Terrapipe protocol
//! This module implements primitives for the Terrapipe protocol
//!

/// Response codes returned by the server
#[derive(Debug, PartialEq)]
pub enum RespCode {
    /// `0`: Okay (Empty Response)
    Okay,
    /// `1`: Not Found
    NotFound,
    /// `2`: Overwrite Error
    OverwriteError,
    /// `3`: Action Error
    ActionError,
    /// `4`: Packet Error
    PacketError,
    /// `5`: Server Error
    ServerError,
    /// `6`: Some other Error, which is a string
    ErrorString(String),
    /// `6`: The same as [`RespCode::ErrorString`] but without any explicit information
    OtherError,
}

impl RespCode {
    pub fn from_str(st: &str) -> Self {
        use RespCode::*;
        let res = match st.parse::<u8>() {
            Ok(val) => match val {
                0 => Okay,
                1 => NotFound,
                2 => OverwriteError,
                3 => ActionError,
                4 => PacketError,
                5 => ServerError,
                6 => OtherError,
                _ => Self::ErrorString(st.to_owned()),
            },
            Err(_) => return ErrorString(st.to_owned()),
        };
        res
    }
}
