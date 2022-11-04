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

//! Response codes

use core::fmt;

/// Response codes returned by the server
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
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
    /// `7`: Wrongtype Error
    Wrongtype,
    /// `8`: Unknown Data Type Error
    UnknownDataType,
    /// `9`: Encoding error
    EncodingError,
    /// `10`: Bad authn credentials
    AuthBadCredentials,
    /// `11`: Permission error
    AuthPermissionError,
}

impl RespCode {
    pub(crate) fn from_str(st: &str) -> Self {
        use RespCode::*;
        match st.parse::<u8>() {
            Ok(val) => match val {
                0 => Okay,
                1 => NotFound,
                2 => OverwriteError,
                3 => ActionError,
                4 => PacketError,
                5 => ServerError,
                6 => OtherError,
                7 => Wrongtype,
                8 => UnknownDataType,
                9 => EncodingError,
                10 => AuthBadCredentials,
                11 => AuthPermissionError,
                _ => Self::ErrorString(st.to_owned()),
            },
            Err(_) => ErrorString(st.to_owned()),
        }
    }
}

impl From<RespCode> for u8 {
    fn from(rcode: RespCode) -> u8 {
        use RespCode::*;
        match rcode {
            Okay => 0,
            NotFound => 1,
            OverwriteError => 2,
            ActionError => 3,
            PacketError => 4,
            ServerError => 5,
            OtherError | ErrorString(_) => 6,
            Wrongtype => 7,
            UnknownDataType => 8,
            EncodingError => 9,
            AuthBadCredentials => 10,
            AuthPermissionError => 11,
        }
    }
}

impl fmt::Display for RespCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespCode::*;
        match self {
            Okay => write!(f, "Response code: 0 (okay)"),
            NotFound => write!(f, "Response code: 1 (not found)"),
            OverwriteError => write!(f, "Response code: 2 (overwrite error)"),
            ActionError => write!(f, "Response code: 3 (action error)"),
            PacketError => write!(f, "Response code: 4 (client side packet error)"),
            ServerError => write!(f, "Response code: 5 (server error)"),
            OtherError => write!(f, "Response code: 6, (other error)"),
            Wrongtype => write!(f, "Response code: 7 (wrongtype error)"),
            UnknownDataType => write!(f, "Response code: 8 (unknown data type error)"),
            EncodingError => write!(f, "Response code: 9 (encoding error)"),
            AuthBadCredentials => write!(f, "Response code: 10 (bad auth credentials)"),
            AuthPermissionError => write!(f, "Response code: 11 (auth permission error)"),
            ErrorString(estr) => write!(f, "Error: {}", estr),
        }
    }
}
