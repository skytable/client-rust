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

/// Response codes returned by the server
#[derive(Debug, PartialEq)]
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
        }
    }
}
