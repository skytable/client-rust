/*
 * Copyright 2023, Sayan Nandan <nandansayan@outlook.com>
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

#[macro_export]
macro_rules! query {
    ($query_str:expr) => { $crate::Query::from($query_str) };
    ($query_str:expr$(, $($query_param:expr),* $(,)?)?) => {{
        let mut q = $crate::Query::from($query_str); $($(q.push_param($query_param);)*)*q
    }};
}

macro_rules! pushlen {
    ($buf:expr, $len:expr) => {{
        let mut buf = ::itoa::Buffer::new();
        let r = ::itoa::Buffer::format(&mut buf, $len);
        $buf.extend(str::as_bytes(r));
        $buf.push(b'\n');
    }};
}
