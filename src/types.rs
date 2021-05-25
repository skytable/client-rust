/*
 * Created on Tue May 25 2021
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

pub trait IntoSkyhashBytes {
    fn into_string(&self) -> String;
}

macro_rules! impl_skyhash_bytes {
    ($($ty:ty),*) => {
        $(
            impl IntoSkyhashBytes for $ty {
                fn into_string(&self) -> String {
                    self.to_string()
                }
            }
        )*
    };
}

impl_skyhash_bytes!(
    u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, f32, f64, bool, char, usize, String, &str
);

pub trait IntoSkyhashAction {
    fn extend_bytes(&self, data: &mut Vec<u8>);
    fn incr_len_by(&self) -> usize;
}

impl<T> IntoSkyhashAction for T
where
    T: IntoSkyhashBytes,
{
    fn extend_bytes(&self, data: &mut Vec<u8>) {
        let arg = self.into_string();
        if arg.len() == 0 {
            panic!("Argument cannot be empty")
        }
        // A data element will look like:
        // `+<bytes_in_next_line>\n<data>`
        data.push(b'+');
        let bytes_in_next_line = arg.len().to_string().into_bytes();
        data.extend(bytes_in_next_line);
        // add the LF char
        data.push(b'\n');
        // Add the data itself, which is `arg`
        data.extend(arg.into_bytes());
        data.push(b'\n'); // add the LF char
    }
    fn incr_len_by(&self) -> usize {
        1
    }
}

impl<T> IntoSkyhashAction for &[T]
where
    T: IntoSkyhashBytes,
{
    fn extend_bytes(&self, mut data: &mut std::vec::Vec<u8>) {
        self.into_iter()
            .for_each(|elem| elem.extend_bytes(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        self.len()
    }
}

impl<T: IntoSkyhashBytes, const N: usize> IntoSkyhashAction for [T; N] {
    fn extend_bytes(&self, mut data: &mut std::vec::Vec<u8>) {
        self.into_iter()
            .for_each(|elem| elem.extend_bytes(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        N
    }
}

impl<T: IntoSkyhashBytes, const N: usize> IntoSkyhashAction for &'static [T; N] {
    fn extend_bytes(&self, mut data: &mut std::vec::Vec<u8>) {
        self.into_iter()
            .for_each(|elem| elem.extend_bytes(&mut data));
    }
    fn incr_len_by(&self) -> usize {
        N
    }
}
