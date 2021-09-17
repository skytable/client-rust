/*
 * Created on Fri Sep 17 2021
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

extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

#[proc_macro_derive(Skyjson)]
pub fn derive_json(item: TokenStream) -> TokenStream {
    let x: DeriveInput = syn::parse_macro_input!(item);
    let struct_name = x.ident;

    match x.data {
        Data::Struct(st) => {
            if let Fields::Named(named_fields) = st.fields {
                let a = named_fields.named;
                if a.is_empty() {
                    panic!("`sky_derive::Skyjson` will not work on fieldless structs")
                }
            }
        }
        _ => panic!("`sky_derive::Skyjson` is only implemented for structs"),
    }

    let tokens = quote! {
        impl skytable::types::IntoSkyhashBytes for &#struct_name {
            fn as_bytes(&self) -> Vec<u8> {
                skytable::derive::into_json_bytes(&self)
            }
        }
        impl skytable::types::FromSkyhashBytes for #struct_name {
            fn from_element(e: skytable::Element) -> skytable::SkyRawResult<Self> {
                let s: String = e.try_element_into()?;
                match skytable::derive::from_json_bytes(&s) {
                    Ok(s) => Ok(s),
                    Err(e) => Err(skytable::error::Error::ParseError(e.to_string())),
                }
            }
        }
    };
    TokenStream::from(tokens)
}

#[proc_macro_derive(Skybin)]
pub fn derive_bin(item: TokenStream) -> TokenStream {
    let x: DeriveInput = syn::parse_macro_input!(item);
    let struct_name = x.ident;

    match x.data {
        Data::Struct(st) => {
            if let Fields::Named(named_fields) = st.fields {
                let a = named_fields.named;
                if a.is_empty() {
                    panic!("`sky_derive::Skybin` will not work on fieldless structs")
                }
            }
        }
        _ => panic!("`sky_derive::Skybin` is only implemented for structs"),
    }

    let tokens = quote! {
        impl skytable::types::IntoSkyhashBytes for &#struct_name {
            fn as_bytes(&self) -> Vec<u8> {
                skytable::derive::into_bin_bytes(&self)
            }
        }
        impl skytable::types::FromSkyhashBytes for #struct_name {
            fn from_element(e: skytable::Element) -> skytable::SkyRawResult<Self> {
                let s: Vec<u8> = e.try_element_into()?;
                match skytable::derive::from_bin_bytes(&s) {
                    Ok(s) => Ok(s),
                    Err(e) => Err(skytable::error::Error::ParseError(e.to_string())),
                }
            }
        }
    };
    TokenStream::from(tokens)
}
