extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields};

#[proc_macro_derive(Query)]
pub fn derive_query(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let ret = match input.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields) => {
                    let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
                    assert!(!field_names.is_empty(), "can't derive on empty field");
                    quote! {
                        impl #impl_generics ::skytable::query::SQParam for #name #ty_generics #where_clause {
                            fn append_param(&self, q: &mut Vec<u8>) -> usize {
                                let mut size = 0;
                                #(size += ::skytable::query::SQParam::append_param(&self.#field_names, q);)*
                                size
                            }
                        }
                    }
                },
                _ => unimplemented!(),
            }
        },
        _ => unimplemented!(),
    };
    TokenStream::from(ret)
}

#[proc_macro_derive(Response)]
pub fn derive_response(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let ret = match input.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields) => {
                    let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
                    assert!(!field_names.is_empty(), "can't derive on empty field");
                    let tuple_pattern = quote! { (#(#field_names),*) };
                    let struct_instantiation = quote! { Self { #(#field_names),* } };    
                    quote! {
                        impl #impl_generics skytable::response::FromResponse for #name #ty_generics #where_clause {
                            fn from_response(resp: skytable::response::Response) -> skytable::ClientResult<Self> {
                                let #tuple_pattern = skytable::response::FromResponse::from_response(resp)?;
                                Ok(#struct_instantiation)
                            }
                        }
                    }
                },
                _ => unimplemented!(),
            }
        },
        _ => unimplemented!(),
    };
    TokenStream::from(ret)
}
