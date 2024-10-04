// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::DeriveInput;

#[proc_macro_derive(FieldList)]
pub fn derive_fields(item: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(item as DeriveInput);
    let struct_name = &ast.ident;

    let fields = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(ref fields),
        ..
    }) = ast.data
    {
        fields
    } else {
        panic!("Only support Struct")
    };

    let mut keys = Vec::new();

    for field in fields.named.iter() {
        let field_name: &syn::Ident = field.ident.as_ref().unwrap();
        let name: String = field_name.to_string();
        let literal_key_str = syn::LitStr::new(&name, field.span());
        keys.push(quote! { #literal_key_str });
    }

    let expanded = quote! {

        impl #struct_name {
            pub fn list() -> Vec<String> {
                let mut fields: Vec<String> = Vec::new();
                #(
                    fields.push(#keys.to_string());
                )*
                fields
            }
        }

    };
    expanded.into()
}
