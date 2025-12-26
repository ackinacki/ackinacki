use proc_macro::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::Attribute;
use syn::Fields;
use syn::ItemStruct;

#[proc_macro_attribute]
pub fn versioned(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let vis = &input.vis;
    let name = &input.ident;
    let new_name = format_ident!("{}", name);
    let generics = &input.generics;
    let struct_attrs: &[Attribute] = &input.attrs;
    // IMPORTANT: we should re-apply other attributes to the new struct.
    // But we should avoid proc_macro_attr recursion, so we filter out #[versioned].
    let filtered_struct_attrs: Vec<Attribute> =
        struct_attrs.iter().filter(|&a| !a.path().is_ident("versioned")).cloned().collect();

    let old_name = format_ident!("{}Old", name);

    let fields = match &input.fields {
        Fields::Named(named) => named.named.clone(),
        _ => {
            return syn::Error::new(
                input.span(),
                "#[versioned] only supports structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut old_fields = Vec::new();
    let mut new_fields = Vec::new();

    for mut field in fields {
        let mut is_deprecated = false;
        let mut is_new_field = false;

        for attr in &field.attrs {
            if attr.path().is_ident("legacy") {
                is_deprecated = true;
            } else if attr.path().is_ident("future") {
                is_new_field = true;
            }
        }

        // keep other attributes, drop markers
        field.attrs.retain(|a| {
            let p = a.path();
            !p.is_ident("legacy") && !p.is_ident("future")
        });

        if !is_new_field {
            old_fields.push(field.clone());
        }

        if !is_deprecated {
            new_fields.push(field);
        }
    }

    let expanded = quote! {
        // Old struct
        #(#filtered_struct_attrs)*
        #vis struct #old_name #generics {
            #(#old_fields),*
        }

        // New struct
        #(#filtered_struct_attrs)*
        #vis struct #new_name #generics {
            #(#new_fields),*
        }
    };

    expanded.into()
}
