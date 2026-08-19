use proc_macro::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::Attribute;
use syn::Fields;
use syn::ItemEnum;
use syn::ItemStruct;
use syn::Variant;

#[proc_macro_attribute]
pub fn versioned(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: syn::Item = parse_macro_input!(item);

    match item {
        syn::Item::Struct(input) => expand_struct(input),
        syn::Item::Enum(input) => expand_enum(input),
        item => syn::Error::new(item.span(), "#[versioned] only supports structs and enums")
            .to_compile_error()
            .into(),
    }
}

fn expand_struct(input: ItemStruct) -> TokenStream {
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

fn expand_enum(input: ItemEnum) -> TokenStream {
    let vis = &input.vis;
    let name = &input.ident;
    let generics = &input.generics;
    let enum_attrs: &[Attribute] = &input.attrs;
    let filtered_enum_attrs: Vec<Attribute> =
        enum_attrs.iter().filter(|attr| !attr.path().is_ident("versioned")).cloned().collect();
    let old_name = format_ident!("{}Old", name);

    let (old_variants, new_variants): (Vec<Variant>, Vec<Variant>) = input
        .variants
        .into_iter()
        .map(|mut variant| {
            let is_deprecated = variant.attrs.iter().any(|attr| attr.path().is_ident("legacy"));
            let is_new_variant = variant.attrs.iter().any(|attr| attr.path().is_ident("future"));

            // Keep attributes such as #[cfg], but remove versioning markers from both enums.
            variant.attrs.retain(|attr| {
                let path = attr.path();
                !path.is_ident("legacy") && !path.is_ident("future")
            });

            let old_variant = (!is_new_variant).then(|| variant.clone());
            let new_variant = (!is_deprecated).then_some(variant);
            (old_variant, new_variant)
        })
        .fold((Vec::new(), Vec::new()), |(mut old, mut new), (old_variant, new_variant)| {
            if let Some(variant) = old_variant {
                old.push(variant);
            }
            if let Some(variant) = new_variant {
                new.push(variant);
            }
            (old, new)
        });

    quote! {
        // Old enum
        #(#filtered_enum_attrs)*
        #vis enum #old_name #generics {
            #(#old_variants),*
        }

        // New enum
        #(#filtered_enum_attrs)*
        #vis enum #name #generics {
            #(#new_variants),*
        }
    }
    .into()
}
