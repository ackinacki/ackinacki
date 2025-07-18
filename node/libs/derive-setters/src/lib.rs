extern crate proc_macro;

use std::result::Result;

use darling::util::Flag;
use darling::*;
use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro2::TokenStream as SynTokenStream;
use quote::*;
use syn::spanned::Spanned;
use syn::*;

fn error(_: Span, data: &str) -> SynTokenStream {
    quote! { compile_error!(#data); }
}

#[derive(Debug, Clone, FromMeta)]
struct ExternalDelegate {
    /// The type to generate a delegate for.
    ty: Path,
    /// The field to delegate the methods to.
    #[darling(default)]
    field: Option<Ident>,
    /// The method to delegate the methods to.
    #[darling(default)]
    method: Option<Ident>,
    /// A prefix for the delegated setter methods.
    #[darling(default)]
    prefix: Option<String>,
}

#[derive(Debug, Clone, FromDeriveInput)]
#[darling(attributes(setters), supports(struct_named))]
struct ContainerAttrs {
    ident: Ident,
    generics: Generics,

    /// Use the core library rather than the std library.
    #[darling(default)]
    no_std: Flag,

    /// Whether to accept any field that converts via `Into` by default.
    #[darling(default)]
    into: bool,

    /// Whether to strip `Option<T>` into `T` in the parameters for the setter method by default.
    #[darling(default)]
    strip_option: bool,

    /// Whether to borrow or take ownership of `self` in the setter method by default.
    #[darling(default)]
    borrow_self: bool,

    /// Whether to check if option is None for Option fields.
    #[darling(default)]
    assert_none: bool,

    /// Separate actions when setter is called with no change.
    /// If assert_none is set with this option, this option takes precedence.
    #[darling(default)]
    no_change_action: Option<Expr>,

    /// Adds post process actions for setters.
    #[darling(default)]
    postprocess: Option<Expr>,

    /// Override setters returned type.
    #[darling(default)]
    result: Option<Type>,

    /// Whether to generate a method that sets a boolean by default.
    #[darling(default)]
    bool: bool,

    /// Whether to generate setters for this struct's fields by default. If set to false,
    /// `generate_public` and `generate_private` are ignored.
    #[darling(default)]
    generate: Option<bool>,

    /// Whether to generate setters for this struct's public fields by default.
    #[darling(default)]
    generate_public: Option<bool>,

    /// Whether to generate setters for this struct's private fields by default.
    #[darling(default)]
    generate_private: Option<bool>,

    /// A prefix for the generated setter methods.
    #[darling(default)]
    prefix: Option<String>,

    /// Other types to generate delegates to this type for. Note that this does not support
    /// generics.
    #[darling(multiple)]
    generate_delegates: Vec<ExternalDelegate>,

    /// Whether to generate a trace.
    #[darling(default)]
    trace: Option<bool>,
}

#[derive(Debug, Clone, FromField)]
#[darling(attributes(setters), forward_attrs(doc))]
struct FieldAttrs {
    attrs: Vec<Attribute>,

    /// The name of the generated setter method.
    #[darling(default)]
    rename: Option<Ident>,

    /// Whether to accept any field that converts via `Into` to this field's type.
    #[darling(default)]
    into: Option<bool>,

    /// Whether to strip `Option<T>` into `T` in the parameters for the setter method.
    #[darling(default)]
    strip_option: Option<bool>,

    /// Whether to borrow or take ownership of `self` for the setter method.
    #[darling(default)]
    borrow_self: Option<bool>,

    /// Whether to check if option is None for Option fields.
    assert_none: Option<bool>,

    /// Separate actions when setter is called with no change.
    /// If assert_none is set with this option, this option takes precedence.
    #[darling(default)]
    no_change_action: Option<Expr>,

    /// Adds post process actions for setters.
    #[darling(default)]
    postprocess: Option<Expr>,

    /// Override setters returned type.
    #[darling(default)]
    result: Option<Type>,

    /// Whether to generate a method that sets a boolean.
    #[darling(default)]
    bool: Option<bool>,

    /// Whether to generate a setter for this field regardless of global settings.
    #[darling(default)]
    generate: bool,

    /// Whether to skip this field regardless of global settings. Overwrites `generate`.
    #[darling(default)]
    skip: bool,

    /// The documentation used for this field's setter.
    #[darling(default)]
    doc: Option<String>,

    /// Whether to generate a trace.
    #[darling(default)]
    trace: Option<bool>,
}

struct ContainerDef {
    name: Ident,
    ty: Type,
    std: Ident,
    generics: Generics,

    prefix: String,
    uses_into: bool,
    strip_option: bool,
    borrow_self: bool,
    assert_none: bool,
    no_change_action: Option<Expr>,
    postprocess: Option<Expr>,
    result: Option<Type>,
    bool: bool,
    generate_public: bool,
    generate_private: bool,
    trace: bool,

    generate_delegates: Vec<ExternalDelegate>,
}

struct FieldDef {
    field_name: Ident,
    field_ty: Type,
    field_doc: SynTokenStream,
    setter_name: Ident,
    uses_into: bool,
    strip_option: bool,
    borrow_self: bool,
    assert_none: bool,
    no_change_action: Option<Expr>,
    postprocess: Option<Expr>,
    result: Option<Type>,
    bool: bool,
    trace: bool,
}

fn init_container_def(input: &DeriveInput) -> Result<ContainerDef, SynTokenStream> {
    let darling_attrs: ContainerAttrs = match FromDeriveInput::from_derive_input(input) {
        Ok(v) => v,
        Err(e) => return Err(e.write_errors()),
    };

    let ident = &darling_attrs.ident;
    let (_, generics_ty, _) = darling_attrs.generics.split_for_impl();
    let ty = quote! { #ident #generics_ty };

    let generate = darling_attrs.generate.unwrap_or(true);
    Ok(ContainerDef {
        name: darling_attrs.ident,
        ty: parse2(ty).expect("Internal error: failed to parse internally generated type."),
        std: if darling_attrs.no_std.is_present() {
            Ident::new("core", Span::call_site())
        } else {
            Ident::new("std", Span::call_site())
        },
        borrow_self: darling_attrs.borrow_self,
        assert_none: darling_attrs.assert_none,
        no_change_action: darling_attrs.no_change_action,
        postprocess: darling_attrs.postprocess,
        result: darling_attrs.result,
        generics: darling_attrs.generics,
        prefix: darling_attrs.prefix.unwrap_or_default(),
        uses_into: darling_attrs.into,
        strip_option: darling_attrs.strip_option,
        bool: darling_attrs.bool,
        generate_public: generate && darling_attrs.generate_public.unwrap_or(true),
        generate_private: generate && darling_attrs.generate_private.unwrap_or(true),
        generate_delegates: darling_attrs.generate_delegates,
        trace: darling_attrs.trace.unwrap_or(false),
    })
}

fn init_field_def(
    container: &ContainerDef,
    field: &Field,
) -> Result<Option<FieldDef>, SynTokenStream> {
    // Decode the various attribute options.
    let darling_attrs: FieldAttrs = match FromField::from_field(field) {
        Ok(v) => v,
        Err(e) => return Err(e.write_errors()),
    };

    // Check whether this field should generate a setter.
    if darling_attrs.skip {
        return Ok(None);
    }
    let generates = match field.vis {
        Visibility::Public(_) => container.generate_public,
        _ => container.generate_private,
    };
    if !(darling_attrs.generate || generates) {
        return Ok(None);
    }

    // Returns a definition for this field.
    let ident = match &field.ident {
        Some(i) => i.clone(),
        None => panic!("Internal error: init_field_def on wrong item."),
    };
    Ok(Some(FieldDef {
        field_name: ident.clone(),
        field_ty: field.ty.clone(),
        field_doc: if let Visibility::Public(_) = field.vis {
            let doc_str =
                format!("Sets the [`{ident}`](#structfield.{ident}) field of this struct.");
            quote! { #[doc = #doc_str] }
        } else if let Some(x) = darling_attrs.doc {
            quote! { #[doc = #x] }
        } else {
            let attrs = darling_attrs.attrs;
            quote! { #( #attrs )* }
        },
        setter_name: darling_attrs
            .rename
            .unwrap_or_else(|| Ident::new(&format!("{}{}", container.prefix, ident), ident.span())),
        uses_into: darling_attrs.into.unwrap_or(container.uses_into),
        strip_option: darling_attrs.strip_option.unwrap_or(container.strip_option),
        borrow_self: darling_attrs.borrow_self.unwrap_or(container.borrow_self),
        assert_none: darling_attrs.assert_none.unwrap_or(container.assert_none),
        no_change_action: darling_attrs.no_change_action.or(container.no_change_action.clone()),
        postprocess: darling_attrs.postprocess.or(container.postprocess.clone()),
        result: darling_attrs.result.or(container.result.clone()),
        bool: darling_attrs.bool.unwrap_or(container.bool),
        trace: darling_attrs.trace.unwrap_or(container.trace),
    }))
}

fn generate_setter_method(
    input: &DeriveInput,
    container: &ContainerDef,
    def: FieldDef,
    additional_prefix: &Option<String>,
    delegate_toks: &Option<SynTokenStream>,
) -> Result<SynTokenStream, SynTokenStream> {
    let FieldDef { field_name, mut field_ty, field_doc, setter_name, .. } = def;
    let std = &container.std;
    let setter_name = if let Some(additional_prefix) = additional_prefix {
        Ident::new(&format!("{additional_prefix}{setter_name}"), setter_name.span())
    } else {
        setter_name
    };

    let function_name = setter_name.to_token_stream().to_string();
    let trace1;
    let trace2;
    if def.trace {
        trace1 = if def.assert_none {
            if def.bool {
                quote! {
                    tracing::trace!("{:?} Call setter: {:?}, args: true", &self, #function_name);
                }
            } else {
                quote! {
                    tracing::trace!("{:?} Call setter: {:?}, args: {:?}", &self, #function_name, &value);
                }
            }
        } else {
            quote! {}
        };

        trace2 = if def.assert_none {
            quote! {}
        } else if def.bool {
            quote! {
                tracing::trace!("{:?} Call setter: {:?}, args: true", &self, #function_name);
            }
        } else {
            quote! {
                tracing::trace!("{:?} Call setter: {:?}, args: {:?}", &self, #function_name, &vvv);
            }
        };
    } else {
        trace1 = quote! {};
        trace2 = quote! {};
    }

    // Checks if the field is Option.
    let mut is_option_field = false;
    if def.assert_none {
        if let Type::Path(path) = &field_ty {
            let segment = path.path.segments.last().unwrap();
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(path) = &segment.arguments {
                    if let GenericArgument::Type(_tp) = path.args.first().unwrap() {
                        is_option_field = true;
                    }
                }
            }
        }
    }

    // Strips `Option<T>` into `T` if the `strip_option` property is set.
    let mut stripped_option = false;
    if def.strip_option {
        if let Type::Path(path) = &field_ty {
            let segment = path.path.segments.last().unwrap();
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(path) = &segment.arguments {
                    if let GenericArgument::Type(tp) = path.args.first().unwrap() {
                        field_ty = tp.clone();
                        stripped_option = true;
                    }
                }
            }
        }
    }

    // The type the setter accepts.
    let value_ty = if def.uses_into {
        quote! { impl ::#std::convert::Into<#field_ty> + std::fmt::Debug }
    } else {
        quote! { #field_ty }
    };

    // The expression actually stored into the field.
    let mut expr = quote! { value };
    if def.uses_into {
        expr = quote! { #expr.into() };
    }
    if def.bool {
        expr = quote! { true };
    }
    if stripped_option {
        expr = quote! { Some(#expr) };
    }

    // Handle the parameters when bool is enabled.
    let params = if def.bool {
        SynTokenStream::new()
    } else {
        quote! { value: #value_ty }
    };

    let _precheck = if def.assert_none && is_option_field {
        if delegate_toks.is_some() {
            return Err(error(
                input.span(),
                "Can not use delegate and assert_none at the same time.",
            ));
        }
        quote! {
            assert!(self.#field_name.is_none());
        }
    } else {
        quote! {}
    };
    let precheck = if let Some(action) = def.no_change_action {
        if delegate_toks.is_some() {
            return Err(error(
                input.span(),
                "Can not use delegate and no_change_action at the same time.",
            ));
        }

        let q = quote! {
            let vvv = #expr;
            if &self.#field_name == &vvv {
                return #action;
            }
            #_precheck
        };
        expr = quote! { vvv };
        q
    } else {
        let q = quote! {
            let vvv = #expr;
            #_precheck
        };
        expr = quote! { vvv };
        q
    };

    let _result = if let Some(result) = def.result {
        quote! { #result }
    } else if def.borrow_self {
        quote! { &mut Self }
    } else {
        quote! { Self }
    };

    let return_result = if let Some(postprocess) = def.postprocess {
        quote! { #postprocess }
    } else {
        quote! { self }
    };

    // Generates the setter method itself.
    let _container_name = &container.name;
    if let Some(delegate) = delegate_toks {
        let _self = if def.borrow_self {
            quote! { &mut self }
        } else {
            quote! { mut self }
        };

        Ok(quote! {
            #field_doc
            pub fn #setter_name (#_self, #params) -> #_result {
                #trace1
                #precheck
                #trace2
                self.#delegate.#field_name = #expr;
                #return_result
            }
        })
    } else if def.borrow_self {
        Ok(quote! {
            #field_doc
            pub fn #setter_name (&mut self, #params) -> #_result {
                #trace1
                #precheck
                #trace2
                self.#field_name = #expr;
                #return_result
            }
        })
    } else {
        Ok(quote! {
            #field_doc
            pub fn #setter_name (mut self, #params) -> #_result {
                #trace1
                #precheck
                #trace2
                self.#field_name = #expr;
                #return_result
            }
        })
    }
}

fn generate_setters_for(
    input: &DeriveInput,
    data: &DataStruct,
    generics: &Generics,
    ty: SynTokenStream,
    additional_prefix: Option<String>,
    delegate_toks: Option<SynTokenStream>,
) -> Result<SynTokenStream, SynTokenStream> {
    let container_def = init_container_def(input)?;
    let mut toks = SynTokenStream::new();
    for field in &data.fields {
        if let Some(field_def) = init_field_def(&container_def, field)? {
            let method = generate_setter_method(
                input,
                &container_def,
                field_def,
                &additional_prefix,
                &delegate_toks,
            )?;
            toks.extend(method);
        }
    }

    let (generics_bound, _, generics_where) = generics.split_for_impl();
    Ok(quote! {
        impl #generics_bound #ty #generics_where {
            #toks
        }
    })
}

fn generate_setters(input: &DeriveInput, data: &DataStruct) -> Result<TokenStream, TokenStream> {
    let container_def = init_container_def(input)?;
    let mut toks = SynTokenStream::new();
    let container_ty = &container_def.ty;
    toks.extend(generate_setters_for(
        input,
        data,
        &container_def.generics,
        quote! { #container_ty },
        None,
        None,
    ));
    for delegate in container_def.generate_delegates {
        let delegate_ty = delegate.ty;
        toks.extend(generate_setters_for(
            input,
            data,
            &Generics::default(),
            quote! { #delegate_ty },
            delegate.prefix,
            if delegate.field.is_some() && delegate.method.is_some() {
                return Err(error(
                    input.span(),
                    "Cannot set both `method` and `field` on a delegate.",
                )
                .into());
            } else if let Some(field) = &delegate.field {
                Some(quote! { #field })
            } else if let Some(method) = &delegate.method {
                Some(quote! { #method() })
            } else {
                return Err(error(
                    input.span(),
                    "Must set either `method` or `field` on a delegate.",
                )
                .into());
            },
        ));
    }
    Ok(toks.into())
}

#[proc_macro_derive(Setters, attributes(setters))]
pub fn derive_setters(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    if let Data::Struct(data) = &input.data {
        match generate_setters(&input, data) {
            Ok(toks) => toks,
            Err(toks) => toks,
        }
    } else {
        error(input.span(), "`#[derive(Setters)] may only be used on structs.").into()
    }
}
