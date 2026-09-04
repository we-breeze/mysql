use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, GenericArgument, LitStr, PathArguments, Type, parse_macro_input,
    parse_quote,
};

#[proc_macro_derive(FromMysqlRow, attributes(mysql))]
pub fn derive_from_mysql_row(input: TokenStream) -> TokenStream {
    expand(parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(mut input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = input.ident;
    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "FromMysqlRow requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "FromMysqlRow can only be derived for structs",
            ));
        }
    };

    let mut decoders = Vec::with_capacity(fields.len());
    let where_clause = input.generics.make_where_clause();
    for field in fields {
        let ident = field.ident.expect("named fields always have identifiers");
        let column = column_name(&field.attrs, &ident.to_string())?;
        if let Some(inner) = option_inner(&field.ty) {
            where_clause
                .predicates
                .push(parse_quote!(#inner: ::brz_mysql::FromMysqlValue));
            decoders.push(quote! {
                #ident: row.get::<#inner>(#column)?
            });
        } else {
            let ty = field.ty;
            where_clause
                .predicates
                .push(parse_quote!(#ty: ::brz_mysql::FromMysqlValue));
            decoders.push(quote! {
                #ident: row.get_required::<#ty>(#column)?
            });
        }
    }

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::brz_mysql::FromMysqlRow for #name #type_generics #where_clause {
            fn from_mysql_row(row: ::brz_mysql::MysqlRow) -> ::brz_mysql::MysqlResult<Self> {
                Ok(Self {
                    #(#decoders,)*
                })
            }
        }
    })
}

fn column_name(attributes: &[syn::Attribute], default: &str) -> syn::Result<LitStr> {
    let mut column = LitStr::new(default, proc_macro2::Span::call_site());
    for attribute in attributes {
        if !attribute.path().is_ident("mysql") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                column = meta.value()?.parse()?;
                Ok(())
            } else {
                Err(meta.error("supported MySQL field attribute: rename = column"))
            }
        })?;
    }
    Ok(column)
}

fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    match arguments.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}
