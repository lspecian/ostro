//! # ostro-sdk-macros
//!
//! Attribute macros for the Ostro Rust SDK. Use them through `ostro_sdk`
//! (`use ostro_sdk::prelude::*;`); generated code refers to `::ostro_sdk::...` paths only,
//! so the crate using the macros must depend on `ostro-sdk` under that name.
//!
//! ## `#[workflow]`
//!
//! ```ignore
//! #[workflow(name = "order", version = 1, queue = "payments", run_timeout = "10m")]
//! pub fn order(ctx: &Ctx, input: Order) -> Result<Done, WorkflowError> { ... }
//!
//! ostro_sdk::export!(OrderWorkflow);
//! ```
//!
//! - Required: `name = "..."` (stable workflow name) and `version = N` (`u32`).
//!   Optional: `queue = "..."` (default task queue) and `run_timeout = "<duration>"`.
//! - The function must take exactly two parameters, `(ctx: &Ctx, input: In)`, and return
//!   `Result<Out, E>` where `E: Into<WorkflowError>`. `In` is decoded from the run's JSON
//!   input and `Out` is encoded as the JSON output.
//! - The function is kept as written (still callable). Next to it the macro emits a unit
//!   struct with the function's visibility, named after the function in UpperCamelCase
//!   with a `Workflow` suffix (`fn order` -> `OrderWorkflow`, `fn sdk_basic` ->
//!   `SdkBasicWorkflow`), implementing `ostro_sdk::Workflow`. Register that struct with
//!   `ostro_sdk::export!`.
//!
//! ## `#[step]`
//!
//! ```ignore
//! #[step(retry(max_attempts = 5, initial = "200ms", max = "10s", backoff = 2.0,
//!              non_retryable = ["card_declined"]),
//!        timeout = "5s", fuel = 1_000_000)]
//! fn charge(order: &Order) -> Result<Receipt, StepError> { ... }
//!
//! let receipt = charge(&order)?; // runs or replays the durable step "charge"
//! ```
//!
//! - The function keeps its signature; calling it (`charge(&order)`, no `ctx` argument)
//!   runs the original body through `ostro_sdk::run_step(name, &options, || body)`. On
//!   replay the recorded outcome is returned without running the body.
//! - The body becomes an `FnMut` closure that the runner may call several times
//!   (in-activation retries), so it may borrow parameters but must not move out of them.
//!   The return type must be `Result<T, StepError>` with `T: Serialize + DeserializeOwned`.
//! - Options (all optional):
//!   - `name = "..."`: journaled step name (default: the function name).
//!   - `retry(max_attempts = N, initial = "<duration>", max = "<duration>",
//!     backoff = <float>, non_retryable = ["kind", ...])`. Keys left out default to
//!     `max_attempts = 3`, `initial = "1s"`, `max = "60s"`, `backoff = 2.0`,
//!     `non_retryable = []`. Without `retry(...)` the host default policy applies.
//!   - `timeout = "<duration>"`: step timeout.
//!   - `fuel = N`: fuel budget for the body.
//!
//! Durations are string literals: an integer followed by `ms`, `s`, `m` or `h`
//! (`"200ms"`, `"5s"`, `"10m"`, `"1h"`). They are checked at compile time.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::meta::ParseNestedMeta;
use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::{
    Expr, ExprLit, FnArg, GenericArgument, ItemFn, Lit, LitStr, PathArguments, ReturnType, Type,
    UnOp,
};

const WORKFLOW_REQUIRED: &str = "#[workflow] requires name = \"...\" and version = N";

/// Declare a durable workflow. See the [crate docs](crate).
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_workflow(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declare a durable step. See the [crate docs](crate).
#[proc_macro_attribute]
pub fn step(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_step(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

// ---------------------------------------------------------------------------------------
// Shared literal helpers
// ---------------------------------------------------------------------------------------

/// Parse a duration literal (`"200ms"`, `"5s"`, `"10m"`, `"1h"`) into milliseconds.
fn parse_duration_ms(s: &str) -> Result<u64, String> {
    let digits_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(digits_end);
    let bad = || {
        format!(
            "invalid duration \"{s}\": expected an integer followed by ms, s, m or h \
             (e.g. \"200ms\", \"5s\", \"10m\")"
        )
    };
    if num.is_empty() {
        return Err(bad());
    }
    let mult: u64 = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => return Err(bad()),
    };
    num.parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .ok_or_else(|| format!("duration \"{s}\" overflows u64 milliseconds"))
}

fn duration_lit(lit: &LitStr) -> syn::Result<u64> {
    parse_duration_ms(&lit.value()).map_err(|m| syn::Error::new(lit.span(), m))
}

/// `key = <expr>` value of a nested meta item.
fn meta_expr(meta: &ParseNestedMeta) -> syn::Result<Expr> {
    meta.value()?.parse::<Expr>()
}

fn expect_str(meta: &ParseNestedMeta, key: &str) -> syn::Result<LitStr> {
    match meta_expr(meta)? {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s),
        other => Err(syn::Error::new(
            other.span(),
            format!("`{key}` expects a string literal"),
        )),
    }
}

/// A non-negative integer literal parsed as `N`.
fn expect_uint<N>(meta: &ParseNestedMeta, key: &str, ty: &str) -> syn::Result<N>
where
    N: std::str::FromStr,
    N::Err: std::fmt::Display,
{
    let expr = meta_expr(meta)?;
    match &expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(i), ..
        }) => i
            .base10_parse::<N>()
            .map_err(|_| syn::Error::new(i.span(), format!("`{key}` must fit in {ty}"))),
        Expr::Unary(u) if matches!(u.op, UnOp::Neg(_)) => Err(syn::Error::new(
            expr.span(),
            format!("`{key}` must be a non-negative integer ({ty})"),
        )),
        _ => Err(syn::Error::new(
            expr.span(),
            format!("`{key}` expects an integer literal ({ty})"),
        )),
    }
}

fn duplicate(meta: &ParseNestedMeta, key: &str) -> syn::Error {
    meta.error(format!("duplicate `{key}` option"))
}

// ---------------------------------------------------------------------------------------
// #[workflow]
// ---------------------------------------------------------------------------------------

#[derive(Default)]
struct WorkflowArgs {
    name: Option<LitStr>,
    version: Option<u32>,
    queue: Option<LitStr>,
    run_timeout_ms: Option<u64>,
}

fn parse_workflow_args(attr: TokenStream2) -> syn::Result<WorkflowArgs> {
    let mut args = WorkflowArgs::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            if args.name.is_some() {
                return Err(duplicate(&meta, "name"));
            }
            let s = expect_str(&meta, "name")?;
            if s.value().is_empty() {
                return Err(syn::Error::new(s.span(), "workflow name must not be empty"));
            }
            args.name = Some(s);
        } else if meta.path.is_ident("version") {
            if args.version.is_some() {
                return Err(duplicate(&meta, "version"));
            }
            args.version = Some(expect_uint::<u32>(&meta, "version", "u32")?);
        } else if meta.path.is_ident("queue") {
            if args.queue.is_some() {
                return Err(duplicate(&meta, "queue"));
            }
            args.queue = Some(expect_str(&meta, "queue")?);
        } else if meta.path.is_ident("run_timeout") {
            if args.run_timeout_ms.is_some() {
                return Err(duplicate(&meta, "run_timeout"));
            }
            args.run_timeout_ms = Some(duration_lit(&expect_str(&meta, "run_timeout")?)?);
        } else {
            return Err(meta.error(format!(
                "unknown #[workflow] option `{}` (expected name, version, queue, run_timeout)",
                path_str(&meta.path)
            )));
        }
        Ok(())
    });
    parser.parse2(attr)?;
    Ok(args)
}

fn path_str(p: &syn::Path) -> String {
    quote!(#p).to_string().replace(' ', "")
}

/// `order_items` -> `OrderItemsWorkflow`.
fn workflow_struct_name(fn_name: &str) -> String {
    let mut out = String::new();
    for part in fn_name
        .trim_start_matches("r#")
        .split('_')
        .filter(|p| !p.is_empty())
    {
        let mut cs = part.chars();
        if let Some(c) = cs.next() {
            out.extend(c.to_uppercase());
            out.push_str(cs.as_str());
        }
    }
    out.push_str("Workflow");
    out
}

/// `T` of a `Result<T, E>` return type.
fn result_ok_type(ret: &ReturnType) -> Option<&Type> {
    let ReturnType::Type(_, ty) = ret else {
        return None;
    };
    let Type::Path(tp) = &**ty else {
        return None;
    };
    let last = tp.path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    match args.args.first()? {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    }
}

fn expand_workflow(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let args = parse_workflow_args(attr)?;
    let func: ItemFn = syn::parse2(item)?;
    let (Some(name), Some(version)) = (args.name, args.version) else {
        return Err(syn::Error::new(Span::call_site(), WORKFLOW_REQUIRED));
    };

    let sig = &func.sig;
    if let Some(a) = &sig.asyncness {
        return Err(syn::Error::new(
            a.span(),
            "#[workflow] functions must not be async (use ctx ops and join!/select!)",
        ));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "#[workflow] functions must not be generic",
        ));
    }
    let params: Vec<&FnArg> = sig.inputs.iter().collect();
    let input_ty = match params.as_slice() {
        [FnArg::Typed(_), FnArg::Typed(input)] => &*input.ty,
        _ => {
            return Err(syn::Error::new(
                sig.paren_token.span.join(),
                "#[workflow] functions take exactly two parameters: (ctx: &Ctx, input: In)",
            ));
        }
    };
    let Some(output_ty) = result_ok_type(&sig.output) else {
        return Err(syn::Error::new(
            match &sig.output {
                ReturnType::Default => sig.ident.span(),
                ReturnType::Type(_, t) => t.span(),
            },
            "#[workflow] functions must return Result<Out, E> where E: Into<WorkflowError>",
        ));
    };

    let vis = &func.vis;
    let fn_ident = &sig.ident;
    let struct_ident = format_ident!("{}", workflow_struct_name(&fn_ident.to_string()));
    let queue = match &args.queue {
        Some(q) => quote!(::core::option::Option::Some(#q)),
        None => quote!(::core::option::Option::None),
    };
    let run_timeout = match args.run_timeout_ms {
        Some(ms) => quote!(::core::option::Option::Some(#ms)),
        None => quote!(::core::option::Option::None),
    };
    let doc = format!(
        "Workflow `{}` version {} (generated by `#[workflow]` on [`{}`]).",
        name.value(),
        version,
        fn_ident
    );

    Ok(quote! {
        #func

        #[doc = #doc]
        #[allow(dead_code)]
        #[derive(Debug, Clone, Copy, Default)]
        #vis struct #struct_ident;

        impl ::ostro_sdk::Workflow for #struct_ident {
            const NAME: &'static str = #name;
            const VERSION: u32 = #version;
            const DEFAULT_QUEUE: ::core::option::Option<&'static str> = #queue;
            const RUN_TIMEOUT_MS: ::core::option::Option<u64> = #run_timeout;
            type Input = #input_ty;
            type Output = #output_ty;

            fn run(
                ctx: &::ostro_sdk::Ctx,
                input: Self::Input,
            ) -> ::core::result::Result<Self::Output, ::ostro_sdk::WorkflowError> {
                #fn_ident(ctx, input).map_err(::core::convert::Into::into)
            }
        }
    })
}

// ---------------------------------------------------------------------------------------
// #[step]
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct RetryArgs {
    max_attempts: u32,
    initial_ms: u64,
    max_ms: u64,
    backoff: f64,
    non_retryable: Vec<String>,
}

impl Default for RetryArgs {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_ms: 1_000,
            max_ms: 60_000,
            backoff: 2.0,
            non_retryable: Vec::new(),
        }
    }
}

#[derive(Debug, Default, PartialEq)]
struct StepArgs {
    name: Option<String>,
    retry: Option<RetryArgs>,
    timeout_ms: Option<u64>,
    fuel: Option<u64>,
}

fn parse_retry(meta: &ParseNestedMeta) -> syn::Result<RetryArgs> {
    let mut r = RetryArgs::default();
    let mut seen: Vec<String> = Vec::new();
    meta.parse_nested_meta(|m| {
        let key = path_str(&m.path);
        if seen.contains(&key) {
            return Err(duplicate(&m, &key));
        }
        match key.as_str() {
            "max_attempts" => r.max_attempts = expect_uint::<u32>(&m, "max_attempts", "u32")?,
            "initial" => r.initial_ms = duration_lit(&expect_str(&m, "initial")?)?,
            "max" => r.max_ms = duration_lit(&expect_str(&m, "max")?)?,
            "backoff" => {
                let expr = meta_expr(&m)?;
                r.backoff = match &expr {
                    Expr::Lit(ExprLit {
                        lit: Lit::Float(f), ..
                    }) => f.base10_parse::<f64>()?,
                    Expr::Lit(ExprLit {
                        lit: Lit::Int(i), ..
                    }) => i.base10_parse::<u32>()? as f64,
                    _ => {
                        return Err(syn::Error::new(
                            expr.span(),
                            "`backoff` expects a number literal (e.g. 2.0)",
                        ));
                    }
                };
                if !(r.backoff.is_finite() && r.backoff >= 1.0) {
                    return Err(syn::Error::new(expr.span(), "`backoff` must be >= 1.0"));
                }
            }
            "non_retryable" => {
                let expr = meta_expr(&m)?;
                let Expr::Array(arr) = &expr else {
                    return Err(syn::Error::new(
                        expr.span(),
                        "`non_retryable` expects an array of string literals, e.g. [\"fatal\"]",
                    ));
                };
                r.non_retryable = arr
                    .elems
                    .iter()
                    .map(|e| match e {
                        Expr::Lit(ExprLit {
                            lit: Lit::Str(s), ..
                        }) => Ok(s.value()),
                        other => Err(syn::Error::new(
                            other.span(),
                            "`non_retryable` entries must be string literals",
                        )),
                    })
                    .collect::<syn::Result<_>>()?;
            }
            _ => {
                return Err(m.error(format!(
                    "unknown retry option `{key}` (expected max_attempts, initial, max, \
                     backoff, non_retryable)"
                )));
            }
        }
        seen.push(key);
        Ok(())
    })?;
    Ok(r)
}

fn parse_step_args(attr: TokenStream2) -> syn::Result<StepArgs> {
    let mut args = StepArgs::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            if args.name.is_some() {
                return Err(duplicate(&meta, "name"));
            }
            let s = expect_str(&meta, "name")?;
            if s.value().is_empty() {
                return Err(syn::Error::new(s.span(), "step name must not be empty"));
            }
            args.name = Some(s.value());
        } else if meta.path.is_ident("retry") {
            if args.retry.is_some() {
                return Err(duplicate(&meta, "retry"));
            }
            if meta.input.is_empty() || meta.input.peek(syn::Token![,]) {
                return Err(meta.error(
                    "`retry` expects a list, e.g. retry(max_attempts = 3, initial = \"1s\")",
                ));
            }
            args.retry = Some(parse_retry(&meta)?);
        } else if meta.path.is_ident("timeout") {
            if args.timeout_ms.is_some() {
                return Err(duplicate(&meta, "timeout"));
            }
            args.timeout_ms = Some(duration_lit(&expect_str(&meta, "timeout")?)?);
        } else if meta.path.is_ident("fuel") {
            if args.fuel.is_some() {
                return Err(duplicate(&meta, "fuel"));
            }
            args.fuel = Some(expect_uint::<u64>(&meta, "fuel", "u64")?);
        } else {
            return Err(meta.error(format!(
                "unknown #[step] option `{}` (expected name, retry, timeout, fuel)",
                path_str(&meta.path)
            )));
        }
        Ok(())
    });
    parser.parse2(attr)?;
    Ok(args)
}

fn step_options_tokens(args: &StepArgs) -> TokenStream2 {
    let retry = match &args.retry {
        Some(r) => {
            let RetryArgs {
                max_attempts,
                initial_ms,
                max_ms,
                backoff,
                non_retryable,
            } = r;
            quote! {
                ::core::option::Option::Some(::ostro_sdk::RetryPolicy {
                    max_attempts: #max_attempts,
                    initial_interval: ::core::time::Duration::from_millis(#initial_ms),
                    max_interval: ::core::time::Duration::from_millis(#max_ms),
                    backoff_coefficient: #backoff,
                    non_retryable_kinds: ::std::vec![
                        #(::std::string::String::from(#non_retryable)),*
                    ],
                })
            }
        }
        None => quote!(::core::option::Option::None),
    };
    let timeout = match args.timeout_ms {
        Some(ms) => {
            quote!(::core::option::Option::Some(::core::time::Duration::from_millis(#ms)))
        }
        None => quote!(::core::option::Option::None),
    };
    let fuel = match args.fuel {
        Some(f) => quote!(::core::option::Option::Some(#f)),
        None => quote!(::core::option::Option::None),
    };
    quote! {
        ::ostro_sdk::StepOptions {
            retry: #retry,
            timeout: #timeout,
            fuel: #fuel,
        }
    }
}

fn expand_step(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let args = parse_step_args(attr)?;
    let mut func: ItemFn = syn::parse2(item)?;
    if let Some(a) = &func.sig.asyncness {
        return Err(syn::Error::new(
            a.span(),
            "#[step] functions must not be async",
        ));
    }
    let ReturnType::Type(_, ret_ty) = &func.sig.output else {
        return Err(syn::Error::new(
            func.sig.ident.span(),
            "#[step] functions must return Result<T, StepError>",
        ));
    };
    let ret_ty = ret_ty.clone();
    let name = args.name.clone().unwrap_or_else(|| {
        func.sig
            .ident
            .to_string()
            .trim_start_matches("r#")
            .to_string()
    });
    let opts = step_options_tokens(&args);
    let body = &func.block;
    let new_body: syn::Block = syn::parse2(quote! {{
        let __ostro_step_options: ::ostro_sdk::StepOptions = #opts;
        let __ostro_step_body = || -> #ret_ty #body;
        ::ostro_sdk::run_step(#name, &__ostro_step_options, __ostro_step_body)
    }})?;
    *func.block = new_body;
    Ok(quote!(#func))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration_ms("200ms"), Ok(200));
        assert_eq!(parse_duration_ms("5s"), Ok(5_000));
        assert_eq!(parse_duration_ms("10m"), Ok(600_000));
        assert_eq!(parse_duration_ms("2h"), Ok(7_200_000));
        assert_eq!(parse_duration_ms("0s"), Ok(0));
        for bad in [
            "",
            "5",
            "s",
            "5x",
            "5 s",
            "1.5s",
            "-1s",
            "10 minutes",
            "5sec",
        ] {
            assert!(parse_duration_ms(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(parse_duration_ms("99999999999999999999h").is_err());
    }

    #[test]
    fn struct_names() {
        assert_eq!(workflow_struct_name("order"), "OrderWorkflow");
        assert_eq!(workflow_struct_name("sdk_basic_v2"), "SdkBasicV2Workflow");
        assert_eq!(workflow_struct_name("_x__y"), "XYWorkflow");
    }

    #[test]
    fn step_args_full() {
        let a = parse_step_args(quote! {
            name = "pay",
            retry(max_attempts = 5, initial = "200ms", max = "10s", backoff = 2.5,
                  non_retryable = ["card_declined", "fraud"]),
            timeout = "5s",
            fuel = 1_000_000
        })
        .unwrap();
        assert_eq!(
            a,
            StepArgs {
                name: Some("pay".into()),
                retry: Some(RetryArgs {
                    max_attempts: 5,
                    initial_ms: 200,
                    max_ms: 10_000,
                    backoff: 2.5,
                    non_retryable: vec!["card_declined".into(), "fraud".into()],
                }),
                timeout_ms: Some(5_000),
                fuel: Some(1_000_000),
            }
        );
    }

    #[test]
    fn step_args_defaults() {
        assert_eq!(parse_step_args(quote!()).unwrap(), StepArgs::default());
        let a = parse_step_args(quote!(retry(max_attempts = 4, backoff = 3))).unwrap();
        assert_eq!(
            a.retry,
            Some(RetryArgs {
                max_attempts: 4,
                backoff: 3.0,
                ..RetryArgs::default()
            })
        );
    }

    #[test]
    fn step_args_errors() {
        let err = |t: TokenStream2| parse_step_args(t).unwrap_err().to_string();
        assert!(err(quote!(timeout = "5x")).contains("invalid duration \"5x\""));
        assert!(err(quote!(retry(attempts = 3))).contains("unknown retry option `attempts`"));
        assert!(err(quote!(retry)).contains("`retry` expects a list"));
        assert!(err(quote!(retry(max_attempts = 1, max_attempts = 2))).contains("duplicate"));
        assert!(err(quote!(fuel = -1)).contains("non-negative"));
        assert!(err(quote!(retry(backoff = 0.5))).contains(">= 1.0"));
        assert!(err(quote!(retry(non_retryable = "x"))).contains("array of string"));
        assert!(err(quote!(speed = 1)).contains("unknown #[step] option `speed`"));
    }

    #[test]
    fn step_expansion_wraps_body_in_run_step() {
        let out = expand_step(
            quote!(retry(max_attempts = 2), fuel = 9),
            quote! { fn charge(o: &Order) -> Result<u64, StepError> { Ok(o.amount) } },
        )
        .unwrap()
        .to_string();
        assert!(
            out.contains(":: ostro_sdk :: run_step (\"charge\""),
            "{out}"
        );
        assert!(out.contains("max_attempts : 2u32"), "{out}");
        assert!(out.contains("Some (9u64)"), "{out}");
        assert!(out.contains("fn charge (o : & Order)"), "{out}");
    }

    #[test]
    fn workflow_args_and_errors() {
        let a = parse_workflow_args(quote!(
            name = "order",
            version = 3,
            queue = "q",
            run_timeout = "1h"
        ))
        .unwrap();
        assert_eq!(a.name.unwrap().value(), "order");
        assert_eq!(a.version, Some(3));
        assert_eq!(a.queue.unwrap().value(), "q");
        assert_eq!(a.run_timeout_ms, Some(3_600_000));

        let item = quote! { fn f(ctx: &Ctx, i: u32) -> Result<u32, WorkflowError> { Ok(i) } };
        let e = expand_workflow(quote!(version = 1), item.clone()).unwrap_err();
        assert_eq!(e.to_string(), WORKFLOW_REQUIRED);
        let e = expand_workflow(quote!(name = "f"), item.clone()).unwrap_err();
        assert_eq!(e.to_string(), WORKFLOW_REQUIRED);
        let out = expand_workflow(quote!(name = "f", version = 1), item)
            .unwrap()
            .to_string();
        assert!(out.contains("struct FWorkflow"), "{out}");
        assert!(out.contains("type Input = u32"), "{out}");
    }
}
