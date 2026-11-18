//! `#[workflow]` / `#[step]` (stub).
use proc_macro::TokenStream;

/// Stub.
#[proc_macro_attribute]
pub fn workflow(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Stub.
#[proc_macro_attribute]
pub fn step(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
