fn main() {
    let target = std::env::var("TARGET").expect("cargo sets TARGET");
    println!("cargo:rustc-env=OSTRO_TARGET={target}");
    println!("cargo:rerun-if-changed=build.rs");
}
