//! `ostro-conf-inspect <component.wasm>`: prints the component's imports, exports and
//! core instance/module counts as JSON.

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let (Some(path), None) = (args.next(), args.next()) else {
        eprintln!("usage: ostro-conf-inspect <component.wasm>");
        return ExitCode::from(2);
    };
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("read {}: {e}", path.to_string_lossy());
            return ExitCode::FAILURE;
        }
    };
    match ostro_conformance::inspect(&bytes) {
        Ok(info) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&info).expect("serialise ComponentInfo")
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{}: {e}", path.to_string_lossy());
            ExitCode::FAILURE
        }
    }
}
