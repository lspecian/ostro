//! The `ostro` binary.

use clap::{Parser, Subcommand};

// Link the hard dependencies into the binary from day one.
use ostro_api as _;
use ostro_engine as _;
use ostro_partition as _;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(name = "ostro", version, about = "Ostro durable Wasm workflow engine")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print build information (version, target triple, allocator).
    BuildInfo,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::BuildInfo) => {
            println!(
                "ostro {} target={} allocator=mimalloc",
                env!("CARGO_PKG_VERSION"),
                env!("OSTRO_TARGET")
            );
        }
        None => {
            use clap::CommandFactory;
            Cli::command().print_help().expect("print help");
            println!();
        }
    }
}
