//! `ostro init` (CLI-03): scaffold a workflow project from a template.
//!
//! Plan 10-03 fixed the flag set; plan 10-08 implements it on
//! `ostro_build::templates::render` and `ostro_build::sdk_bundle::materialize`.

use std::path::{Path, PathBuf};

use clap::{Args, ValueEnum};
use ostro_build::project::{Lang, ProjectError};
use ostro_build::{sdk_bundle, templates};
use serde_json::json;

use crate::output::{Outcome, local_json};

/// Project language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InitLang {
    Rust,
    Python,
    Ts,
}

impl From<InitLang> for Lang {
    fn from(l: InitLang) -> Lang {
        match l {
            InitLang::Rust => Lang::Rust,
            InitLang::Python => Lang::Python,
            InitLang::Ts => Lang::Ts,
        }
    }
}

/// `ostro init [DIR] --lang <rust|python|ts> [--name N] [--force] [--json]`.
#[derive(Debug, Clone, Args)]
pub struct InitArgs {
    /// Project directory [default: .].
    #[arg(value_name = "DIR")]
    pub dir: Option<PathBuf>,
    /// Project language.
    #[arg(long, value_enum)]
    pub lang: Option<InitLang>,
    /// Project (module) name [default: the directory name].
    #[arg(long)]
    pub name: Option<String>,
    /// Write into a non-empty directory, overwriting template files.
    #[arg(long)]
    pub force: bool,
    /// Print an `ostro.init.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// Run `ostro init`: print the result (stdout) or one error line (stderr).
pub fn run(a: &InitArgs) -> Outcome {
    let (outcome, res) = execute(a);
    match res {
        Ok(text) => println!("{text}"),
        Err(line) => eprintln!("ostro: error: {line}"),
    }
    outcome
}

/// Scaffold the project; returns the outcome and the stdout text (success) or the error
/// line without the `ostro: error: ` prefix (failure). Usage errors (no `--lang`, a bad
/// name, a non-empty directory without `--force`) are [`Outcome::Usage`] (exit 2).
pub fn execute(a: &InitArgs) -> (Outcome, Result<String, String>) {
    let Some(lang) = a.lang.map(Lang::from) else {
        return usage("--lang is required (rust, python or ts)".to_owned());
    };
    let given = a.dir.clone().unwrap_or_else(|| PathBuf::from("."));
    let dir = match absolute(&given) {
        Ok(d) => d,
        Err(e) => return (Outcome::Error, Err(format!("{}: {e}", given.display()))),
    };
    let name = match &a.name {
        Some(n) => n.clone(),
        None => match dir.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_owned(),
            None => {
                return usage(format!(
                    "cannot derive a project name from {}; pass --name",
                    dir.display()
                ));
            }
        },
    };
    let written = match templates::render(lang, &name, &dir, a.force) {
        Ok(w) => w,
        Err(e @ ProjectError::InvalidName { .. }) if a.name.is_none() => {
            return usage(format!("{e} (the directory name; pass --name)"));
        }
        Err(e) if e.is_usage() => return usage(e.to_string()),
        Err(e) => return (Outcome::Error, Err(e.to_string())),
    };
    let sdk_dir = dir.join("target").join("ostro").join("sdk");
    if let Err(e) = sdk_bundle::materialize(&sdk_dir) {
        return (
            Outcome::Error,
            Err(format!(
                "cannot materialise the SDK bundle into {}: {e}",
                sdk_dir.display()
            )),
        );
    }
    let files: Vec<String> = written
        .iter()
        .map(|p| {
            p.strip_prefix(&dir)
                .unwrap_or(p)
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    let id = sdk_bundle::bundle_id();
    if a.json {
        let body = json!({
            "dir": dir.to_string_lossy(),
            "lang": lang.as_str(),
            "name": name,
            "files": files,
            "sdk_bundle": id,
        });
        return (Outcome::Ok, Ok(local_json("init", &body)));
    }
    (Outcome::Ok, Ok(human(lang, &name, &given, &files, &id)))
}

fn usage(line: String) -> (Outcome, Result<String, String>) {
    (Outcome::Usage, Err(line))
}

/// `dir` made absolute (canonical when it exists, so `.` names the current directory).
fn absolute(dir: &Path) -> std::io::Result<PathBuf> {
    if dir.exists() {
        dir.canonicalize()
    } else {
        std::path::absolute(dir)
    }
}

fn human(lang: Lang, name: &str, given: &Path, files: &[String], id: &str) -> String {
    let mut s = format!("Created {lang} project `{name}` in {}\n", given.display());
    for f in files {
        s.push_str(&format!("  {f}\n"));
    }
    s.push_str(&format!(
        "  target/ostro/sdk/ (SDK bundle {id})\n\nNext steps:\n"
    ));
    if given != Path::new(".") {
        s.push_str(&format!("  cd {}\n", given.display()));
    }
    let test = match lang {
        Lang::Rust => "cargo test",
        Lang::Python => "pytest",
        Lang::Ts => "npm test",
    };
    s.push_str(&format!(
        "  ostro dev --watch                                  # local server, redeploys on save\n  \
         ostro deploy                                       # build and deploy\n  \
         ostro run hello --input '{{\"name\":\"ada\"}}' --wait\n  \
         {test:<50} # run the tests (see README.md)"
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lang_conversion() {
        assert_eq!(Lang::from(InitLang::Rust), Lang::Rust);
        assert_eq!(Lang::from(InitLang::Python), Lang::Python);
        assert_eq!(Lang::from(InitLang::Ts), Lang::Ts);
    }

    #[test]
    fn missing_lang_is_usage() {
        let a = InitArgs {
            dir: None,
            lang: None,
            name: None,
            force: false,
            json: false,
        };
        let (o, r) = execute(&a);
        assert_eq!(o, Outcome::Usage);
        assert!(r.unwrap_err().contains("--lang"));
    }
}
