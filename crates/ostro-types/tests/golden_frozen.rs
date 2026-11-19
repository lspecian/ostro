//! Append-only pin of the frozen golden vectors (plan 05-05, WIT-07).
//!
//! The golden files under `tests/golden/` are the on-disk format contract. Their own tests
//! (`command.rs`, `journal_golden.rs`, `ids.rs`) check that the code still renders them;
//! this test checks that nobody re-blessed them: the first `line_count` lines of each pinned
//! section must hash to the pinned sha256. Lines may be **appended** (new command vectors,
//! new journal kinds at the end of the `event` section, new results/hashes), but no pinned
//! line may change, move or disappear.
//!
//! `journal_v1.txt` is rendered in sections (`#` header, then `event`, `result`, `hash`
//! lines), so a new journal kind lands in the middle of the file; each section is pinned as
//! its own prefix. `commands.txt` and `routing.txt` are pinned as whole-file prefixes.
//!
//! First pinned at the post-05-01 contents (plan 05-05: 21 command vectors, 27 event lines for
//! 25 kinds, the last being `event 97 signal_matched`). **Re-pinned at the Phase 5 freeze
//! (05-20, 2026-11-19)** to the final line counts: no Phase 5 plan appended to the
//! `ostro-types` goldens after 05-05, so their counts and hashes are unchanged, and every line
//! of every file is now pinned. 05-20 also added the meta golden
//! (`crates/ostro-meta/tests/golden/meta_v2.txt`, META_FORMAT 2, D20), pinned per section
//! (`#`, `record`, `command`, `snapshot`) because new meta variants land mid-file. After the
//! freeze, a pin may only be **extended** (larger `line_count` over a file whose old prefix is
//! unchanged), never changed.

use sha2::{Digest, Sha256};

/// Which lines of a golden file a pin covers.
#[derive(Clone, Copy, Debug)]
enum Section {
    /// Every line, in file order.
    All,
    /// Lines whose first space-separated token equals the string, in file order.
    Prefix(&'static str),
}

struct Pin {
    file: &'static str,
    content: &'static str,
    section: Section,
    line_count: usize,
    sha256: &'static str,
}

const COMMANDS: &str = include_str!("golden/commands.txt");
const JOURNAL: &str = include_str!("golden/journal_v1.txt");
const ROUTING: &str = include_str!("golden/routing.txt");
const META: &str = include_str!("../../ostro-meta/tests/golden/meta_v2.txt");

const PINS: &[Pin] = &[
    Pin {
        file: "commands.txt",
        content: COMMANDS,
        section: Section::All,
        line_count: 22, // header + 21 command vectors
        sha256: "de05b21ed437c7d74c9276abc99eb5365aff1468476ad3a8a47474bb0b90c3bc",
    },
    Pin {
        file: "journal_v1.txt",
        content: JOURNAL,
        section: Section::Prefix("#"),
        line_count: 1,
        sha256: "2861c7e66296431d3d670175785fbbb6690ef1b307946a138d3ce78eb8206276",
    },
    Pin {
        file: "journal_v1.txt",
        content: JOURNAL,
        section: Section::Prefix("event"),
        line_count: 27, // 25 kinds (kind 18 has three effect vectors); last = 97 signal_matched
        sha256: "7a82a12d73a3b8ce6920cb7cb3f80211c18d432415f6388ca4af5df7b4241877",
    },
    Pin {
        file: "journal_v1.txt",
        content: JOURNAL,
        section: Section::Prefix("result"),
        line_count: 14,
        sha256: "7dde4cef34125d05b0c765df17ae569fc328af48fc5f09bf67e717e411ce792f",
    },
    Pin {
        file: "journal_v1.txt",
        content: JOURNAL,
        section: Section::Prefix("hash"),
        line_count: 14,
        sha256: "1b46305e33d70a7d995a2f2e0d1e46266ff4b086e311e89eb4bbd2accdcbd624",
    },
    Pin {
        file: "routing.txt",
        content: ROUTING,
        section: Section::All,
        line_count: 49, // header + 48 routing vectors
        sha256: "6464da2ec8394f66e0ea8a52a4af9b188756df35df96c06605eb6324e3a84848",
    },
    Pin {
        file: "meta_v2.txt",
        content: META,
        section: Section::Prefix("#"),
        line_count: 1,
        sha256: "9f1fa5db8cf0db6c156ca6550f8cf6ca72a36fcf25ec6d819402573268c2cb3a",
    },
    Pin {
        file: "meta_v2.txt",
        content: META,
        section: Section::Prefix("record"),
        line_count: 17, // MetaRecord variants 0..=16, last = webhook
        sha256: "9f34a651ec7a96e1943f3a49f5ed125cb8702c7827729516fc7497c7aaf90ee6",
    },
    Pin {
        file: "meta_v2.txt",
        content: META,
        section: Section::Prefix("command"),
        line_count: 4, // MetaCommand variants 0..=3
        sha256: "bea76988136918f6de3b161df4a54521bf54f498ba6c88c9cf206e5e01266de9",
    },
    Pin {
        file: "meta_v2.txt",
        content: META,
        section: Section::Prefix("snapshot"),
        line_count: 2, // empty + full MetaState
        sha256: "bfea23ef414a8bb46d17c4c2b87d8a87a63e8201f8d37ea924c75836bc5ca272",
    },
];

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn section_lines(content: &str, section: Section) -> Vec<&str> {
    content
        .lines()
        .filter(|l| match section {
            Section::All => true,
            Section::Prefix(p) => l.split(' ').next() == Some(p),
        })
        .collect()
}

/// Check one pin against `content`; `Err` describes the violation.
fn check(content: &str, section: Section, line_count: usize, sha256: &str) -> Result<(), String> {
    let lines = section_lines(content, section);
    if lines.len() < line_count {
        return Err(format!(
            "{section:?}: {} lines, pinned prefix is {line_count} (lines removed)",
            lines.len()
        ));
    }
    let mut h = Sha256::new();
    for l in &lines[..line_count] {
        h.update(l.as_bytes());
        h.update(b"\n");
    }
    let got = hex(&h.finalize());
    if got != sha256 {
        return Err(format!(
            "{section:?}: sha256 of the first {line_count} lines is {got}, pinned {sha256} \
             (a frozen golden line changed; append instead of re-blessing)"
        ));
    }
    Ok(())
}

#[test]
fn golden_frozen_prefixes_unchanged() {
    let mut errs = Vec::new();
    for p in PINS {
        let total = section_lines(p.content, p.section).len();
        eprintln!(
            "{} {:?}: {total} lines (pinned prefix {})",
            p.file, p.section, p.line_count
        );
        if let Err(e) = check(p.content, p.section, p.line_count, p.sha256) {
            errs.push(format!("{}: {e}", p.file));
        }
    }
    assert!(errs.is_empty(), "{errs:#?}");
}

#[test]
fn appended_lines_are_allowed() {
    for p in PINS {
        let mut grown = p.content.to_string();
        let extra = match p.section {
            Section::All => "appended\tvector\n".to_string(),
            Section::Prefix(tag) => format!("{tag} 999 appended deadbeef\n"),
        };
        grown.push_str(&extra);
        assert_eq!(
            check(&grown, p.section, p.line_count, p.sha256),
            Ok(()),
            "{}",
            p.file
        );
    }
}

#[test]
fn tampered_copies_are_rejected() {
    for p in PINS {
        let lines = section_lines(p.content, p.section);
        // 1. Change one byte of the last pinned line.
        let victim = lines[p.line_count - 1];
        let mut flipped = victim.to_string();
        let last = flipped.pop().expect("non-empty line");
        flipped.push(if last == '0' { '1' } else { '0' });
        let tampered = p.content.replacen(victim, &flipped, 1);
        assert!(
            check(&tampered, p.section, p.line_count, p.sha256).is_err(),
            "{} {:?}: byte flip not detected",
            p.file,
            p.section
        );
        // 2. Drop the last pinned line.
        let dropped = p.content.replacen(&format!("{victim}\n"), "", 1);
        assert!(
            check(&dropped, p.section, p.line_count, p.sha256).is_err(),
            "{} {:?}: removed line not detected",
            p.file,
            p.section
        );
    }
    // 3. Reordering two pinned lines is a change, too.
    let lines = section_lines(COMMANDS, Section::All);
    let swapped = COMMANDS.replacen(
        &format!("{}\n{}\n", lines[1], lines[2]),
        &format!("{}\n{}\n", lines[2], lines[1]),
        1,
    );
    assert!(check(&swapped, Section::All, 22, PINS[0].sha256).is_err());
}
