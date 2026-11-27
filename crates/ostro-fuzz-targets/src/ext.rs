//! Fuzz target `ext_tlv` (14-07, SEC-05): the meta `ext` TLV codec (`ostro_meta::ext`).
//!
//! Input: `selector ‖ ext bytes`. The selector picks the tag and value used for the
//! put/remove checks; the rest is treated as a stored `ext`.
//!
//! Invariants:
//! - `ext_entries`, `ext_validate`, `ext_get`, `ext_put`, `ext_remove` and every typed getter
//!   never panic, whatever the bytes;
//! - a well-formed `ext` re-encodes byte for byte (canonical varints, strictly increasing
//!   tags), `ext_get` finds every entry, `ext_put` / `ext_remove` keep every other entry
//!   (unknown tags included) byte for byte and yield a well-formed `ext`;
//! - a malformed `ext` is rejected by every mutator (nothing is "repaired");
//! - a typed value that decodes re-encodes to a value that decodes to the same value.

use ostro_meta::ext::{self as x, ExtEntry};
use ostro_meta::{ext_entries, ext_get, ext_put, ext_remove, ext_validate};

/// Fuzz entry point.
pub fn ext_tlv(data: &[u8]) {
    let Some((&sel, ext)) = data.split_first() else {
        return;
    };
    // Tag for the mutations: a known tag or a small/huge unknown one.
    const TAGS: [u32; 8] = [1, 15, 80, 99, 113, 117, 4096, u32::MAX];
    let tag = TAGS[usize::from(sel & 7)];
    let val: &[u8] = &ext[..ext.len().min(usize::from(sel >> 3))];

    match ext_entries(ext) {
        Ok(entries) => well_formed(ext, &entries, tag, val),
        Err(e) => {
            assert_eq!(ext_validate(ext), Err(e.clone()), "validate disagrees");
            assert!(ext_put(ext, tag, val).is_err(), "put repaired {e}");
            assert!(ext_remove(ext, tag).is_err(), "remove repaired {e}");
            assert_eq!(ext_get(ext, tag), None);
        }
    }
    typed(ext);
}

fn encode(entries: &[ExtEntry<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        out = ext_put(&out, e.tag, e.value).expect("put of a decoded entry");
    }
    out
}

fn well_formed(ext: &[u8], entries: &[ExtEntry<'_>], tag: u32, val: &[u8]) {
    assert_eq!(ext_validate(ext), Ok(()));
    for w in entries.windows(2) {
        assert!(w[0].tag < w[1].tag, "tags not strictly increasing");
    }
    assert!(entries.iter().all(|e| e.tag != 0), "tag 0 accepted");
    assert_eq!(encode(entries), ext, "re-encoding is not canonical");
    for e in entries {
        assert_eq!(ext_get(ext, e.tag), Some(e.value));
    }

    let others = |es: &[ExtEntry<'_>]| -> Vec<(u32, Vec<u8>)> {
        es.iter()
            .filter(|e| e.tag != tag)
            .map(|e| (e.tag, e.value.to_vec()))
            .collect()
    };

    let put = ext_put(ext, tag, val).expect("put on a valid ext");
    let after = ext_entries(&put).expect("put yields a valid ext");
    assert_eq!(ext_get(&put, tag), Some(val));
    assert_eq!(others(entries), others(&after), "put changed another entry");

    let removed = ext_remove(ext, tag).expect("remove on a valid ext");
    let rem = ext_entries(&removed).expect("remove yields a valid ext");
    assert_eq!(ext_get(&removed, tag), None);
    assert_eq!(
        others(entries),
        others(&rem),
        "remove changed another entry"
    );
    assert_eq!(
        ext_remove(&put, tag).expect("remove after put"),
        removed,
        "put then remove differs from remove"
    );
    if ext_get(ext, tag).is_none() {
        assert_eq!(removed, ext, "removing an absent tag changed the bytes");
    }
}

/// Typed getters: decode → put into an empty ext → decode equals.
fn typed(ext: &[u8]) {
    macro_rules! rt {
        ($get:path, $put:path) => {
            if let Ok(Some(v)) = $get(ext) {
                let again = $put(&[], &v).expect("typed put");
                assert_eq!($get(&again).expect("typed re-get"), Some(v));
            }
        };
    }
    rt!(x::get_election_info, x::put_election_info);
    rt!(x::get_node_version, x::put_node_version);
    rt!(x::get_member_state, x::put_member_state);
    rt!(x::get_cluster_version, x::put_cluster_version);
    rt!(x::get_replication_policy, x::put_replication_policy);
    rt!(x::get_reassignment, x::put_reassignment);
    rt!(x::get_wit_requirement, x::put_wit_requirement);
    rt!(x::get_retired, x::put_retired);
    rt!(x::get_api_token_meta, x::put_api_token_meta);
    rt!(x::get_module_signatures, x::put_module_signatures);
    rt!(x::get_module_uploaders, x::put_module_uploaders);
    rt!(x::get_security_policy, x::put_security_policy);
    rt!(x::get_tenant_security, x::put_tenant_security);
    rt!(x::get_secret_times, x::put_secret_times);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_inputs() {
        ext_tlv(&[0]);
        ext_tlv(&[0x13, 0x0f, 0x01, 0x00]);
        ext_tlv(&[0x13, 0x0f, 0x01, 0x00, 0x0f, 0x00]); // not increasing
        ext_tlv(&[0x13, 0x80, 0x00, 0x00]); // non-canonical varint
        ext_tlv(&[0xff, 0x00, 0x00]); // tag 0
    }
}
