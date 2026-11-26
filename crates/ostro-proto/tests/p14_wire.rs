//! Phase 14 (14-02) wire contracts: every message of `ostro/api/v1/security.proto`
//! round-trips, the appended fields (`UploadModuleRequest.signatures`,
//! `ListRunsRequest.tenant`) keep old encodings decodable, the services exist under the
//! reserved names (D-P14-29), and token bearers stay out of `Debug`.

use std::collections::HashMap;

use ostro_proto::api::v1 as api;
use prost::Message;

/// Field numbers allocated by 14-02 on existing messages (appended after the highest one).
const UPLOAD_SIGNATURES_FIELD: u32 = 6;
const LIST_RUNS_TENANT_FIELD: u32 = 12;

fn rt<M: Message + Default + PartialEq + std::fmt::Debug>(m: M) {
    let bytes = m.encode_to_vec();
    let back = M::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, m);
}

fn spec() -> api::TenantSpec {
    api::TenantSpec {
        id: "acme".into(),
        weight: Some(3),
        max_concurrency: Some(10),
        fuel_per_s: Some(1_000_000),
        fuel_burst: Some(5_000_000),
        memory_bytes: Some(64 << 20),
        event_rate: Some(api::RateSpec {
            limit: 100,
            period_ms: 1000,
            burst: 10,
        }),
    }
}

fn token() -> api::ApiToken {
    api::ApiToken {
        id: "otk_aaaaaaaaaaaaaaaa".into(),
        name: "ci".into(),
        description: "deploys".into(),
        scopes: vec!["deploy".into(), "read".into()],
        tenant: Some("acme".into()),
        created_at_ms: 1,
        expires_at_ms: Some(2),
        created_by: "dev".into(),
        rotated_from: None,
    }
}

fn key() -> api::TrustedKey {
    api::TrustedKey {
        alg: "minisign".into(),
        key_id: "0123456789abcdef".into(),
        public_key: "RWQ...".into(),
        comment: "release key".into(),
        tenant: None,
    }
}

#[test]
fn tenant_messages_round_trip() {
    let t = api::Tenant {
        spec: Some(spec()),
        suspended: true,
        created_at_ms: 7,
    };
    rt(t.clone());
    rt(api::TenantSpec {
        id: "b".into(),
        ..Default::default()
    });
    rt(api::CreateTenantRequest { spec: Some(spec()) });
    rt(api::GetTenantRequest { id: "acme".into() });
    rt(api::ListTenantsRequest {});
    rt(api::ListTenantsResponse {
        tenants: vec![t.clone(), t],
    });
    rt(api::UpdateTenantRequest {
        id: "acme".into(),
        weight: Some(api::QuotaU32 { value: Some(4) }),
        // Present with no value = clear the quota.
        max_concurrency: Some(api::QuotaU32 { value: None }),
        fuel_per_s: Some(api::QuotaU64 { value: Some(9) }),
        fuel_burst: None,
        memory_bytes: Some(api::QuotaU64 { value: None }),
        event_rate: Some(api::QuotaRate {
            value: Some(api::RateSpec {
                limit: 1,
                period_ms: 1,
                burst: 0,
            }),
        }),
    });
    // "Clear" and "keep" stay distinguishable on the wire.
    let clear = api::UpdateTenantRequest {
        id: "a".into(),
        weight: Some(api::QuotaU32 { value: None }),
        ..Default::default()
    };
    let back = api::UpdateTenantRequest::decode(clear.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(back.weight, Some(api::QuotaU32 { value: None }));
    assert_eq!(back.max_concurrency, None);
    rt(api::SuspendTenantRequest { id: "acme".into() });
    rt(api::ResumeTenantRequest { id: "acme".into() });
    rt(api::DeleteTenantRequest { id: "acme".into() });
    rt(api::DeleteTenantResponse {
        id: "acme".into(),
        deleted: true,
    });
}

#[test]
fn token_messages_round_trip() {
    rt(token());
    rt(api::CreateTokenRequest {
        name: "ci".into(),
        description: String::new(),
        scopes: vec!["admin".into()],
        tenant: None,
        ttl_ms: Some(3_600_000),
        no_expiry: false,
    });
    rt(api::CreateTokenResponse {
        token: Some(token()),
        bearer: "ostro_otk_aaaaaaaaaaaaaaaa_x".into(),
    });
    rt(api::ListTokensRequest {
        tenant: Some("acme".into()),
    });
    rt(api::ListTokensResponse {
        tokens: vec![token(), api::ApiToken::default()],
    });
    rt(api::RevokeTokenRequest {
        id: "otk_aaaaaaaaaaaaaaaa".into(),
    });
    rt(api::RevokeTokenResponse {
        id: "otk_aaaaaaaaaaaaaaaa".into(),
        revoked: true,
    });
    rt(api::RotateTokenRequest {
        id: "otk_aaaaaaaaaaaaaaaa".into(),
        revoke_old: true,
        ttl_ms: None,
    });
    rt(api::RotateTokenResponse {
        token: Some(token()),
        bearer: "ostro_otk_bbbbbbbbbbbbbbbb_y".into(),
    });
    rt(api::WhoAmIRequest {});
    rt(api::WhoAmIResponse {
        token_id: "otk_aaaaaaaaaaaaaaaa".into(),
        tenant: Some("acme".into()),
        scopes: vec!["read".into()],
        kind: "token".into(),
    });
}

#[test]
fn audit_and_security_messages_round_trip() {
    rt(api::AuditQueryRequest {
        tenant: Some("acme".into()),
        actor: Some("otk_x".into()),
        action: Some("token.*".into()),
        since_ms: Some(1),
        until_ms: Some(2),
        limit: Some(50),
        cursor: Some("c1".into()),
    });
    let rec = api::AuditRecord {
        seq: 1,
        ts_ms: 2,
        node: 3,
        actor: "otk_x".into(),
        actor_kind: "token".into(),
        tenant: None,
        action: "deploy".into(),
        target: "module:abc".into(),
        outcome: "ok".into(),
        request_id: Some("r1".into()),
        details: HashMap::from([("sha256".to_owned(), "abc".to_owned())]),
        hash: "00ff".into(),
    };
    rt(rec.clone());
    rt(api::AuditPage {
        records: vec![rec],
        cursor: None,
        partial_nodes: vec![2, 5],
    });
    rt(key());
    let policy = api::ModulePolicy {
        require_signed_modules: true,
        trusted_keys: vec![
            key(),
            api::TrustedKey {
                tenant: Some("acme".into()),
                ..key()
            },
        ],
    };
    rt(policy.clone());
    rt(api::GetModulePolicyRequest {});
    rt(api::SetModulePolicyRequest {
        policy: Some(policy),
    });
    rt(api::AddTrustedKeyRequest { key: Some(key()) });
    rt(api::RemoveTrustedKeyRequest {
        key_id: "0123456789abcdef".into(),
    });
    rt(api::AttachSignaturesRequest {
        sha256: "a".repeat(64),
        signatures: vec![vec![1, 2, 3], vec![0; 8192]],
    });
    rt(api::AttachSignaturesResponse {
        sha256: "a".repeat(64),
        key_ids: vec!["0123456789abcdef".into()],
    });
}

/// Encode `(field number, LEN payload)` as protobuf wire bytes.
fn len_field(field: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, &mut out);
    prost::encoding::encode_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    out
}

#[test]
fn appended_fields_keep_old_encodings_decodable() {
    // An old (pre-14-02) UploadModuleRequest: fields 1..5 only.
    let old = api::UploadModuleRequest {
        sha256: "a".repeat(64),
        wasm: vec![0, 97, 115, 109],
        language: Some("rust".into()),
        tenant: Some("acme".into()),
        name: Some("orders".into()),
        signatures: vec![],
    };
    let bytes = old.encode_to_vec();
    let back = api::UploadModuleRequest::decode(bytes.as_slice()).expect("decode old");
    assert!(back.signatures.is_empty());
    assert_eq!(back, old);
    // The new field travels under its recorded number.
    let mut with_sig = bytes.clone();
    with_sig.extend(len_field(UPLOAD_SIGNATURES_FIELD, b"sig-one"));
    with_sig.extend(len_field(UPLOAD_SIGNATURES_FIELD, b"sig-two"));
    let back = api::UploadModuleRequest::decode(with_sig.as_slice()).expect("decode new");
    assert_eq!(
        back.signatures,
        vec![b"sig-one".to_vec(), b"sig-two".to_vec()]
    );
    assert_eq!(
        api::UploadModuleRequest {
            signatures: back.signatures.clone(),
            ..old.clone()
        }
        .encode_to_vec(),
        with_sig
    );

    // An old ListRunsRequest: fields 1..11 only.
    let old = api::ListRunsRequest {
        workflow: Some("orders".into()),
        limit: 5,
        tags: vec!["a".into()],
        dlq: true,
        ..Default::default()
    };
    let bytes = old.encode_to_vec();
    let back = api::ListRunsRequest::decode(bytes.as_slice()).expect("decode old");
    assert_eq!(back.tenant, None);
    assert_eq!(back, old);
    let new = api::ListRunsRequest {
        tenant: Some("acme".into()),
        ..old.clone()
    };
    let mut expect = bytes;
    expect.extend(len_field(LIST_RUNS_TENANT_FIELD, b"acme"));
    assert_eq!(new.encode_to_vec(), expect);
    // `Some("")` keeps presence (optional).
    let empty = api::ListRunsRequest {
        tenant: Some(String::new()),
        ..Default::default()
    };
    assert_eq!(
        api::ListRunsRequest::decode(empty.encode_to_vec().as_slice())
            .expect("decode")
            .tenant,
        Some(String::new())
    );
}

#[test]
fn services_and_methods_have_the_reserved_names() {
    use ostro_proto::GRPC_METHODS;
    for m in [
        "/ostro.api.v1.Tenants/Create",
        "/ostro.api.v1.Tenants/Get",
        "/ostro.api.v1.Tenants/List",
        "/ostro.api.v1.Tenants/Update",
        "/ostro.api.v1.Tenants/Suspend",
        "/ostro.api.v1.Tenants/Resume",
        "/ostro.api.v1.Tenants/Delete",
        "/ostro.api.v1.ApiTokens/Create",
        "/ostro.api.v1.ApiTokens/List",
        "/ostro.api.v1.ApiTokens/Revoke",
        "/ostro.api.v1.ApiTokens/Rotate",
        "/ostro.api.v1.ApiTokens/WhoAmI",
        "/ostro.api.v1.Audit/Query",
        "/ostro.api.v1.Security/GetModulePolicy",
        "/ostro.api.v1.Security/SetModulePolicy",
        "/ostro.api.v1.Security/AddTrustedKey",
        "/ostro.api.v1.Security/RemoveTrustedKey",
        "/ostro.api.v1.Security/AttachSignatures",
        // Pre-existing services are listed too.
        "/ostro.api.v1.Cluster/Join",
        "/ostro.health.v1.Health/Check",
    ] {
        assert!(GRPC_METHODS.contains(&m), "{m} missing from GRPC_METHODS");
    }
    let mut sorted = GRPC_METHODS.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), GRPC_METHODS.len(), "sorted and unique");
    // The generated service names (tonic) match.
    assert_eq!(api::tenants_server::SERVICE_NAME, "ostro.api.v1.Tenants");
    assert_eq!(
        api::api_tokens_server::SERVICE_NAME,
        "ostro.api.v1.ApiTokens"
    );
    assert_eq!(api::audit_server::SERVICE_NAME, "ostro.api.v1.Audit");
    assert_eq!(api::security_server::SERVICE_NAME, "ostro.api.v1.Security");
}

#[test]
fn bearers_are_redacted_from_debug() {
    let c = api::CreateTokenResponse {
        token: Some(token()),
        bearer: "ostro_otk_aaaaaaaaaaaaaaaa_SECRETSECRET".into(),
    };
    let r = api::RotateTokenResponse {
        token: Some(token()),
        bearer: "ostro_otk_aaaaaaaaaaaaaaaa_SECRETSECRET".into(),
    };
    for dbg in [format!("{c:?}"), format!("{r:?}")] {
        assert!(!dbg.contains("SECRETSECRET"), "{dbg}");
        assert!(dbg.contains("<redacted>"), "{dbg}");
        assert!(dbg.contains("otk_aaaaaaaaaaaaaaaa"), "{dbg}");
    }
}
