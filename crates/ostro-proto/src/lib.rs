//! ostro-proto: generated gRPC types (codegen via protox, no protoc).

/// `ostro.health.v1` protocol.
pub mod health {
    /// Version 1 of the health service.
    pub mod v1 {
        tonic::include_proto!("ostro.health.v1");
    }
}

/// `ostro.api.v1` control-plane protocol (gRPC half of API-01).
///
/// Services `Modules`, `Workflows`, `Runs` and `Tokens` (await-token
/// completion, Phase 5) mirror the REST operations one-to-one. JSON payloads travel as UTF-8 JSON in `bytes` fields named
/// `*_json`; timestamps are Unix milliseconds in `*_at_ms` fields.
pub mod api {
    /// Version 1 of the control-plane API.
    pub mod v1 {
        tonic::include_proto!("ostro.api.v1");
    }
}

#[cfg(test)]
mod tests {
    use super::api::v1::{
        AwaitResultResponse, CompleteTokenRequest, ConflictPolicy, ListRunsRequest, RetryMode,
        RunInfo, RunPolicy, RunStatus, SignalRunRequest, StartRunRequest,
        modules_client::ModulesClient, modules_server::ModulesServer, runs_client::RunsClient,
        runs_server::RunsServer, tokens_client::TokensClient, tokens_server::TokensServer,
        workflows_client::WorkflowsClient, workflows_server::WorkflowsServer,
    };
    use super::health::v1::CheckRequest;
    use prost::Message;

    #[test]
    fn check_request_roundtrip() {
        let req = CheckRequest::default();
        let bytes = req.encode_to_vec();
        let back = CheckRequest::decode(bytes.as_slice()).expect("decode");
        assert_eq!(req, back);
    }

    #[test]
    fn start_run_request_all_fields_roundtrip() {
        let req = StartRunRequest {
            workflow: "order".into(),
            input_json: br#"{"id":1}"#.to_vec(),
            version: Some(0),
            queue: Some("default".into()),
            delay_ms: Some(0),
            priority: Some(-3),
            tags: vec!["a".into(), "b".into()],
            traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
            tenant: Some(String::new()),
            idempotency_key: Some("k1".into()),
            conflict_policy: ConflictPolicy::Reject as i32,
            concurrency_key: Some("c".into()),
            schedule_to_start_timeout_ms: Some(0),
            retention_ms: Some(3_600_000),
            dry_run: true,
        };
        let back = StartRunRequest::decode(req.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(req, back);
        // Presence survives even for zero/empty values.
        assert_eq!(back.version, Some(0));
        assert_eq!(back.delay_ms, Some(0));
        assert_eq!(back.tenant.as_deref(), Some(""));
        assert_eq!(back.schedule_to_start_timeout_ms, Some(0));
        assert_eq!(back.conflict_policy(), ConflictPolicy::Reject);

        let unset = StartRunRequest::decode(StartRunRequest::default().encode_to_vec().as_slice())
            .expect("decode");
        assert_eq!(unset.version, None);
        assert_eq!(unset.conflict_policy(), ConflictPolicy::ReturnExisting);
    }

    #[test]
    fn run_info_and_list_roundtrip() {
        let info = RunInfo {
            run_id: "r1".into(),
            status: RunStatus::Completed as i32,
            result_json: Some(b"null".to_vec()),
            completed_at_ms: Some(42),
            ..Default::default()
        };
        assert_eq!(
            RunInfo::decode(info.encode_to_vec().as_slice()).expect("decode"),
            info
        );
        let list = ListRunsRequest {
            status: Some(RunStatus::Unspecified as i32),
            limit: 10,
            ..Default::default()
        };
        let back = ListRunsRequest::decode(list.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back.status, Some(0));
    }

    #[test]
    fn run_status_numbering() {
        assert_eq!(RunStatus::Unspecified as i32, 0);
        assert_eq!(RunStatus::Scheduled as i32, 1);
        assert_eq!(RunStatus::Ready as i32, 2);
        assert_eq!(RunStatus::Running as i32, 3);
        assert_eq!(RunStatus::Completed as i32, 4);
        assert_eq!(RunStatus::Failed as i32, 5);
        assert_eq!(RunStatus::Cancelled as i32, 6);
        assert_eq!(ConflictPolicy::ReturnExisting as i32, 0);
        assert_eq!(ConflictPolicy::Reject as i32, 1);
        assert_eq!(
            RunStatus::from_str_name("CANCELLED"),
            Some(RunStatus::Cancelled)
        );
    }

    #[test]
    fn server_and_client_types_are_nameable() {
        fn name<T: ?Sized>() -> &'static str {
            std::any::type_name::<T>()
        }
        assert!(name::<ModulesClient<tonic::transport::Channel>>().contains("ModulesClient"));
        assert!(name::<RunsClient<tonic::transport::Channel>>().contains("RunsClient"));
        assert!(name::<WorkflowsClient<tonic::transport::Channel>>().contains("WorkflowsClient"));
        assert!(name::<ModulesServer<()>>().contains("ModulesServer"));
        assert!(name::<RunsServer<()>>().contains("RunsServer"));
        assert!(name::<WorkflowsServer<()>>().contains("WorkflowsServer"));
        assert!(name::<TokensClient<tonic::transport::Channel>>().contains("TokensClient"));
        assert!(name::<TokensServer<()>>().contains("TokensServer"));
    }

    #[test]
    fn phase5_lifecycle_messages_roundtrip() {
        let sig = SignalRunRequest {
            run_id: "r1".into(),
            name: "approve".into(),
            payload_json: b"{}".to_vec(),
            request_id: Some(String::new()),
            dry_run: true,
        };
        let back = SignalRunRequest::decode(sig.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, sig);
        assert_eq!(back.request_id.as_deref(), Some(""));

        let tok = CompleteTokenRequest {
            token: "ot1_abc".into(),
            payload_json: b"1".to_vec(),
            dry_run: false,
        };
        assert_eq!(
            CompleteTokenRequest::decode(tok.encode_to_vec().as_slice()).expect("decode"),
            tok
        );

        let res = AwaitResultResponse {
            run_id: "r1".into(),
            status: RunStatus::TimedOut as i32,
            completed: true,
            result_json: None,
            failure_json: Some(b"\"x\"".to_vec()),
        };
        assert_eq!(
            AwaitResultResponse::decode(res.encode_to_vec().as_slice()).expect("decode"),
            res
        );

        // Presence of zero durations survives.
        let p = RunPolicy {
            retention_ms: Some(0),
            schedule_to_start_ms: None,
        };
        let back = RunPolicy::decode(p.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back.retention_ms, Some(0));
        assert_eq!(back.schedule_to_start_ms, None);

        assert_eq!(RetryMode::Unspecified as i32, 0);
        assert_eq!(RetryMode::Resumed as i32, 1);
        assert_eq!(RetryMode::Restarted as i32, 2);
    }
}
