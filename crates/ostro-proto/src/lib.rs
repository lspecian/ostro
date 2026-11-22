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
/// completion, Phase 5) mirror the REST operations one-to-one, as do the Phase 8
/// services `Schedules`, `RunLogs` (`schedules.proto`) and `Webhooks` (`webhooks.proto`)
/// and the Phase 6 services `Secrets` and `Connectors` (`bridge.proto`), and the Phase 7
/// services `Queues` (`queues.proto`) and `Workers` (`workers.proto`), and the Phase 11
/// services `RunOps` and `Inventory` (`ops.proto`). JSON payloads travel as UTF-8 JSON in `bytes` fields named
/// `*_json`; timestamps are Unix milliseconds in `*_at_ms` fields.
pub mod api {
    /// Version 1 of the control-plane API.
    pub mod v1 {
        tonic::include_proto!("ostro.api.v1");

        // Phase 7 (07-11): join-token secrets never reach `Debug` output (D-W3).
        impl std::fmt::Debug for WorkerTokenCreated {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("WorkerTokenCreated")
                    .field("token_id", &self.token_id)
                    .field("token", &"<redacted>")
                    .field("join_command", &"<redacted>")
                    .field("expires_at_ms", &self.expires_at_ms)
                    .finish()
            }
        }

        impl std::fmt::Debug for JoinWorkerRequest {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("JoinWorkerRequest")
                    .field("token", &"<redacted>")
                    .field("csr_pem", &self.csr_pem)
                    .field("worker_id", &self.worker_id)
                    .field("labels", &self.labels)
                    .finish()
            }
        }
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
    use super::api::v1::{
        CatchupPolicy, GetRunLogsRequest, LogLevel, OverlapPolicy, Schedule, Webhook,
        WebhookAction, WebhookSignalAction, WebhookVerifierKind, run_logs_client::RunLogsClient,
        run_logs_server::RunLogsServer, schedules_client::SchedulesClient,
        schedules_server::SchedulesServer, webhook_action, webhook_signal_action,
        webhooks_client::WebhooksClient, webhooks_server::WebhooksServer,
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

    #[test]
    fn phase8_messages_roundtrip_and_numbering() {
        // Overlap / catch-up numbering equals the meta constants (05-07).
        assert_eq!(OverlapPolicy::Skip as i32, 0);
        assert_eq!(OverlapPolicy::BufferOne as i32, 1);
        assert_eq!(OverlapPolicy::AllowAll as i32, 2);
        assert_eq!(OverlapPolicy::CancelOther as i32, 3);
        assert_eq!(OverlapPolicy::TerminateOther as i32, 4);
        assert_eq!(CatchupPolicy::Skip as i32, 0);
        assert_eq!(CatchupPolicy::One as i32, 1);
        assert_eq!(CatchupPolicy::All as i32, 2);
        assert_eq!(LogLevel::Unspecified as i32, 0);
        assert_eq!(WebhookVerifierKind::None as i32, 0);

        let s = Schedule {
            name: "nightly".into(),
            workflow: "report".into(),
            version: Some(0),
            cron: "0 0 * * *".into(),
            overlap: OverlapPolicy::BufferOne as i32,
            catchup_window_ms: Some(0),
            version_tag: Some(3),
            ..Default::default()
        };
        let back = Schedule::decode(s.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, s);
        assert_eq!(back.version, Some(0));
        assert_eq!(back.catchup_window_ms, Some(0));

        let w = Webhook {
            name: "gh".into(),
            action: Some(WebhookAction {
                kind: Some(webhook_action::Kind::Signal(WebhookSignalAction {
                    workflow: "w".into(),
                    name: "push".into(),
                    run_by: Some(webhook_signal_action::RunBy::RunIdPointer("/run".into())),
                })),
            }),
            ..Default::default()
        };
        assert_eq!(
            Webhook::decode(w.encode_to_vec().as_slice()).expect("decode"),
            w
        );
        let l = GetRunLogsRequest {
            run_id: "r".into(),
            limit: 0,
            level: Some(LogLevel::Warn as i32),
        };
        assert_eq!(
            GetRunLogsRequest::decode(l.encode_to_vec().as_slice()).expect("decode"),
            l
        );

        fn name<T: ?Sized>() -> &'static str {
            std::any::type_name::<T>()
        }
        assert!(name::<SchedulesClient<tonic::transport::Channel>>().contains("SchedulesClient"));
        assert!(name::<SchedulesServer<()>>().contains("SchedulesServer"));
        assert!(name::<WebhooksClient<tonic::transport::Channel>>().contains("WebhooksClient"));
        assert!(name::<WebhooksServer<()>>().contains("WebhooksServer"));
        assert!(name::<RunLogsClient<tonic::transport::Channel>>().contains("RunLogsClient"));
        assert!(name::<RunLogsServer<()>>().contains("RunLogsServer"));
    }

    #[test]
    fn phase7_messages_roundtrip_and_redaction() {
        use super::api::v1::{
            ExhaustAction, JoinWorkerRequest, PauseRunRequest, Placement, PutQueueFlowRequest,
            QueueFlow, RateSpec, WorkerTokenCreated, queues_client::QueuesClient,
            queues_server::QueuesServer, workers_client::WorkersClient,
            workers_server::WorkersServer,
        };
        assert_eq!(Placement::Unspecified as i32, 0);
        assert_eq!(Placement::Embedded as i32, 1);
        assert_eq!(Placement::Workers as i32, 2);
        assert_eq!(Placement::Auto as i32, 3);
        assert_eq!(ExhaustAction::Unspecified as i32, 0);
        assert_eq!(ExhaustAction::Fail as i32, 1);
        assert_eq!(ExhaustAction::Pause as i32, 2);

        let put = PutQueueFlowRequest {
            name: "q".into(),
            flow: Some(QueueFlow {
                placement: Placement::Auto as i32,
                selector: vec!["gpu=true".into()],
                concurrency: Some(0),
                throttle: Some(RateSpec {
                    limit: 10,
                    period_ms: 1_000,
                    burst: 2,
                }),
                ..Default::default()
            }),
            expected_version: Some(0),
        };
        let back = PutQueueFlowRequest::decode(put.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, put);
        assert_eq!(back.flow.as_ref().and_then(|f| f.concurrency), Some(0));
        assert_eq!(back.expected_version, Some(0));
        let p = PauseRunRequest {
            run_id: "r".into(),
            detail: Some(String::new()),
        };
        assert_eq!(
            PauseRunRequest::decode(p.encode_to_vec().as_slice()).expect("decode"),
            p
        );

        // Secrets are redacted from Debug.
        let created = WorkerTokenCreated {
            token_id: "wkr_abc".into(),
            token: "wkr_abc.s3cr3t-canary".into(),
            join_command: "ostro worker --token wkr_abc.s3cr3t-canary".into(),
            expires_at_ms: 1,
        };
        let join = JoinWorkerRequest {
            token: "wkr_abc.s3cr3t-canary".into(),
            csr_pem: "csr".into(),
            worker_id: "w1".into(),
            labels: [("gpu".to_owned(), "true".to_owned())].into(),
        };
        for dbg in [format!("{created:?}"), format!("{join:?}")] {
            assert!(!dbg.contains("s3cr3t-canary"), "{dbg}");
            assert!(dbg.contains("<redacted>"), "{dbg}");
        }
        assert_eq!(
            JoinWorkerRequest::decode(join.encode_to_vec().as_slice()).expect("decode"),
            join
        );

        fn name<T: ?Sized>() -> &'static str {
            std::any::type_name::<T>()
        }
        assert!(name::<QueuesClient<tonic::transport::Channel>>().contains("QueuesClient"));
        assert!(name::<QueuesServer<()>>().contains("QueuesServer"));
        assert!(name::<WorkersClient<tonic::transport::Channel>>().contains("WorkersClient"));
        assert!(name::<WorkersServer<()>>().contains("WorkersServer"));
    }

    #[test]
    fn bridge_messages_roundtrip() {
        use super::api::v1::{
            ConnectionInfo, GrantInfo, PutSecretRequest, SecretInfo,
            connectors_client::ConnectorsClient, connectors_server::ConnectorsServer,
            secrets_client::SecretsClient, secrets_server::SecretsServer,
        };
        let put = PutSecretRequest {
            name: "stripe/live".into(),
            value: b"sk_live_0123456789".to_vec(),
            tenant: Some(String::new()),
        };
        let back = PutSecretRequest::decode(put.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, put);
        assert_eq!(back.tenant.as_deref(), Some(""));
        let info = SecretInfo {
            name: "stripe/live".into(),
            key_id: "k1".into(),
            tenant: None,
        };
        assert_eq!(
            SecretInfo::decode(info.encode_to_vec().as_slice()).expect("decode"),
            info
        );
        let g = GrantInfo {
            id: String::new(),
            module: "acme/billing".into(),
            secret: "stripe/live".into(),
            origins: vec!["https://api.stripe.com".into()],
            body: true,
        };
        assert_eq!(
            GrantInfo::decode(g.encode_to_vec().as_slice()).expect("decode"),
            g
        );
        let c = ConnectionInfo {
            name: "db".into(),
            kind: "sql-postgres".into(),
            config_json: br#"{"v":1}"#.to_vec(),
        };
        assert_eq!(
            ConnectionInfo::decode(c.encode_to_vec().as_slice()).expect("decode"),
            c
        );

        fn name<T: ?Sized>() -> &'static str {
            std::any::type_name::<T>()
        }
        assert!(name::<SecretsClient<tonic::transport::Channel>>().contains("SecretsClient"));
        assert!(name::<SecretsServer<()>>().contains("SecretsServer"));
        assert!(name::<ConnectorsClient<tonic::transport::Channel>>().contains("ConnectorsClient"));
        assert!(name::<ConnectorsServer<()>>().contains("ConnectorsServer"));
    }

    #[test]
    fn phase11_messages_roundtrip_and_numbering() {
        use super::api::v1::{
            BulkAction, BulkJob, BulkNoArgs, CreateBulkRequest, DlqEntry, FailurePolicy,
            HistoryEvent, ListOrder, ListRunsRequest, OnFailurePolicy, RequeueDlqRequest,
            RequeueMode, RerunFrom, RerunRequest, RerunStep, RunStatus, SetFailurePolicyRequest,
            bulk_action, inventory_client::InventoryClient, inventory_server::InventoryServer,
            requeue_mode, rerun_from, run_ops_client::RunOpsClient, run_ops_server::RunOpsServer,
            set_failure_policy_request,
        };

        // Appended fields keep the pre-Phase 11 numbering: an old encoder's bytes decode
        // with the new fields at their defaults, and the new fields use 9..11 / 6.
        let old = ListRunsRequest {
            tag: Some("a".into()),
            limit: 5,
            ..Default::default()
        };
        let back = ListRunsRequest::decode(old.encode_to_vec().as_slice()).expect("decode");
        assert!(back.tags.is_empty());
        assert_eq!(back.order(), ListOrder::Unspecified);
        let new = ListRunsRequest {
            tags: vec!["a".into(), "b".into()],
            order: ListOrder::Newest as i32,
            dlq: true,
            ..Default::default()
        };
        // Field 9 (tags, LEN) = 0x4a, field 10 (order, varint) = 0x50, field 11 = 0x58.
        let bytes = new.encode_to_vec();
        assert_eq!(bytes[0], 0x4a);
        assert!(bytes.windows(2).any(|w| w == [0x50, 2]));
        assert!(bytes.windows(2).any(|w| w == [0x58, 1]));
        assert_eq!(
            ListRunsRequest::decode(bytes.as_slice()).expect("decode"),
            new
        );
        let ev = HistoryEvent {
            seq: 1,
            attempt: Some(0),
            ..Default::default()
        };
        let bytes = ev.encode_to_vec();
        // Field 6, fixed64 = 0x31; presence survives for 0.
        assert!(bytes.contains(&0x31));
        assert_eq!(
            HistoryEvent::decode(bytes.as_slice())
                .expect("decode")
                .attempt,
            Some(0)
        );

        let rerun = RerunRequest {
            run_id: "r".into(),
            from: Some(RerunFrom {
                point: Some(rerun_from::Point::Step(RerunStep {
                    name: "charge".into(),
                    occurrence: 2,
                })),
            }),
            idempotency_key: Some(String::new()),
            ..Default::default()
        };
        assert_eq!(
            RerunRequest::decode(rerun.encode_to_vec().as_slice()).expect("decode"),
            rerun
        );
        let rq = RequeueDlqRequest {
            run_id: "r".into(),
            mode: Some(RequeueMode {
                mode: Some(requeue_mode::Mode::Restart(true)),
            }),
            dry_run: true,
        };
        assert_eq!(
            RequeueDlqRequest::decode(rq.encode_to_vec().as_slice()).expect("decode"),
            rq
        );
        let bulk = CreateBulkRequest {
            action: Some(BulkAction {
                action: Some(bulk_action::Action::Pause(BulkNoArgs {})),
            }),
            rate_per_sec: Some(0),
            ..Default::default()
        };
        assert_eq!(
            CreateBulkRequest::decode(bulk.encode_to_vec().as_slice()).expect("decode"),
            bulk
        );
        let e = DlqEntry {
            run_id: "r".into(),
            status: RunStatus::TimedOut as i32,
            dismissed_at_ms: Some(0),
            ..Default::default()
        };
        assert_eq!(
            DlqEntry::decode(e.encode_to_vec().as_slice()).expect("decode"),
            e
        );
        let j = BulkJob {
            job_id: "blk_1".into(),
            eta_ms: Some(5),
            ..Default::default()
        };
        assert_eq!(
            BulkJob::decode(j.encode_to_vec().as_slice()).expect("decode"),
            j
        );
        let p = SetFailurePolicyRequest {
            scope: Some(set_failure_policy_request::Scope::Queue("q".into())),
            policy: Some(FailurePolicy {
                on_failure: Some(OnFailurePolicy {
                    workflow: "alert".into(),
                    queue: None,
                    include_input: true,
                }),
                dlq: None,
                version_tag: Some(3),
            }),
        };
        assert_eq!(
            SetFailurePolicyRequest::decode(p.encode_to_vec().as_slice()).expect("decode"),
            p
        );

        fn name<T: ?Sized>() -> &'static str {
            std::any::type_name::<T>()
        }
        assert!(name::<RunOpsClient<tonic::transport::Channel>>().contains("RunOpsClient"));
        assert!(name::<RunOpsServer<()>>().contains("RunOpsServer"));
        assert!(name::<InventoryClient<tonic::transport::Channel>>().contains("InventoryClient"));
        assert!(name::<InventoryServer<()>>().contains("InventoryServer"));
    }
}
