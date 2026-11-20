//! In-process OTLP collector (plan 08-04): hosts the OTLP gRPC `TraceService` and
//! `LogsService` (opentelemetry-proto `gen-tonic`, over the workspace's locked tonic) on
//! `127.0.0.1:0` and records every export request.
//!
//! ```ignore
//! let (addr, collector) = Collector::start().await;
//! cfg.telemetry.otlp_endpoint = Some(format!("http://{addr}"));
//! // ... run the server ...
//! collector.wait_for(|c| c.logs().iter().any(|l| l.body == "hello"), T).await;
//! ```

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use tokio::task::JoinHandle;

/// One exported span, flattened.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanView {
    pub name: String,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub parent_span_id: Vec<u8>,
    /// Instrumentation scope name.
    pub scope: String,
    pub attributes: BTreeMap<String, String>,
    /// Resource attributes of the batch the span came in.
    pub resource: BTreeMap<String, String>,
}

/// One exported log record, flattened.
#[derive(Clone, Debug, PartialEq)]
pub struct LogView {
    /// Instrumentation scope name (the `tracing` target for bridged node logs).
    pub scope: String,
    pub severity_text: String,
    /// The body rendered as a string (`""` when absent).
    pub body: String,
    pub attributes: BTreeMap<String, String>,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub resource: BTreeMap<String, String>,
}

#[derive(Default)]
struct Recorded {
    traces: Vec<ExportTraceServiceRequest>,
    logs: Vec<ExportLogsServiceRequest>,
}

/// A running collector. Dropping it stops the server task.
#[derive(Clone)]
pub struct Collector {
    rec: Arc<Mutex<Recorded>>,
    _task: Arc<AbortOnDrop>,
}

impl std::fmt::Debug for Collector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = self.lock();
        f.debug_struct("Collector")
            .field("trace_requests", &r.traces.len())
            .field("log_requests", &r.logs.len())
            .finish()
    }
}

struct TraceSvc(Arc<Mutex<Recorded>>);
struct LogsSvc(Arc<Mutex<Recorded>>);

#[tonic::async_trait]
impl TraceService for TraceSvc {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .traces
            .push(request.into_inner());
        Ok(tonic::Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl LogsService for LogsSvc {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .logs
            .push(request.into_inner());
        Ok(tonic::Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

/// Stops the server task when the last [`Collector`] clone is dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Collector {
    /// Bind `127.0.0.1:0` and serve both services on the current Tokio runtime.
    pub async fn start() -> (SocketAddr, Collector) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OTLP collector");
        let addr = listener.local_addr().expect("collector addr");
        let rec = Arc::new(Mutex::new(Recorded::default()));
        let server = tonic::transport::Server::builder()
            .add_service(TraceServiceServer::new(TraceSvc(Arc::clone(&rec))))
            .add_service(LogsServiceServer::new(LogsSvc(Arc::clone(&rec))));
        let task: JoinHandle<()> = tokio::spawn(async move {
            let incoming = tonic::transport::server::TcpIncoming::from(listener);
            if let Err(e) = server.serve_with_incoming(incoming).await {
                eprintln!("OTLP collector stopped: {e}");
            }
        });
        (
            addr,
            Collector {
                rec,
                _task: Arc::new(AbortOnDrop(task)),
            },
        )
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Recorded> {
        self.rec.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every recorded trace export request.
    pub fn trace_requests(&self) -> Vec<ExportTraceServiceRequest> {
        self.lock().traces.clone()
    }

    /// Every recorded logs export request.
    pub fn log_requests(&self) -> Vec<ExportLogsServiceRequest> {
        self.lock().logs.clone()
    }

    /// Every exported span, flattened, in arrival order.
    pub fn spans(&self) -> Vec<SpanView> {
        let r = self.lock();
        let mut out = Vec::new();
        for req in &r.traces {
            for rs in &req.resource_spans {
                let resource = rs
                    .resource
                    .as_ref()
                    .map(|r| attrs(&r.attributes))
                    .unwrap_or_default();
                for ss in &rs.scope_spans {
                    let scope = ss
                        .scope
                        .as_ref()
                        .map(|s| s.name.clone())
                        .unwrap_or_default();
                    for s in &ss.spans {
                        out.push(SpanView {
                            name: s.name.clone(),
                            trace_id: s.trace_id.clone(),
                            span_id: s.span_id.clone(),
                            parent_span_id: s.parent_span_id.clone(),
                            scope: scope.clone(),
                            attributes: attrs(&s.attributes),
                            resource: resource.clone(),
                        });
                    }
                }
            }
        }
        out
    }

    /// Every exported log record, flattened, in arrival order.
    pub fn logs(&self) -> Vec<LogView> {
        let r = self.lock();
        let mut out = Vec::new();
        for req in &r.logs {
            for rl in &req.resource_logs {
                let resource = rl
                    .resource
                    .as_ref()
                    .map(|r| attrs(&r.attributes))
                    .unwrap_or_default();
                for sl in &rl.scope_logs {
                    let scope = sl
                        .scope
                        .as_ref()
                        .map(|s| s.name.clone())
                        .unwrap_or_default();
                    for l in &sl.log_records {
                        out.push(LogView {
                            scope: scope.clone(),
                            severity_text: l.severity_text.clone(),
                            body: l.body.as_ref().map(render).unwrap_or_default(),
                            attributes: attrs(&l.attributes),
                            trace_id: l.trace_id.clone(),
                            span_id: l.span_id.clone(),
                            resource: resource.clone(),
                        });
                    }
                }
            }
        }
        out
    }

    /// Every recorded request (traces, then logs) as bytes, for canary scans (08-20/21).
    ///
    /// The testkit has no `prost` dependency, so this is the decoded requests' `Debug`
    /// rendering, not the protobuf wire bytes: every string field appears verbatim
    /// (`Debug` escapes only quotes, backslashes and control characters), bytes fields
    /// appear as decimal lists.
    pub fn all_bytes(&self) -> Vec<u8> {
        let r = self.lock();
        let mut out = Vec::new();
        for req in &r.traces {
            out.extend_from_slice(format!("{req:?}\n").as_bytes());
        }
        for req in &r.logs {
            out.extend_from_slice(format!("{req:?}\n").as_bytes());
        }
        out
    }

    /// Poll every 20 ms until `pred(self)` holds; panics after `timeout`.
    pub async fn wait_for(&self, pred: impl Fn(&Collector) -> bool, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if pred(self) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "OTLP collector condition not met within {timeout:?}: {self:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn attrs(kvs: &[KeyValue]) -> BTreeMap<String, String> {
    kvs.iter()
        .map(|kv| {
            (
                kv.key.clone(),
                kv.value.as_ref().map(render).unwrap_or_default(),
            )
        })
        .collect()
}

/// A value rendered as a plain string (strings unquoted).
pub fn render(v: &AnyValue) -> String {
    match &v.value {
        Some(any_value::Value::StringValue(s)) => s.clone(),
        Some(any_value::Value::BoolValue(b)) => b.to_string(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(any_value::Value::DoubleValue(d)) => d.to_string(),
        Some(other) => format!("{other:?}"),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    use super::*;

    #[tokio::test]
    async fn collector_records_log_exports() {
        let (addr, c) = Collector::start().await;
        let mut client = LogsServiceClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let record = LogRecord {
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("hello".into())),
            }),
            ..LogRecord::default()
        };
        client
            .export(ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    scope_logs: vec![ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "ostro::probe".into(),
                            ..InstrumentationScope::default()
                        }),
                        log_records: vec![record],
                        ..ScopeLogs::default()
                    }],
                    ..ResourceLogs::default()
                }],
            })
            .await
            .expect("export");
        c.wait_for(|c| !c.logs().is_empty(), Duration::from_secs(5))
            .await;
        let logs = c.logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].scope, "ostro::probe");
        assert_eq!(logs[0].body, "hello");
        assert!(String::from_utf8_lossy(&c.all_bytes()).contains("hello"));
        assert!(c.spans().is_empty());
    }
}
