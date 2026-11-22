//! Phase 12: implemented in 12-14 (the tonic `Replication` service on the cluster
//! listener). Each RPC delegates to its module (`fetch::handle_fetch`,
//! `snapshot::stream_snapshot`, `blob::{fetch_blob, push_blob}`) behind
//! `authz::require_node`; until then every RPC is `Status::unimplemented`.

use std::pin::Pin;

use futures_util::Stream;
use ostro_proto::cluster::v1::replication_server::Replication;
use ostro_proto::cluster::v1::{
    BlobChunk, FetchBlobRequest, FetchRequest, FetchResponse, FetchSnapshotRequest, PushBlobAck,
    SnapshotFrame,
};
use tonic::{Request, Response, Status, Streaming};

/// A boxed server stream of `T`.
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// The `Replication` service (state owned by 12-14).
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ReplicationService {}

fn unimplemented() -> Status {
    Status::unimplemented("Phase 12: implemented in 12-14")
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    async fn fetch(&self, _req: Request<FetchRequest>) -> Result<Response<FetchResponse>, Status> {
        Err(unimplemented())
    }

    type FetchSnapshotStream = BoxStream<SnapshotFrame>;

    async fn fetch_snapshot(
        &self,
        _req: Request<FetchSnapshotRequest>,
    ) -> Result<Response<Self::FetchSnapshotStream>, Status> {
        Err(unimplemented())
    }

    type FetchBlobStream = BoxStream<BlobChunk>;

    async fn fetch_blob(
        &self,
        _req: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        Err(unimplemented())
    }

    async fn push_blob(
        &self,
        _req: Request<Streaming<BlobChunk>>,
    ) -> Result<Response<PushBlobAck>, Status> {
        Err(unimplemented())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_rpc_is_unimplemented_until_12_14() {
        let svc = ReplicationService::default();
        let s = svc
            .fetch(Request::new(FetchRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(s.code(), tonic::Code::Unimplemented);
        let s = svc
            .fetch_snapshot(Request::new(FetchSnapshotRequest::default()))
            .await
            .err()
            .unwrap();
        assert_eq!(s.code(), tonic::Code::Unimplemented);
        let s = svc
            .fetch_blob(Request::new(FetchBlobRequest::default()))
            .await
            .err()
            .unwrap();
        assert_eq!(s.code(), tonic::Code::Unimplemented);
        let _server = ostro_proto::cluster::v1::replication_server::ReplicationServer::new(svc);
    }

    #[test]
    fn require_node_without_identity_is_unauthenticated() {
        // 12-05 implemented require_node (authz.rs); its full matrix is tested there.
        let req = Request::new(());
        assert_eq!(
            crate::authz::require_node(&req).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }
}
