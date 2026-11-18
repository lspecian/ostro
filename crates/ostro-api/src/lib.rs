//! ostro-api: HTTP + gRPC API surface (skeleton).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_builds() {
        let _r: axum::Router = router();
    }

    #[tokio::test]
    async fn health_check_serving() {
        use ostro_proto::health::v1::{CheckRequest, health_server::Health};
        let resp = HealthSvc
            .check(tonic::Request::new(CheckRequest::default()))
            .await
            .expect("check");
        assert_eq!(resp.into_inner().status, "SERVING");
    }

    #[test]
    fn aws_lc_rs_provider_has_cipher_suites() {
        let p = rustls::crypto::aws_lc_rs::default_provider();
        assert!(!p.cipher_suites.is_empty());
    }
}
