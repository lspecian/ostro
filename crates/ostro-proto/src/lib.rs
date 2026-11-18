//! ostro-proto: generated gRPC types (codegen via protox, no protoc).

/// `ostro.health.v1` protocol.
pub mod health {
    /// Version 1 of the health service.
    pub mod v1 {
        tonic::include_proto!("ostro.health.v1");
    }
}

#[cfg(test)]
mod tests {
    use super::health::v1::CheckRequest;
    use prost::Message;

    #[test]
    fn check_request_roundtrip() {
        let req = CheckRequest::default();
        let bytes = req.encode_to_vec();
        let back = CheckRequest::decode(bytes.as_slice()).expect("decode");
        assert_eq!(req, back);
    }
}
