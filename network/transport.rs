use quinn::{Endpoint, ServerConfig};
use std::net::SocketAddr;
use rustls::{Certificate, PrivateKey};

/// Layer 2: Transport Layer (The Firehose)
/// QUIC-based high-speed delivery
pub struct TransportLayer {
    endpoint: Endpoint,
}

impl TransportLayer {
    pub fn new(bind_addr: SocketAddr) -> Self {
        // Generate self-signed cert for now
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = cert.serialize_der().unwrap();
        let priv_key = cert.serialize_private_key_der();
        let priv_key = PrivateKey(priv_key);
        let cert_chain = vec![Certificate(cert_der)];

        let server_config = ServerConfig::with_single_cert(cert_chain, priv_key).unwrap();
        let endpoint = Endpoint::server(server_config, bind_addr).unwrap();

        Self { endpoint }
    }

    pub async fn push_header(&self, _peer: SocketAddr, _data: &[u8]) {
        // Open stream and push
        // let conn = self.endpoint.connect(peer, "localhost").unwrap().await.unwrap();
        // let (mut send, _) = conn.open_bi().await.unwrap();
        // send.write_all(data).await.unwrap();
    }
}