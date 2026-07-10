use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use crate::config::TlsConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Build a shared rustls server configuration from PEM certificate and key files.
pub fn build_tls_config(config: &TlsConfig) -> std::io::Result<Arc<rustls::ServerConfig>> {
    let cert_file = File::open(&config.cert_path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to open cert file '{}': {}", config.cert_path, e),
        )
    })?;
    let key_file = File::open(&config.key_path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to open key file '{}': {}", config.key_path, e),
        )
    })?;

    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "No certificates found in cert file",
        ));
    }

    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
        .ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "No private key found in key file",
        )
    })?;

    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("TLS config error: {}", e),
            )
        })?;
    server_config.alpn_protocols =
        vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"postgresql".to_vec()];

    Ok(Arc::new(server_config))
}

/// Build a pgwire-compatible TLS acceptor.
pub fn build_tls_acceptor(config: &TlsConfig) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    build_tls_config(config).map(tokio_rustls::TlsAcceptor::from)
}
