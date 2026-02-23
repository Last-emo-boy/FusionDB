use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::config::TlsConfig;

/// Build a TLS acceptor from PEM cert + key files.
pub fn build_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let cert_file = File::open(&config.cert_path)
        .map_err(|e| format!("Failed to open cert file '{}': {}", config.cert_path, e))?;
    let key_file = File::open(&config.key_path)
        .map_err(|e| format!("Failed to open key file '{}': {}", config.key_path, e))?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .filter_map(|r| r.ok())
        .collect();
    if certs.is_empty() {
        return Err("No certificates found in cert file".into());
    }

    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
        .ok_or("No private key found in key file")?;

    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("TLS config error: {}", e))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}
