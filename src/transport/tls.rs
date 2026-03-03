use std::sync::Arc;

use super::error::TransportError;

/// Build a `rustls::ClientConfig` for use with [`super::connection::IgniteConnection`].
///
/// * `accept_invalid_certs` — when `true`, a custom verifier that accepts **any**
///   server certificate is installed.  Use this only in development / testing
///   environments with self-signed certificates.  Default (false) loads the
///   system's native CA bundle via `rustls-native-certs`.
///
/// Returns a shared, ready-to-use config wrapped in `Arc`.
pub fn build_tls_config(
    accept_invalid_certs: bool,
) -> Result<Arc<rustls::ClientConfig>, TransportError> {
    use rustls::crypto::ring::default_provider;

    let provider = Arc::new(default_provider());

    if accept_invalid_certs {
        // ── Danger mode: accept any server certificate ─────────────────────────
        // Used for development / self-signed certs.  Never use in production.
        #[derive(Debug)]
        struct AcceptAny;

        impl rustls::client::danger::ServerCertVerifier for AcceptAny {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls::pki_types::CertificateDer<'_>,
                _intermediates: &[rustls::pki_types::CertificateDer<'_>],
                _server_name: &rustls::pki_types::ServerName<'_>,
                _ocsp_response: &[u8],
                _now: rustls::pki_types::UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            }
        }

        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAny))
            .with_no_client_auth();

        Ok(Arc::new(config))
    } else {
        // ── Production mode: verify server certificate against system CA bundle ─
        // `rustls_native_certs::load_native_certs()` returns a `CertificateResult`
        // struct (not a `Result`) with a `.certs` field; partial failures for
        // individual certificate files are silently ignored, which is acceptable
        // for native-cert loading.
        let mut roots = rustls::RootCertStore::empty();
        let result = rustls_native_certs::load_native_certs();
        for cert in result.certs {
            roots
                .add(cert)
                .map_err(|e| TransportError::Tls(e.to_string()))?;
        }

        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();

        Ok(Arc::new(config))
    }
}
