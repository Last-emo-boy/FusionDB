use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use hmac::{Hmac, Mac};
use http::{HeaderMap, Method};
use sha2::{Digest, Sha256};

pub(crate) const FORWARDED_HEADER: &str = "x-fusiondb-forwarded";
pub(crate) const FORWARDED_VALUE: &str = "shard-owner";
pub(crate) const FORWARDED_USER_HEADER: &str = "x-fusiondb-user";
pub(crate) const FORWARDED_TIMESTAMP_HEADER: &str = "x-fusiondb-forwarded-timestamp";
pub(crate) const FORWARDED_SIGNATURE_HEADER: &str = "x-fusiondb-forwarded-signature";
pub(crate) const FORWARDED_CONTENT_SHA256_HEADER: &str = "x-fusiondb-forwarded-content-sha256";

const MAX_CLOCK_SKEW_SECS: u64 = 60;
type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub(crate) struct ForwardingAuth {
    secret: Arc<[u8]>,
}

impl ForwardingAuth {
    pub(crate) fn new(secret: impl AsRef<[u8]>) -> Option<Self> {
        let secret = secret.as_ref();
        if secret.is_empty() {
            None
        } else {
            Some(Self {
                secret: Arc::from(secret),
            })
        }
    }

    pub(crate) fn apply(
        &self,
        request: reqwest::RequestBuilder,
        username: &str,
    ) -> reqwest::RequestBuilder {
        let Some(metadata) = request.try_clone().and_then(|request| request.build().ok()) else {
            return request;
        };
        let body = match metadata.body() {
            Some(body) => {
                let Some(body) = body.as_bytes() else {
                    return request;
                };
                body
            }
            None => &[],
        };
        let request_target = request_target(metadata.url());
        let content_sha256 = content_sha256(body);
        let timestamp = unix_timestamp();
        let signature = self.signature(
            metadata.method(),
            &request_target,
            username,
            timestamp,
            &content_sha256,
        );

        request
            .header(FORWARDED_HEADER, FORWARDED_VALUE)
            .header(FORWARDED_USER_HEADER, username)
            .header(FORWARDED_TIMESTAMP_HEADER, timestamp.to_string())
            .header(FORWARDED_CONTENT_SHA256_HEADER, content_sha256)
            .header(FORWARDED_SIGNATURE_HEADER, signature)
    }

    pub(crate) fn verify(
        &self,
        headers: &HeaderMap,
        method: &Method,
        request_target: &str,
        body: &[u8],
    ) -> Option<String> {
        if headers.get(FORWARDED_HEADER)?.to_str().ok()? != FORWARDED_VALUE {
            return None;
        }

        let username = headers.get(FORWARDED_USER_HEADER)?.to_str().ok()?.trim();
        if username.is_empty() {
            return None;
        }

        let timestamp = headers
            .get(FORWARDED_TIMESTAMP_HEADER)?
            .to_str()
            .ok()?
            .parse::<u64>()
            .ok()?;
        if unix_timestamp().abs_diff(timestamp) > MAX_CLOCK_SKEW_SECS {
            return None;
        }

        let supplied_content_sha256 = headers
            .get(FORWARDED_CONTENT_SHA256_HEADER)?
            .to_str()
            .ok()?;
        let actual_content_sha256 = content_sha256(body);
        if supplied_content_sha256 != actual_content_sha256 {
            return None;
        }

        let supplied = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(headers.get(FORWARDED_SIGNATURE_HEADER)?.as_bytes())
            .ok()?;
        let mut mac = HmacSha256::new_from_slice(&self.secret).ok()?;
        mac.update(&signature_payload(
            method,
            request_target,
            username,
            timestamp,
            &actual_content_sha256,
        ));
        mac.verify_slice(&supplied).ok()?;
        Some(username.to_string())
    }

    fn signature(
        &self,
        method: &Method,
        request_target: &str,
        username: &str,
        timestamp: u64,
        content_sha256: &str,
    ) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.secret)
            .expect("HMAC accepts keys of any non-zero size");
        mac.update(&signature_payload(
            method,
            request_target,
            username,
            timestamp,
            content_sha256,
        ));
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }
}

fn signature_payload(
    method: &Method,
    request_target: &str,
    username: &str,
    timestamp: u64,
    content_sha256: &str,
) -> Vec<u8> {
    format!(
        "{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        request_target,
        username,
        timestamp,
        content_sha256
    )
    .into_bytes()
}

fn request_target(url: &reqwest::Url) -> String {
    match url.query() {
        Some(query) => format!("{}?{}", url.path(), query),
        None => url.path().to_string(),
    }
}

fn content_sha256(body: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(body))
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_request_verifies_and_tampering_fails() {
        let auth = ForwardingAuth::new("a sufficiently distinct cluster secret").unwrap();
        let request = auth
            .apply(
                reqwest::Client::new()
                    .post("http://127.0.0.1:8091/query?linearizable=true")
                    .body("original body"),
                "alice",
            )
            .build()
            .unwrap();
        let body = request.body().unwrap().as_bytes().unwrap();
        let target = "/query?linearizable=true";

        assert_eq!(
            auth.verify(request.headers(), request.method(), target, body),
            Some("alice".to_string())
        );
        assert_eq!(
            auth.verify(request.headers(), &Method::GET, target, body),
            None
        );
        assert_eq!(
            auth.verify(
                request.headers(),
                request.method(),
                "/query?linearizable=false",
                body,
            ),
            None
        );
        assert_eq!(
            auth.verify(
                request.headers(),
                request.method(),
                target,
                b"tampered body",
            ),
            None
        );
    }
}
