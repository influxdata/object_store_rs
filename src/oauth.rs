use crate::token::TemporaryToken;
use reqwest::{Client, Method};
use snafu::{ResultExt, Snafu};
use std::time::{Duration, Instant};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to decode service account key: {}", source))]
    DecodeKey { source: jsonwebtoken::errors::Error },

    #[snafu(display("Unable to encode jwt: {}", source))]
    EncodeJwt { source: jsonwebtoken::errors::Error },

    #[snafu(display("Error performing token request: {}", source))]
    TokenRequest { source: reqwest::Error },
}

#[derive(serde::Serialize)]
struct TokenClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(serde::Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Encapsulates the logic to perform an OAuth token challenge
#[derive(Debug)]
pub struct OAuthProvider {
    issuer: String,
    private_key: String,
    scope: String,
    audience: String,
}

impl OAuthProvider {
    /// Create a new [`OAuthProvider`]
    pub fn new(issuer: String, private_key: String, scope: String, audience: String) -> Self {
        Self {
            issuer,
            private_key,
            scope,
            audience,
        }
    }

    /// Fetch a fresh token
    pub async fn fetch_token(&self, client: &Client) -> Result<TemporaryToken<String>, Error> {
        let now = seconds_since_epoch();
        let exp = now + 3600;

        // https://cloud.google.com/storage/docs/authentication#oauth-scopes
        let claims = TokenClaims {
            iss: &self.issuer,
            scope: &self.scope,
            aud: &self.audience,
            exp,
            iat: now,
        };
        let header = jsonwebtoken::Header {
            alg: jsonwebtoken::Algorithm::RS256,
            ..Default::default()
        };
        let private_key_bytes = self.private_key.as_bytes();

        let private_key =
            jsonwebtoken::EncodingKey::from_rsa_pem(private_key_bytes).context(DecodeKeySnafu)?;
        let jwt = jsonwebtoken::encode(&header, &claims, &private_key).context(EncodeJwtSnafu)?;
        let body = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ];

        let response: TokenResponse = client
            .request(Method::POST, &self.audience)
            .form(&body)
            .send()
            .await
            .context(TokenRequestSnafu)?
            .error_for_status()
            .context(TokenRequestSnafu)?
            .json()
            .await
            .context(TokenRequestSnafu)?;

        let token = TemporaryToken {
            token: response.access_token,
            expiry: Instant::now() + Duration::from_secs(response.expires_in),
        };

        Ok(token)
    }
}

/// Returns the number of seconds since unix epoch
fn seconds_since_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
