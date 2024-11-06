use async_trait::async_trait;
use std::collections::HashMap;
use thiserror::Error;

use pgwire::{
    api::auth::{AuthSource, LoginInfo, Password},
    error::{ErrorInfo, PgWireError, PgWireResult},
};

use crate::config::AuthConfig;

pub struct Authentication {
    pairs: HashMap<String, String>,
}

impl Authentication {
    pub fn from_env() -> Self {
        let pairs = AuthConfig::get_pairs();

        Self { pairs }
    }
}

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Missing username")]
    MissingUsername,
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Internal error: {0}")]
    Internal(String),
}

#[async_trait]
impl AuthSource for Authentication {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        let username = login_info
            .user()
            .ok_or(AuthError::MissingUsername)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "SQLSTATE".to_string(),
                    "ERROR".to_string(),
                    e.to_string(),
                )))
            })?
            .to_string();

        let password = self
            .pairs
            .get(&username)
            .ok_or(AuthError::InvalidCredentials)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "SQLSTATE".to_string(),
                    "ERROR".to_string(),
                    e.to_string(),
                )))
            })?;

        Ok(Password::new(None, password.as_bytes().to_vec()))
    }
}
