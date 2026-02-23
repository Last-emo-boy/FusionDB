use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use std::collections::{HashMap, HashSet};

use crate::common::{FusionError, Result};
use crate::storage::Transaction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub password_hash: String,
    pub is_superuser: bool,
    pub permissions: HashMap<String, HashSet<String>>, // table -> {SELECT, INSERT, UPDATE, DELETE}
}

impl UserRecord {
    pub fn new(password: &str, is_superuser: bool) -> Self {
        Self {
            password_hash: hash_password(password),
            is_superuser,
            permissions: HashMap::new(),
        }
    }

    pub fn verify_password(&self, password: &str) -> bool {
        self.password_hash == hash_password(password)
    }

    pub fn has_permission(&self, table: &str, operation: &str) -> bool {
        if self.is_superuser {
            return true;
        }
        if let Some(perms) = self.permissions.get("*") {
            if perms.contains("ALL") || perms.contains(operation) {
                return true;
            }
        }
        if let Some(perms) = self.permissions.get(table) {
            return perms.contains("ALL") || perms.contains(operation);
        }
        false
    }

    pub fn grant(&mut self, table: &str, privilege: &str) {
        self.permissions
            .entry(table.to_string())
            .or_default()
            .insert(privilege.to_string());
    }

    pub fn revoke(&mut self, table: &str, privilege: &str) {
        if let Some(perms) = self.permissions.get_mut(table) {
            perms.remove(privilege);
            if perms.is_empty() {
                self.permissions.remove(table);
            }
        }
    }
}

pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub async fn get_user(txn: &mut dyn Transaction, username: &str) -> Result<Option<UserRecord>> {
    let key = format!("user:{}", username);
    if let Some(bytes) = txn.get(key.as_bytes()).await? {
        let record: UserRecord = serde_json::from_slice(&bytes)
            .map_err(|e| FusionError::Execution(format!("User record decode error: {}", e)))?;
        Ok(Some(record))
    } else {
        Ok(None)
    }
}

pub async fn save_user(txn: &mut dyn Transaction, username: &str, record: &UserRecord) -> Result<()> {
    let key = format!("user:{}", username);
    let bytes = serde_json::to_vec(record)
        .map_err(|e| FusionError::Execution(format!("User record encode error: {}", e)))?;
    txn.put(key.as_bytes(), &bytes).await?;
    Ok(())
}

pub async fn delete_user(txn: &mut dyn Transaction, username: &str) -> Result<()> {
    let key = format!("user:{}", username);
    txn.delete(key.as_bytes()).await?;
    Ok(())
}

/// Check if a user has permission to perform an operation on a table.
/// Returns Ok(()) if allowed, Err if denied.
/// If username is None or empty, permission is granted (anonymous/legacy mode).
pub async fn check_permission(
    txn: &mut dyn Transaction,
    username: &str,
    table: &str,
    operation: &str,
) -> Result<()> {
    if username.is_empty() {
        return Ok(()); // Anonymous mode — no enforcement
    }
    match get_user(txn, username).await? {
        Some(user) => {
            if user.has_permission(table, operation) {
                Ok(())
            } else {
                Err(FusionError::Execution(format!(
                    "Permission denied: user '{}' lacks {} on '{}'",
                    username, operation, table
                )))
            }
        }
        None => {
            // User not in RBAC system — allow (legacy compatibility)
            Ok(())
        }
    }
}

pub async fn list_users(txn: &mut dyn Transaction) -> Result<Vec<(String, UserRecord)>> {
    let prefix = "user:";
    let pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
    let mut users = Vec::new();
    for (k, v) in pairs {
        if let Ok(key_str) = std::str::from_utf8(&k) {
            if let Some(name) = key_str.strip_prefix(prefix) {
                if let Ok(record) = serde_json::from_slice::<UserRecord>(&v) {
                    users.push((name.to_string(), record));
                }
            }
        }
    }
    Ok(users)
}
