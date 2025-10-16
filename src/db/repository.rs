//! Repository pattern abstractions for StreamForge
//!
//! This module defines the repository trait and associated error types
//! for database operations with proper error handling and retry logic.

use async_trait::async_trait;

use std::fmt::Debug;
use thiserror::Error;

/// Result type for repository operations
pub type RepositoryResult<T> = Result<T, RepositoryError>;

/// Repository error types
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Database connection error
    #[error("Database connection error: {0}")]
    Connection(String),

    /// Query execution error
    #[error("Query execution error: {0}")]
    QueryExecution(String),

    /// Entity not found
    #[error("Entity not found: {0}")]
    NotFound(String),

    /// Conflict (e.g., duplicate key)
    #[error("Conflict error: {0}")]
    Conflict(String),

    /// Transaction error
    #[error("Transaction error: {0}")]
    Transaction(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Pool exhausted
    #[error("Connection pool exhausted")]
    PoolExhausted,

    /// Timeout
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Generic database error
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl RepositoryError {
    /// Check if the error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            RepositoryError::Connection(_)
            | RepositoryError::PoolExhausted
            | RepositoryError::Timeout(_) => true,
            RepositoryError::Database(e) => {
                // Check SQLx error for retryable conditions
                matches!(
                    e,
                    sqlx::Error::PoolTimedOut
                        | sqlx::Error::PoolClosed
                        | sqlx::Error::Io(_)
                        | sqlx::Error::Tls(_)
                )
            },
            _ => false,
        }
    }

    /// Check if this is a conflict error (duplicate key, etc.)
    pub fn is_conflict(&self) -> bool {
        match self {
            RepositoryError::Conflict(_) => true,
            RepositoryError::Database(e) => {
                // Check for unique constraint violations
                if let sqlx::Error::Database(db_err) = e {
                    // PostgreSQL unique violation error code is 23505
                    db_err.code().map_or(false, |code| code == "23505")
                } else {
                    false
                }
            },
            _ => false,
        }
    }

    /// Check if this is a not found error
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            RepositoryError::NotFound(_) | RepositoryError::Database(sqlx::Error::RowNotFound)
        )
    }
}

/// Convert repository errors to application errors
impl From<RepositoryError> for crate::error::Error {
    fn from(err: RepositoryError) -> Self {
        match err {
            RepositoryError::NotFound(msg) => crate::error::Error::NotFound(msg),
            RepositoryError::Timeout(msg) => crate::error::Error::Timeout(msg),
            _ => crate::error::Error::database(err.to_string()),
        }
    }
}

/// Base repository trait
#[async_trait]
pub trait Repository: Send + Sync {
    /// The entity type this repository manages
    type Entity: Send + Sync;

    /// The ID type for the entity
    type Id: Send + Sync + Debug;

    /// Find an entity by ID
    async fn find_by_id(&self, id: Self::Id) -> RepositoryResult<Option<Self::Entity>>;

    /// Check if an entity exists
    async fn exists(&self, id: Self::Id) -> RepositoryResult<bool>;

    /// Delete an entity by ID
    async fn delete(&self, id: Self::Id) -> RepositoryResult<bool>;

    /// Count total entities
    async fn count(&self) -> RepositoryResult<i64>;

    /// Health check for the repository
    async fn health_check(&self) -> RepositoryResult<()>;
}

/// Repository with upsert capability
#[async_trait]
pub trait UpsertRepository: Repository {
    /// Upsert an entity (insert or update)
    async fn upsert(&self, entity: &Self::Entity) -> RepositoryResult<Self::Entity>;
}

/// Repository with batch operations
#[async_trait]
pub trait BatchRepository: Repository {
    /// Insert multiple entities in a batch
    async fn insert_batch(&self, entities: &[Self::Entity]) -> RepositoryResult<u64>;

    /// Delete multiple entities by IDs
    async fn delete_batch(&self, ids: &[Self::Id]) -> RepositoryResult<u64>;
}

/// Repository with query capabilities
#[async_trait]
pub trait QueryRepository: Repository {
    /// Query filter type
    type Filter: Send + Sync;

    /// Query ordering type
    type Order: Send + Sync;

    /// Find entities matching a filter
    async fn find_by_filter(
        &self,
        filter: Self::Filter,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> RepositoryResult<Vec<Self::Entity>>;

    /// Count entities matching a filter
    async fn count_by_filter(&self, filter: Self::Filter) -> RepositoryResult<i64>;

    /// Find entities with ordering
    async fn find_ordered(
        &self,
        order: Self::Order,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> RepositoryResult<Vec<Self::Entity>>;
}

/// Transaction support for repositories
#[async_trait]
pub trait TransactionalRepository: Repository {
    /// Transaction type
    type Transaction<'a>: Send
    where
        Self: 'a;

    /// Begin a new transaction
    async fn begin_transaction(&self) -> RepositoryResult<Self::Transaction<'_>>;

    /// Commit a transaction
    async fn commit_transaction(&self, tx: Self::Transaction<'_>) -> RepositoryResult<()>;

    /// Rollback a transaction
    async fn rollback_transaction(&self, tx: Self::Transaction<'_>) -> RepositoryResult<()>;
}

/// Retry configuration for repository operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds
    pub max_backoff_ms: u64,
    /// Backoff multiplier
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10000,
            multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Default::default()
        }
    }

    /// Set the initial backoff
    pub fn with_initial_backoff(mut self, ms: u64) -> Self {
        self.initial_backoff_ms = ms;
        self
    }

    /// Set the maximum backoff
    pub fn with_max_backoff(mut self, ms: u64) -> Self {
        self.max_backoff_ms = ms;
        self
    }

    /// Set the backoff multiplier
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repository_error_retryable() {
        assert!(RepositoryError::Connection("test".to_string()).is_retryable());
        assert!(RepositoryError::PoolExhausted.is_retryable());
        assert!(RepositoryError::Timeout("test".to_string()).is_retryable());
        assert!(!RepositoryError::NotFound("test".to_string()).is_retryable());
        assert!(!RepositoryError::Conflict("test".to_string()).is_retryable());
    }

    #[test]
    fn test_repository_error_conflict() {
        assert!(RepositoryError::Conflict("test".to_string()).is_conflict());
        assert!(!RepositoryError::NotFound("test".to_string()).is_conflict());
    }

    #[test]
    fn test_repository_error_not_found() {
        assert!(RepositoryError::NotFound("test".to_string()).is_not_found());
        assert!(!RepositoryError::Conflict("test".to_string()).is_not_found());
    }

    #[test]
    fn test_retry_config() {
        let config = RetryConfig::new(5)
            .with_initial_backoff(200)
            .with_max_backoff(5000)
            .with_multiplier(1.5);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5000);
        assert_eq!(config.multiplier, 1.5);
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 100);
        assert_eq!(config.max_backoff_ms, 10000);
        assert_eq!(config.multiplier, 2.0);
    }
}
