//! Database module for StreamForge
//!
//! This module provides database connectivity, connection pooling,
//! and repository implementations for persistent storage.

pub mod event_repo;
pub mod pool;
pub mod repository;

// Re-export commonly used types
pub use event_repo::{EventRepository, PgEventRepository};
pub use pool::{create_pool, DbPool};
pub use repository::{Repository, RepositoryError, RepositoryResult};

use sqlx::migrate::Migrator;

/// Database migrator for running schema migrations
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Run database migrations
pub async fn run_migrations(pool: &DbPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}

/// Check database connectivity
pub async fn check_connection(pool: &DbPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").fetch_one(pool).await.map(|_| ())
}

/// Get database statistics
pub async fn get_stats(pool: &DbPool) -> DatabaseStats {
    DatabaseStats {
        pool_size: pool.size(),
        idle_connections: pool.num_idle(),
        max_connections: pool.options().get_max_connections(),
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    /// Current pool size
    pub pool_size: u32,
    /// Number of idle connections
    pub idle_connections: usize,
    /// Maximum connections allowed
    pub max_connections: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_stats() {
        let stats = DatabaseStats {
            pool_size: 10,
            idle_connections: 5,
            max_connections: 20,
        };

        assert_eq!(stats.pool_size, 10);
        assert_eq!(stats.idle_connections, 5);
        assert_eq!(stats.max_connections, 20);
    }
}
