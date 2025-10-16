//! Event repository implementation for StreamForge
//!
//! This module provides the PostgreSQL implementation of the event repository
//! with idempotent upsert operations and retry logic.

use async_trait::async_trait;
use backoff::{future::retry, ExponentialBackoff};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgQueryResult, Row};
use std::time::Duration;
use uuid::Uuid;

use crate::{
    db::{
        repository::{
            Repository, RepositoryError, RepositoryResult, RetryConfig, UpsertRepository,
        },
        DbPool,
    },
    models::event::{EventType, NormalizedEvent},
};

/// Event repository trait
#[async_trait]
pub trait EventRepository:
    Repository<Entity = NormalizedEvent, Id = Uuid> + UpsertRepository
{
    /// Find events by user ID
    async fn find_by_user(
        &self,
        user_id: Uuid,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> RepositoryResult<Vec<NormalizedEvent>>;

    /// Find events by time range
    async fn find_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
    ) -> RepositoryResult<Vec<NormalizedEvent>>;

    /// Find events by Kafka partition and offset
    async fn find_by_kafka_position(
        &self,
        partition: i32,
        offset: i64,
    ) -> RepositoryResult<Option<NormalizedEvent>>;

    /// Get the latest processed offset for a partition
    async fn get_latest_offset(&self, partition: i32) -> RepositoryResult<Option<i64>>;

    /// Count events by type
    async fn count_by_type(&self, event_type: EventType) -> RepositoryResult<i64>;

    /// Get total revenue (sum of amounts for PURCHASE events)
    async fn get_total_revenue(&self) -> RepositoryResult<i64>;
}

/// PostgreSQL implementation of EventRepository
pub struct PgEventRepository {
    pool: DbPool,
    retry_config: RetryConfig,
}

impl PgEventRepository {
    /// Create a new PostgreSQL event repository
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            retry_config: RetryConfig::default(),
        }
    }

    /// Create with custom retry configuration
    pub fn with_retry_config(pool: DbPool, retry_config: RetryConfig) -> Self {
        Self { pool, retry_config }
    }

    /// Execute a query with retry logic
    async fn execute_with_retry<F, T>(&self, operation: F) -> RepositoryResult<T>
    where
        F: Fn() -> futures::future::BoxFuture<'static, Result<T, RepositoryError>>,
    {
        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(self.retry_config.initial_backoff_ms),
            max_interval: Duration::from_millis(self.retry_config.max_backoff_ms),
            multiplier: self.retry_config.multiplier,
            max_elapsed_time: Some(Duration::from_secs(30)),
            ..Default::default()
        };

        retry(backoff, || async {
            match operation().await {
                Ok(value) => Ok(value),
                Err(e) if e.is_retryable() => {
                    tracing::warn!(error = ?e, "Retrying database operation");
                    Err(backoff::Error::transient(e))
                },
                Err(e) => Err(backoff::Error::permanent(e)),
            }
        })
        .await
    }

    /// Convert a database row to NormalizedEvent
    fn row_to_event(row: &sqlx::postgres::PgRow) -> RepositoryResult<NormalizedEvent> {
        let event_type_str: String = row.try_get("event_type")?;
        let event_type = EventType::from_str(&event_type_str)
            .map_err(|e| RepositoryError::Serialization(e.to_string()))?;

        Ok(NormalizedEvent {
            event_id: row.try_get("event_id")?,
            event_type,
            occurred_at: row.try_get("occurred_at")?,
            user_id: row.try_get("user_id")?,
            amount_cents: row.try_get("amount_cents")?,
            path: row.try_get("path")?,
            referrer: row.try_get("referrer")?,
            kafka_partition: row.try_get("src_partition")?,
            kafka_offset: row.try_get("src_offset")?,
            processed_at: row.try_get("processed_at")?,
        })
    }
}

#[async_trait]
impl Repository for PgEventRepository {
    type Entity = NormalizedEvent;
    type Id = Uuid;

    async fn find_by_id(&self, id: Uuid) -> RepositoryResult<Option<NormalizedEvent>> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            let id = id.clone();
            Box::pin(async move {
                let result = sqlx::query(
                    r#"
                    SELECT event_id, event_type, occurred_at, user_id, amount_cents,
                           path, referrer, processed_at, src_partition, src_offset
                    FROM events_normalized
                    WHERE event_id = $1
                    "#,
                )
                .bind(id)
                .fetch_optional(&pool)
                .await?;

                match result {
                    Some(row) => Ok(Some(Self::row_to_event(&row)?)),
                    None => Ok(None),
                }
            })
        })
        .await
    }

    async fn exists(&self, id: Uuid) -> RepositoryResult<bool> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            let id = id.clone();
            Box::pin(async move {
                let result = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM events_normalized WHERE event_id = $1)",
                )
                .bind(id)
                .fetch_one(&pool)
                .await?;

                Ok(result)
            })
        })
        .await
    }

    async fn delete(&self, id: Uuid) -> RepositoryResult<bool> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            let id = id.clone();
            Box::pin(async move {
                let result = sqlx::query("DELETE FROM events_normalized WHERE event_id = $1")
                    .bind(id)
                    .execute(&pool)
                    .await?;

                Ok(result.rows_affected() > 0)
            })
        })
        .await
    }

    async fn count(&self) -> RepositoryResult<i64> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            Box::pin(async move {
                let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events_normalized")
                    .fetch_one(&pool)
                    .await?;

                Ok(count)
            })
        })
        .await
    }

    async fn health_check(&self) -> RepositoryResult<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| RepositoryError::Connection(format!("Health check failed: {}", e)))
    }
}

#[async_trait]
impl UpsertRepository for PgEventRepository {
    async fn upsert(&self, entity: &NormalizedEvent) -> RepositoryResult<PgQueryResult> {
        let pool = self.pool.clone();
        let entity = entity.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            let entity = entity.clone();
            Box::pin(async move {
                let result = sqlx::query(
                    r#"
                    INSERT INTO events_normalized (
                        event_id, event_type, occurred_at, user_id, amount_cents,
                        path, referrer, processed_at, src_partition, src_offset
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (event_id) DO UPDATE SET
                        event_type = EXCLUDED.event_type,
                        occurred_at = EXCLUDED.occurred_at,
                        user_id = EXCLUDED.user_id,
                        amount_cents = EXCLUDED.amount_cents,
                        path = EXCLUDED.path,
                        referrer = EXCLUDED.referrer,
                        processed_at = EXCLUDED.processed_at,
                        src_partition = EXCLUDED.src_partition,
                        src_offset = EXCLUDED.src_offset
                    "#,
                )
                .bind(entity.event_id)
                .bind(entity.event_type.as_str())
                .bind(entity.occurred_at)
                .bind(entity.user_id)
                .bind(entity.amount_cents)
                .bind(&entity.path)
                .bind(&entity.referrer)
                .bind(entity.processed_at)
                .bind(entity.kafka_partition)
                .bind(entity.kafka_offset)
                .execute(&pool)
                .await?;

                Ok(result)
            })
        })
        .await
    }
}

#[async_trait]
impl EventRepository for PgEventRepository {
    async fn find_by_user(
        &self,
        user_id: Uuid,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> RepositoryResult<Vec<NormalizedEvent>> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            let user_id = user_id.clone();
            Box::pin(async move {
                let mut query = sqlx::query(
                    r#"
                    SELECT event_id, event_type, occurred_at, user_id, amount_cents,
                           path, referrer, processed_at, src_partition, src_offset
                    FROM events_normalized
                    WHERE user_id = $1
                    ORDER BY occurred_at DESC
                    "#,
                )
                .bind(user_id);

                if let Some(limit) = limit {
                    query = sqlx::query(
                        r#"
                        SELECT event_id, event_type, occurred_at, user_id, amount_cents,
                               path, referrer, processed_at, src_partition, src_offset
                        FROM events_normalized
                        WHERE user_id = $1
                        ORDER BY occurred_at DESC
                        LIMIT $2 OFFSET $3
                        "#,
                    )
                    .bind(user_id)
                    .bind(limit)
                    .bind(offset.unwrap_or(0));
                }

                let rows = query.fetch_all(&pool).await?;
                rows.iter().map(Self::row_to_event).collect()
            })
        })
        .await
    }

    async fn find_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
    ) -> RepositoryResult<Vec<NormalizedEvent>> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            Box::pin(async move {
                let mut query_str = r#"
                    SELECT event_id, event_type, occurred_at, user_id, amount_cents,
                           path, referrer, processed_at, src_partition, src_offset
                    FROM events_normalized
                    WHERE occurred_at >= $1 AND occurred_at <= $2
                    ORDER BY occurred_at DESC
                "#;

                if limit.is_some() {
                    query_str = r#"
                        SELECT event_id, event_type, occurred_at, user_id, amount_cents,
                               path, referrer, processed_at, src_partition, src_offset
                        FROM events_normalized
                        WHERE occurred_at >= $1 AND occurred_at <= $2
                        ORDER BY occurred_at DESC
                        LIMIT $3
                    "#;
                }

                let mut query = sqlx::query(query_str).bind(start).bind(end);

                if let Some(limit) = limit {
                    query = query.bind(limit);
                }

                let rows = query.fetch_all(&pool).await?;
                rows.iter().map(Self::row_to_event).collect()
            })
        })
        .await
    }

    async fn find_by_kafka_position(
        &self,
        partition: i32,
        offset: i64,
    ) -> RepositoryResult<Option<NormalizedEvent>> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            Box::pin(async move {
                let result = sqlx::query(
                    r#"
                    SELECT event_id, event_type, occurred_at, user_id, amount_cents,
                           path, referrer, processed_at, src_partition, src_offset
                    FROM events_normalized
                    WHERE src_partition = $1 AND src_offset = $2
                    "#,
                )
                .bind(partition)
                .bind(offset)
                .fetch_optional(&pool)
                .await?;

                match result {
                    Some(row) => Ok(Some(Self::row_to_event(&row)?)),
                    None => Ok(None),
                }
            })
        })
        .await
    }

    async fn get_latest_offset(&self, partition: i32) -> RepositoryResult<Option<i64>> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            Box::pin(async move {
                let offset = sqlx::query_scalar::<_, Option<i64>>(
                    "SELECT MAX(src_offset) FROM events_normalized WHERE src_partition = $1",
                )
                .bind(partition)
                .fetch_one(&pool)
                .await?;

                Ok(offset)
            })
        })
        .await
    }

    async fn count_by_type(&self, event_type: EventType) -> RepositoryResult<i64> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            let event_type = event_type.clone();
            Box::pin(async move {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM events_normalized WHERE event_type = $1",
                )
                .bind(event_type.as_str())
                .fetch_one(&pool)
                .await?;

                Ok(count)
            })
        })
        .await
    }

    async fn get_total_revenue(&self) -> RepositoryResult<i64> {
        let pool = self.pool.clone();

        self.execute_with_retry(|| {
            let pool = pool.clone();
            Box::pin(async move {
                let total = sqlx::query_scalar::<_, i64>(
                    "SELECT COALESCE(SUM(amount_cents), 0)::BIGINT FROM events_normalized WHERE event_type = 'PURCHASE'",
                )
                .fetch_one(&pool)
                .await?;

                Ok(total)
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_creation() {
        let config = RetryConfig::new(5).with_initial_backoff(200).with_max_backoff(5000);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5000);
    }
}
