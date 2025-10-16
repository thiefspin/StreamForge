//! Database integration tests for StreamForge
//!
//! These tests verify database operations using testcontainers for
//! isolated PostgreSQL instances.

use chrono::Utc;
use std::time::Duration;
use streamforge::{
    config::DatabaseConfig,
    db::{
        event_repo::{EventRepository, PgEventRepository},
        pool::create_pool,
        repository::{Repository, RetryConfig, UpsertRepository},
        run_migrations,
    },
    models::event::{EventType, NormalizedEvent},
};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

/// Test container setup
struct TestDb {
    _container: ContainerAsync<Postgres>,
    connection_string: String,
}

impl TestDb {
    /// Create a new test database container
    async fn new() -> Self {
        let postgres = Postgres::default()
            .with_db_name("streamforge_test")
            .with_user("test_user")
            .with_password("test_password");

        let container = postgres.start().await.expect("Failed to start postgres container");
        let port = container.get_host_port_ipv4(5432.tcp()).await.expect("Failed to get port");

        let connection_string = format!(
            "postgresql://test_user:test_password@127.0.0.1:{}/streamforge_test",
            port
        );

        // Wait for PostgreSQL to be ready
        tokio::time::sleep(Duration::from_secs(3)).await;

        Self {
            _container: container,
            connection_string,
        }
    }

    /// Get database configuration
    fn config(&self) -> DatabaseConfig {
        DatabaseConfig {
            url: self.connection_string.clone(),
            pool_max_size: 5,
            pool_min_idle: 1,
            pool_timeout_seconds: 30,
            pool_idle_timeout_seconds: 600,
        }
    }
}

/// Create a test event
fn create_test_event(event_type: EventType) -> NormalizedEvent {
    let mut event = NormalizedEvent {
        event_id: Uuid::new_v4(),
        event_type,
        occurred_at: Utc::now(),
        user_id: Some(Uuid::new_v4()),
        amount_cents: None,
        path: Some("/test".to_string()),
        referrer: Some("https://example.com".to_string()),
        kafka_partition: 0,
        kafka_offset: 0,
        processed_at: Utc::now(),
    };

    // Add amount for purchase events
    if event_type == EventType::Purchase {
        event.amount_cents = Some(2500);
    }

    event
}

#[tokio::test]
async fn test_database_connection_and_migrations() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    // Run migrations
    run_migrations(&pool).await.expect("Failed to run migrations");

    // Verify table exists
    let result = sqlx::query("SELECT COUNT(*) FROM events_normalized").fetch_one(&pool).await;

    assert!(result.is_ok(), "events_normalized table should exist");
}

#[tokio::test]
async fn test_event_repository_upsert() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());
    let event = create_test_event(EventType::Click);

    // First upsert
    let result = repo.upsert(&event).await;
    assert!(result.is_ok());
    assert!(result.is_ok());

    // Verify event was inserted
    let found = repo.find_by_id(event.event_id).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().event_id, event.event_id);
}

#[tokio::test]
async fn test_idempotent_upsert() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());
    let event = create_test_event(EventType::View);

    // Insert the same event multiple times
    for _ in 0..3 {
        let result = repo.upsert(&event).await;
        assert!(result.is_ok());
    }

    // Verify only one event exists
    let count = repo.count().await.unwrap();
    assert_eq!(count, 1);

    // Verify the event data is correct
    let found = repo.find_by_id(event.event_id).await.unwrap().unwrap();
    assert_eq!(found.event_id, event.event_id);
    assert_eq!(found.event_type, event.event_type);
}

#[tokio::test]
async fn test_upsert_with_update() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());
    let mut event = create_test_event(EventType::Click);

    // First insert
    repo.upsert(&event).await.unwrap();

    // Update the event
    event.path = Some("/updated".to_string());
    event.referrer = Some("https://updated.com".to_string());

    // Upsert the updated event
    repo.upsert(&event).await.unwrap();

    // Verify the event was updated
    let found = repo.find_by_id(event.event_id).await.unwrap().unwrap();
    assert_eq!(found.path, Some("/updated".to_string()));
    assert_eq!(found.referrer, Some("https://updated.com".to_string()));
}

#[tokio::test]
async fn test_find_by_user() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());
    let user_id = Uuid::new_v4();

    // Insert events for the user
    for i in 0..5 {
        let mut event = create_test_event(EventType::Click);
        event.user_id = Some(user_id);
        event.kafka_offset = i;
        repo.upsert(&event).await.unwrap();
    }

    // Insert events for another user
    for i in 0..3 {
        let mut event = create_test_event(EventType::View);
        event.kafka_offset = i + 10;
        repo.upsert(&event).await.unwrap();
    }

    // Find events for the first user
    let events = repo.find_by_user(user_id, Some(10), None).await.unwrap();
    assert_eq!(events.len(), 5);
    assert!(events.iter().all(|e| e.user_id == Some(user_id)));

    // Test pagination
    let page1 = repo.find_by_user(user_id, Some(2), Some(0)).await.unwrap();
    assert_eq!(page1.len(), 2);

    let page2 = repo.find_by_user(user_id, Some(2), Some(2)).await.unwrap();
    assert_eq!(page2.len(), 2);
}

#[tokio::test]
async fn test_find_by_time_range() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());
    let now = Utc::now();

    // Insert events with different timestamps
    for i in 0..10 {
        let mut event = create_test_event(EventType::Click);
        event.occurred_at = now - chrono::Duration::hours(i);
        event.kafka_offset = i;
        repo.upsert(&event).await.unwrap();
    }

    // Find events in the last 5 hours
    let start = now - chrono::Duration::hours(5);
    let end = now;
    let events = repo.find_by_time_range(start, end, None).await.unwrap();
    assert_eq!(events.len(), 6); // 0-5 hours ago

    // Test with limit
    let limited = repo.find_by_time_range(start, end, Some(3)).await.unwrap();
    assert_eq!(limited.len(), 3);
}

#[tokio::test]
async fn test_find_by_kafka_position() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());
    let mut event = create_test_event(EventType::Purchase);
    event.kafka_partition = 3;
    event.kafka_offset = 12345;

    repo.upsert(&event).await.unwrap();

    // Find by exact position
    let found = repo.find_by_kafka_position(3, 12345).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().event_id, event.event_id);

    // Try non-existent position
    let not_found = repo.find_by_kafka_position(3, 99999).await.unwrap();
    assert!(not_found.is_none());
}

#[tokio::test]
async fn test_get_latest_offset() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());

    // Insert events with different offsets
    for offset in [100, 200, 150, 300, 250] {
        let mut event = create_test_event(EventType::Click);
        event.kafka_partition = 1;
        event.kafka_offset = offset;
        repo.upsert(&event).await.unwrap();
    }

    // Get latest offset
    let latest = repo.get_latest_offset(1).await.unwrap();
    assert_eq!(latest, Some(300));

    // Check empty partition
    let empty = repo.get_latest_offset(999).await.unwrap();
    assert_eq!(empty, None);
}

#[tokio::test]
async fn test_count_by_type() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());

    // Insert different event types
    for i in 0..5 {
        let mut event = create_test_event(EventType::Click);
        event.kafka_offset = i;
        repo.upsert(&event).await.unwrap();
    }

    for i in 0..3 {
        let mut event = create_test_event(EventType::View);
        event.kafka_offset = i + 10;
        repo.upsert(&event).await.unwrap();
    }

    for i in 0..2 {
        let mut event = create_test_event(EventType::Purchase);
        event.kafka_offset = i + 20;
        repo.upsert(&event).await.unwrap();
    }

    // Count by type
    assert_eq!(repo.count_by_type(EventType::Click).await.unwrap(), 5);
    assert_eq!(repo.count_by_type(EventType::View).await.unwrap(), 3);
    assert_eq!(repo.count_by_type(EventType::Purchase).await.unwrap(), 2);
}

#[tokio::test]
async fn test_get_total_revenue() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());

    // Insert purchase events with different amounts
    let amounts = vec![1000, 2500, 1500, 3000];
    for (i, amount) in amounts.iter().enumerate() {
        let mut event = create_test_event(EventType::Purchase);
        event.amount_cents = Some(*amount);
        event.kafka_offset = i as i64;
        repo.upsert(&event).await.unwrap();
    }

    // Insert non-purchase events (shouldn't affect revenue)
    for i in 0..3 {
        let mut event = create_test_event(EventType::Click);
        event.kafka_offset = i + 10;
        repo.upsert(&event).await.unwrap();
    }

    // Check total revenue
    let total = repo.get_total_revenue().await.unwrap();
    assert_eq!(total, 8000); // Sum of amounts
}

#[tokio::test]
async fn test_retry_on_transient_errors() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    // Create repository with custom retry config
    let retry_config = RetryConfig::new(3).with_initial_backoff(10).with_max_backoff(100);

    let repo = PgEventRepository::with_retry_config(pool.clone(), retry_config);

    // This should work even if there are transient issues
    let event = create_test_event(EventType::Click);
    let result = repo.upsert(&event).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_repository_crud_operations() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());
    let event = create_test_event(EventType::View);

    // Create
    repo.upsert(&event).await.unwrap();

    // Read
    let found = repo.find_by_id(event.event_id).await.unwrap();
    assert!(found.is_some());

    // Exists
    assert!(repo.exists(event.event_id).await.unwrap());

    // Count
    let count = repo.count().await.unwrap();
    assert_eq!(count, 1);

    // Delete
    let deleted = repo.delete(event.event_id).await.unwrap();
    assert!(deleted);

    // Verify deletion
    assert!(!repo.exists(event.event_id).await.unwrap());
    assert_eq!(repo.count().await.unwrap(), 0);
}

#[tokio::test]
async fn test_health_check() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());

    // Health check should succeed
    let result = repo.health_check().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_concurrent_upserts() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());

    // Test concurrent upserts of different events
    let mut handles = vec![];

    for i in 0..10 {
        let repo_clone = PgEventRepository::new(pool.clone());
        // Create a unique event for each task
        let mut event = create_test_event(EventType::Click);
        // Each event needs a unique event_id AND unique kafka offset
        event.event_id = Uuid::new_v4();
        event.kafka_offset = 100 + i as i64;

        let handle = tokio::spawn(async move { repo_clone.upsert(&event).await });
        handles.push(handle);
    }

    // Wait for all tasks - all should succeed
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap();
        if let Err(e) = &result {
            eprintln!("Task {} failed with error: {:?}", i, e);
        }
        assert!(result.is_ok(), "Task {} failed", i);
    }

    // Should have 10 different events (different event_ids)
    let count = repo.count().await.unwrap();
    assert_eq!(
        count, 10,
        "Expected 10 events after concurrent upserts, but found {}",
        count
    );

    // Now test concurrent upserts of the SAME event_id
    // This tests the idempotency of upserts
    let mut same_event = create_test_event(EventType::Purchase);
    let same_event_id = same_event.event_id;

    // Important: For true idempotency testing, we need to use different kafka positions
    // for each concurrent attempt, because the unique constraint on (partition, offset)
    // would prevent multiple inserts with the same position.
    // In real usage, the same event_id would come from the same kafka position,
    // but for testing concurrent behavior, we simulate different delivery attempts.
    let mut handles = vec![];

    for i in 0..5 {
        let repo_clone = PgEventRepository::new(pool.clone());
        let mut event_clone = same_event.clone();
        // Use different kafka offsets for each concurrent attempt
        event_clone.kafka_offset = 2000 + i as i64;
        // Track which task last updated (for debugging)
        event_clone.path = Some(format!("/test-{}", i));

        let handle = tokio::spawn(async move { repo_clone.upsert(&event_clone).await });
        handles.push(handle);
    }

    // Wait for all tasks
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap();
        if let Err(e) = &result {
            eprintln!("Idempotent upsert task {} failed with error: {:?}", i, e);
        }
        assert!(result.is_ok(), "Idempotent upsert task {} failed", i);
    }

    // Should have 11 events total (10 from before + 1 new with same event_id)
    let count = repo.count().await.unwrap();
    assert_eq!(count, 11, "Expected 11 events total, but found {}", count);

    // Verify the event exists
    let stored = repo.find_by_id(same_event_id).await.unwrap();
    assert!(
        stored.is_some(),
        "Event should exist after concurrent upserts"
    );
}

#[tokio::test]
async fn test_constraint_violations() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    let repo = PgEventRepository::new(pool.clone());

    // Insert an event
    let mut event1 = create_test_event(EventType::Click);
    event1.kafka_partition = 1;
    event1.kafka_offset = 100;
    repo.upsert(&event1).await.unwrap();

    // Try to insert a different event with the same kafka position
    // This should fail due to the unique constraint on (partition, offset)
    let mut event2 = create_test_event(EventType::View);
    event2.kafka_partition = 1;
    event2.kafka_offset = 100;

    let result = repo.upsert(&event2).await;
    // This will fail because we have a different event_id but same kafka position
    assert!(result.is_err());

    // Verify the original event is still there unchanged
    let found = repo.find_by_kafka_position(1, 100).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().event_type, EventType::Click);
}

#[tokio::test]
async fn test_pool_metrics() {
    let test_db = TestDb::new().await;
    let pool = create_pool(&test_db.config()).await.expect("Failed to create pool");

    run_migrations(&pool).await.expect("Failed to run migrations");

    // Check pool metrics
    let metrics = streamforge::db::pool::PoolMetrics::from_pool(&pool);
    assert!(metrics.is_healthy());
    assert!(metrics.size > 0);
    assert!(metrics.max_size > 0);
}
