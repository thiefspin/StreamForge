//! Test utilities for StreamForge
//!
//! This module provides mock implementations and utilities for testing.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::db::repository::{Repository, RepositoryResult, UpsertRepository};
use crate::db::EventRepository;
use crate::models::{EventType, NormalizedEvent};

/// Mock implementation of EventRepository for testing
#[derive(Debug, Clone)]
pub struct MockEventRepository {
    events: Arc<Mutex<Vec<NormalizedEvent>>>,
    fail_next: Arc<Mutex<bool>>,
    error_message: Arc<Mutex<Option<String>>>,
}

impl Default for MockEventRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl MockEventRepository {
    /// Create a new mock repository
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            fail_next: Arc::new(Mutex::new(false)),
            error_message: Arc::new(Mutex::new(None)),
        }
    }

    /// Configure the mock to fail on the next operation
    pub fn fail_next_operation(&self, error_message: &str) {
        *self.fail_next.lock().unwrap() = true;
        *self.error_message.lock().unwrap() = Some(error_message.to_string());
    }

    /// Get all stored events
    pub fn get_all_events(&self) -> Vec<NormalizedEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Clear all events
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    /// Add an event directly
    pub fn add_event(&self, event: NormalizedEvent) {
        self.events.lock().unwrap().push(event);
    }

    fn check_failure(&self) -> RepositoryResult<()> {
        let mut fail = self.fail_next.lock().unwrap();
        if *fail {
            *fail = false;
            let msg = self
                .error_message
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "Mock failure".to_string());
            return Err(crate::db::RepositoryError::QueryExecution(msg));
        }
        Ok(())
    }
}

#[async_trait]
impl Repository for MockEventRepository {
    type Entity = NormalizedEvent;
    type Id = Uuid;

    async fn find_by_id(&self, id: Self::Id) -> RepositoryResult<Option<Self::Entity>> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        Ok(events.iter().find(|e| e.event_id == id).cloned())
    }

    async fn exists(&self, id: Self::Id) -> RepositoryResult<bool> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        Ok(events.iter().any(|e| e.event_id == id))
    }

    async fn delete(&self, id: Self::Id) -> RepositoryResult<bool> {
        self.check_failure()?;
        let mut events = self.events.lock().unwrap();
        let initial_len = events.len();
        events.retain(|e| e.event_id != id);
        Ok(events.len() < initial_len)
    }

    async fn count(&self) -> RepositoryResult<i64> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        Ok(events.len() as i64)
    }

    async fn health_check(&self) -> RepositoryResult<()> {
        self.check_failure()?;
        Ok(())
    }
}

#[async_trait]
impl UpsertRepository for MockEventRepository {
    async fn upsert(&self, entity: &NormalizedEvent) -> RepositoryResult<NormalizedEvent> {
        self.check_failure()?;
        let mut events = self.events.lock().unwrap();
        if let Some(pos) = events.iter().position(|e| e.event_id == entity.event_id) {
            events[pos] = entity.clone();
        } else {
            events.push(entity.clone());
        }
        Ok(entity.clone())
    }
}

#[async_trait]
impl EventRepository for MockEventRepository {
    async fn find_by_user(
        &self,
        user_id: Uuid,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> RepositoryResult<Vec<NormalizedEvent>> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        let filtered: Vec<_> = events
            .iter()
            .filter(|e| e.user_id == Some(user_id))
            .skip(offset.unwrap_or(0) as usize)
            .take(limit.unwrap_or(i64::MAX) as usize)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn find_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
    ) -> RepositoryResult<Vec<NormalizedEvent>> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        let filtered: Vec<_> = events
            .iter()
            .filter(|e| e.occurred_at >= start && e.occurred_at <= end)
            .take(limit.unwrap_or(i64::MAX) as usize)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn find_by_kafka_position(
        &self,
        partition: i32,
        offset: i64,
    ) -> RepositoryResult<Option<NormalizedEvent>> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .find(|e| e.kafka_partition == partition && e.kafka_offset == offset)
            .cloned())
    }

    async fn get_latest_offset(&self, partition: i32) -> RepositoryResult<Option<i64>> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .filter(|e| e.kafka_partition == partition)
            .map(|e| e.kafka_offset)
            .max())
    }

    async fn count_by_type(&self, event_type: EventType) -> RepositoryResult<i64> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        Ok(events.iter().filter(|e| e.event_type == event_type).count() as i64)
    }

    async fn get_total_revenue(&self) -> RepositoryResult<i64> {
        self.check_failure()?;
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .filter(|e| e.event_type == EventType::Purchase)
            .filter_map(|e| e.amount_cents)
            .sum())
    }

    async fn upsert_event(&self, event: &NormalizedEvent) -> RepositoryResult<NormalizedEvent> {
        self.upsert(event).await
    }

    async fn get_event(&self, event_id: &Uuid) -> RepositoryResult<Option<NormalizedEvent>> {
        self.find_by_id(*event_id).await
    }
}

/// Create a test NormalizedEvent with default values
pub fn create_test_event() -> NormalizedEvent {
    NormalizedEvent {
        event_id: Uuid::new_v4(),
        event_type: EventType::View,
        occurred_at: Utc::now(),
        user_id: Some(Uuid::new_v4()),
        amount_cents: None,
        path: Some("/test".to_string()),
        referrer: None,
        kafka_partition: 0,
        kafka_offset: 0,
        processed_at: Utc::now(),
    }
}

/// Create a test NormalizedEvent with specified type
pub fn create_test_event_with_type(event_type: EventType) -> NormalizedEvent {
    let mut event = create_test_event();
    event.event_type = event_type;
    if event_type == EventType::Purchase {
        event.amount_cents = Some(9999);
    }
    event
}

/// Create multiple test events
pub fn create_test_events(count: usize) -> Vec<NormalizedEvent> {
    (0..count).map(|_| create_test_event()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_repository() {
        let repo = MockEventRepository::new();
        let event = create_test_event();

        // Test upsert
        let created = repo.upsert(&event).await.unwrap();
        assert_eq!(created.event_id, event.event_id);

        // Test find_by_id
        let found = repo.find_by_id(event.event_id).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().event_id, event.event_id);

        // Test exists
        let exists = repo.exists(event.event_id).await.unwrap();
        assert!(exists);

        // Test count
        let count = repo.count().await.unwrap();
        assert_eq!(count, 1);

        // Test delete
        let deleted = repo.delete(event.event_id).await.unwrap();
        assert!(deleted);
        let count = repo.count().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_mock_repository_failure() {
        let repo = MockEventRepository::new();
        let event = create_test_event();

        // Configure to fail
        repo.fail_next_operation("Test error");

        // Should fail
        let result = repo.upsert(&event).await;
        assert!(result.is_err());

        // Should succeed after failure
        let result = repo.upsert(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_event_repository_methods() {
        let repo = MockEventRepository::new();

        // Create events with different users
        let user1 = Uuid::new_v4();
        let user2 = Uuid::new_v4();

        for i in 0..5 {
            let mut event = create_test_event();
            event.user_id = Some(if i % 2 == 0 { user1 } else { user2 });
            event.kafka_offset = i;
            repo.upsert(&event).await.unwrap();
        }

        // Test find_by_user
        let user1_events = repo.find_by_user(user1, None, None).await.unwrap();
        assert_eq!(user1_events.len(), 3);

        // Test get_latest_offset
        let latest = repo.get_latest_offset(0).await.unwrap();
        assert_eq!(latest, Some(4));

        // Test count_by_type
        let count = repo.count_by_type(EventType::View).await.unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_health_check() {
        let repo = MockEventRepository::new();

        // Should succeed normally
        let result = repo.health_check().await;
        assert!(result.is_ok());

        // Should fail when configured to fail
        repo.fail_next_operation("Health check failed");
        let result = repo.health_check().await;
        assert!(result.is_err());
    }
}
