//! Message processing logic for Kafka events

use crate::db::EventRepository;
use crate::error::{Error, Result};
use crate::models::{NormalizedEvent, RawEvent};
use async_trait::async_trait;
use backoff::ExponentialBackoff;
use chrono::Utc;
use rdkafka::message::{BorrowedMessage, Message};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::{DlqMessage, KafkaIntegrationError, MessageMetadata, ProcessingResult};

/// Message processor that handles the event processing pipeline
pub struct MessageProcessor {
    /// Database repository for storing events
    db_repo: Arc<dyn EventRepository>,

    /// Maximum number of retries for transient errors
    max_retries: u32,

    /// Backoff configuration for retries
    retry_backoff: Duration,
}

impl MessageProcessor {
    /// Create a new message processor
    pub fn new(
        db_repo: Arc<dyn EventRepository>,
        max_retries: u32,
        retry_backoff: Duration,
    ) -> Self {
        Self {
            db_repo,
            max_retries,
            retry_backoff,
        }
    }

    /// Process a single Kafka message
    pub async fn process_message<'a>(&self, message: &BorrowedMessage<'a>) -> ProcessingResult {
        // Extract message metadata
        let metadata = MessageMetadata {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            timestamp: message.timestamp().to_millis(),
            key: message.key().map(|k| k.to_vec()),
        };

        debug!(
            "Processing message from topic: {}, partition: {}, offset: {}",
            metadata.topic, metadata.partition, metadata.offset
        );

        // Extract payload
        let payload = match message.payload() {
            Some(data) => data,
            None => {
                error!("Message has no payload");
                return ProcessingResult::PermanentError("Empty message payload".to_string());
            },
        };

        // Parse JSON to RawEvent
        let raw_event = match self.parse_raw_event(payload) {
            Ok(event) => event,
            Err(e) => {
                error!("Failed to parse message: {}", e);
                return ProcessingResult::PermanentError(format!("Parse error: {}", e));
            },
        };

        // Validate and transform to NormalizedEvent
        let mut normalized_event = match self.transform_event(raw_event, &metadata) {
            Ok(event) => event,
            Err(e) => {
                error!("Failed to transform event: {}", e);
                return ProcessingResult::PermanentError(format!("Validation error: {}", e));
            },
        };

        // Add Kafka metadata
        normalized_event.kafka_partition = metadata.partition;
        normalized_event.kafka_offset = metadata.offset;

        // Store in database with retries
        match self.store_event_with_retry(normalized_event).await {
            Ok(_) => {
                info!(
                    "Successfully processed message from partition {} offset {}",
                    metadata.partition, metadata.offset
                );
                ProcessingResult::Success
            },
            Err(e) => {
                error!(
                    "Failed to store event after {} retries: {}",
                    self.max_retries, e
                );
                ProcessingResult::RetryableError(format!("Database error: {}", e))
            },
        }
    }

    /// Parse raw bytes into RawEvent
    fn parse_raw_event(&self, payload: &[u8]) -> Result<RawEvent> {
        let json_str = std::str::from_utf8(payload)
            .map_err(|e| Error::from(KafkaIntegrationError::DeserializationError(e.to_string())))?;

        serde_json::from_str(json_str)
            .map_err(|e| Error::from(KafkaIntegrationError::DeserializationError(e.to_string())))
    }

    /// Transform and validate RawEvent into NormalizedEvent
    fn transform_event(
        &self,
        raw_event: RawEvent,
        _metadata: &MessageMetadata,
    ) -> Result<NormalizedEvent> {
        // Use the TryFrom implementation for validation and transformation
        let mut normalized: NormalizedEvent = raw_event.try_into()?;

        // Add processing timestamp
        normalized.processed_at = Utc::now();

        Ok(normalized)
    }

    /// Store event with exponential backoff retry
    async fn store_event_with_retry(&self, event: NormalizedEvent) -> Result<()> {
        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(30)),
            initial_interval: self.retry_backoff,
            multiplier: 2.0,
            max_interval: Duration::from_secs(10),
            ..Default::default()
        };

        let operation = || async {
            match self.db_repo.upsert(&event).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    warn!("Database write failed, will retry: {}", e);
                    Err(backoff::Error::transient(e))
                },
            }
        };

        backoff::future::retry(backoff, operation)
            .await
            .map_err(|e| Error::database(format!("Database operation failed: {}", e)))
    }

    /// Create a DLQ message from a failed processing attempt
    pub fn create_dlq_message<'a>(
        &self,
        message: &BorrowedMessage<'a>,
        error: String,
        error_type: String,
        retry_count: u32,
    ) -> DlqMessage {
        DlqMessage {
            original_message: message.payload().map(|p| p.to_vec()).unwrap_or_default(),
            error,
            error_type,
            timestamp: Utc::now(),
            partition: message.partition(),
            offset: message.offset(),
            retry_count,
            source_topic: message.topic().to_string(),
        }
    }
}

#[async_trait]
impl Clone for MessageProcessor {
    fn clone(&self) -> Self {
        Self {
            db_repo: Arc::clone(&self.db_repo),
            max_retries: self.max_retries,
            retry_backoff: self.retry_backoff,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MockEventRepository;
    use crate::models::EventType;

    #[tokio::test]
    async fn test_parse_raw_event() {
        let mock_repo = Arc::new(MockEventRepository::new());
        let processor = MessageProcessor::new(mock_repo, 3, Duration::from_millis(100));

        let event_json = r#"{
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_type": "CLICK",
            "occurred_at": "2024-01-15T10:30:00Z",
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "amount_cents": 1000,
            "path": "/products/123",
            "referrer": "google.com"
        }"#;

        let result = processor.parse_raw_event(event_json.as_bytes());
        assert!(result.is_ok());

        let raw_event = result.unwrap();
        assert_eq!(raw_event.event_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(raw_event.event_type, "CLICK");
    }

    #[tokio::test]
    async fn test_parse_invalid_json() {
        let mock_repo = Arc::new(MockEventRepository::new());
        let processor = MessageProcessor::new(mock_repo, 3, Duration::from_millis(100));

        let result = processor.parse_raw_event(b"invalid json");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_transform_event() {
        let mock_repo = Arc::new(MockEventRepository::new());
        let processor = MessageProcessor::new(mock_repo, 3, Duration::from_millis(100));

        let raw_event = RawEvent {
            event_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            event_type: "PURCHASE".to_string(),
            occurred_at: "2024-01-15T10:30:00Z".to_string(),
            user_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            amount_cents: Some(1000),
            path: Some("/checkout".to_string()),
            referrer: None,
        };

        let metadata = MessageMetadata {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            timestamp: None,
            key: None,
        };

        let result = processor.transform_event(raw_event, &metadata);
        assert!(result.is_ok());

        let normalized = result.unwrap();
        assert_eq!(normalized.event_type, EventType::Purchase);
        assert_eq!(normalized.amount_cents, Some(1000));
    }

    #[tokio::test]
    async fn test_store_event_with_retry() {
        let mock_repo = Arc::new(MockEventRepository::new());
        let processor = MessageProcessor::new(mock_repo.clone(), 3, Duration::from_millis(100));

        let event = NormalizedEvent {
            event_id: uuid::Uuid::new_v4(),
            event_type: EventType::Click,
            occurred_at: Utc::now(),
            user_id: Some(uuid::Uuid::new_v4()),
            amount_cents: None,
            path: Some("/test".to_string()),
            referrer: None,
            kafka_partition: 0,
            kafka_offset: 100,
            processed_at: Utc::now(),
        };

        let result = processor.store_event_with_retry(event.clone()).await;
        assert!(result.is_ok());

        // Verify event was stored
        let stored = mock_repo.get_event(&event.event_id).await.unwrap();
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().event_id, event.event_id);
    }
}
