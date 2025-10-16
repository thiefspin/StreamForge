//! Kafka integration module for event streaming
//!
//! This module provides:
//! - Event consumer with manual offset management
//! - Dead Letter Queue (DLQ) producer for failed messages
//! - Message processing pipeline with retries
//! - Graceful shutdown with offset commits

mod config;
mod consumer;
mod processor;
mod producer;

pub use config::KafkaConfig;
pub use consumer::EventConsumer;
pub use processor::MessageProcessor;
pub use producer::DlqProducer;

use rdkafka::error::KafkaError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Kafka-specific error types
#[derive(Debug, Error)]
pub enum KafkaIntegrationError {
    #[error("Kafka connection error: {0}")]
    ConnectionError(#[from] KafkaError),

    #[error("Message deserialization failed: {0}")]
    DeserializationError(String),

    #[error("Offset commit failed: {0}")]
    OffsetCommitError(String),

    #[error("DLQ send failed: {0}")]
    DlqError(String),

    #[error("Consumer group rebalance in progress")]
    RebalanceError,
}

/// Dead Letter Queue message format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    /// Original message payload that failed processing
    pub original_message: Vec<u8>,

    /// Error description
    pub error: String,

    /// Error type/category
    pub error_type: String,

    /// Timestamp when the error occurred
    pub timestamp: chrono::DateTime<chrono::Utc>,

    /// Source Kafka partition
    pub partition: i32,

    /// Source Kafka offset
    pub offset: i64,

    /// Number of retry attempts
    pub retry_count: u32,

    /// Source topic
    pub source_topic: String,
}

/// Kafka message metadata
#[derive(Debug, Clone)]
pub struct MessageMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
}

/// Processing result for a Kafka message
#[derive(Debug)]
pub enum ProcessingResult {
    /// Message was successfully processed
    Success,

    /// Message failed but can be retried
    RetryableError(String),

    /// Message failed and should go to DLQ
    PermanentError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_message_serialization() {
        let dlq_msg = DlqMessage {
            original_message: vec![1, 2, 3],
            error: "Test error".to_string(),
            error_type: "TestError".to_string(),
            timestamp: chrono::Utc::now(),
            partition: 0,
            offset: 100,
            retry_count: 2,
            source_topic: "events".to_string(),
        };

        let json = serde_json::to_string(&dlq_msg).unwrap();
        let deserialized: DlqMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.original_message, dlq_msg.original_message);
        assert_eq!(deserialized.error, dlq_msg.error);
        assert_eq!(deserialized.partition, dlq_msg.partition);
    }
}
