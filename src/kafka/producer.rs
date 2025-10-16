//! Dead Letter Queue producer for failed messages

use super::{DlqMessage, KafkaIntegrationError};
use crate::error::{Error, Result};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::time::Duration;
use tracing::{error, info};

/// Producer for sending messages to the Dead Letter Queue
pub struct DlqProducer {
    /// Kafka producer instance
    producer: FutureProducer,

    /// DLQ topic name
    dlq_topic: String,

    /// Timeout for send operations
    send_timeout: Duration,
}

impl DlqProducer {
    /// Create a new DLQ producer
    pub fn new(config: ClientConfig, dlq_topic: String) -> Result<Self> {
        let producer: FutureProducer = config
            .create()
            .map_err(|e| Error::from(KafkaIntegrationError::ConnectionError(e)))?;

        Ok(Self {
            producer,
            dlq_topic,
            send_timeout: Duration::from_secs(30),
        })
    }

    /// Send a message to the Dead Letter Queue
    pub async fn send(&self, message: DlqMessage) -> Result<()> {
        // Serialize the DLQ message
        let payload = serde_json::to_string(&message).map_err(|e| {
            Error::from(KafkaIntegrationError::DlqError(format!(
                "Failed to serialize DLQ message: {}",
                e
            )))
        })?;

        // Create a key based on the original partition and offset
        let key = format!("{}-{}", message.partition, message.offset);

        // Create the Kafka record
        let record = FutureRecord::to(&self.dlq_topic).payload(&payload).key(&key);

        // Send the message
        let delivery_result = self.producer.send(record, self.send_timeout).await;

        match delivery_result {
            Ok(delivery) => {
                let partition = delivery.partition;
                let offset = delivery.offset;
                info!(
                    "Sent message to DLQ topic '{}' partition {} offset {}",
                    self.dlq_topic, partition, offset
                );
                Ok(())
            },
            Err((kafka_error, _)) => {
                error!(
                    "Failed to send message to DLQ topic '{}': {}",
                    self.dlq_topic, kafka_error
                );
                Err(Error::from(KafkaIntegrationError::DlqError(format!(
                    "Failed to send to DLQ: {}",
                    kafka_error
                ))))
            },
        }
    }

    /// Send a batch of messages to the DLQ
    pub async fn send_batch(&self, messages: Vec<DlqMessage>) -> Result<()> {
        let mut errors = Vec::new();

        for message in messages {
            if let Err(e) = self.send(message).await {
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            error!("Failed to send {} messages to DLQ", errors.len());
            return Err(Error::from(KafkaIntegrationError::DlqError(format!(
                "Failed to send {} messages to DLQ",
                errors.len()
            ))));
        }

        Ok(())
    }

    /// Flush any pending messages
    pub fn flush(&self) -> Result<()> {
        let _ = self.producer.flush(self.send_timeout);
        Ok(())
    }
}

impl Clone for DlqProducer {
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            dlq_topic: self.dlq_topic.clone(),
            send_timeout: self.send_timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rdkafka::ClientConfig;

    fn create_test_config() -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000");
        config
    }

    fn create_test_dlq_message() -> DlqMessage {
        DlqMessage {
            original_message: vec![1, 2, 3],
            error: "Test error".to_string(),
            error_type: "TestError".to_string(),
            timestamp: Utc::now(),
            partition: 0,
            offset: 100,
            retry_count: 2,
            source_topic: "events".to_string(),
        }
    }

    #[test]
    fn test_dlq_producer_creation() {
        let config = create_test_config();
        let result = DlqProducer::new(config, "events-dlq".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_dlq_message_serialization() {
        let message = create_test_dlq_message();
        let json = serde_json::to_string(&message);
        assert!(json.is_ok());

        let json_str = json.unwrap();
        assert!(json_str.contains("\"error\":\"Test error\""));
        assert!(json_str.contains("\"partition\":0"));
        assert!(json_str.contains("\"offset\":100"));
    }

    // Integration test would require a running Kafka instance
    #[ignore]
    #[tokio::test]
    async fn test_send_to_dlq() {
        let config = create_test_config();
        let producer = DlqProducer::new(config, "events-dlq".to_string()).unwrap();
        let message = create_test_dlq_message();

        let result = producer.send(message).await;
        assert!(result.is_ok());
    }
}
