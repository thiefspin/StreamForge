//! Kafka event consumer with manual offset management

use super::{DlqProducer, KafkaConfig, MessageProcessor, ProcessingResult};
use crate::db::EventRepository;
use crate::error::Result;
use anyhow::anyhow;
use futures::stream::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Event consumer that processes messages from Kafka
pub struct EventConsumer {
    /// Kafka consumer instance
    consumer: Arc<StreamConsumer>,

    /// Message processor
    processor: MessageProcessor,

    /// DLQ producer for failed messages
    dlq_producer: DlqProducer,

    /// Configuration
    config: KafkaConfig,

    /// Shutdown signal
    shutdown: Arc<AtomicBool>,

    /// Concurrency limiter
    semaphore: Arc<Semaphore>,

    /// Track if we're in a rebalance
    rebalancing: Arc<AtomicBool>,
}

impl EventConsumer {
    /// Create a new event consumer
    pub fn new(config: KafkaConfig, db_repo: Arc<dyn EventRepository>) -> Result<Self> {
        // Create consumer
        let consumer: StreamConsumer = config
            .build_consumer_config()
            .create()
            .map_err(|e| anyhow!("Failed to create Kafka consumer: {}", e))?;

        // Subscribe to topics
        consumer
            .subscribe(&[&config.events_topic])
            .map_err(|e| anyhow!("Failed to subscribe to topic: {}", e))?;

        // Create DLQ producer
        let dlq_producer =
            DlqProducer::new(config.build_producer_config(), config.dlq_topic.clone())?;

        // Create message processor
        let processor = MessageProcessor::new(db_repo, config.max_retries, config.retry_backoff());

        // Create concurrency semaphore
        let semaphore = Arc::new(Semaphore::new(config.batch_size));

        Ok(Self {
            consumer: Arc::new(consumer),
            processor,
            dlq_producer,
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
            semaphore,
            rebalancing: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Start consuming messages
    pub async fn start(self) -> Result<()> {
        info!(
            "Starting Kafka consumer for topic '{}'",
            self.config.events_topic
        );

        // Spawn a task to handle the stream consumption
        let consumer = Arc::clone(&self.consumer);
        let processor = self.processor.clone();
        let dlq_producer = self.dlq_producer.clone();
        let config = self.config.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let rebalancing = Arc::clone(&self.rebalancing);
        let semaphore = Arc::clone(&self.semaphore);

        // Run the consumer in a dedicated task
        let handle = tokio::spawn(async move {
            Self::consume_loop(
                consumer,
                processor,
                dlq_producer,
                config,
                shutdown,
                rebalancing,
                semaphore,
            )
            .await
        });

        // Wait for the consumer task to complete
        handle.await.unwrap_or_else(|e| {
            error!("Consumer task panicked: {}", e);
            Err(crate::error::Error::internal("Consumer task panicked"))
        })
    }

    /// Internal consumption loop
    async fn consume_loop(
        consumer: Arc<StreamConsumer>,
        processor: MessageProcessor,
        dlq_producer: DlqProducer,
        config: KafkaConfig,
        shutdown: Arc<AtomicBool>,
        rebalancing: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
    ) -> Result<()> {
        // Track messages for batch commit
        let mut processed_offsets = TopicPartitionList::new();
        let mut messages_since_commit = 0;
        let commit_interval = Duration::from_secs(5);
        let mut last_commit = tokio::time::Instant::now();

        // Create stream from consumer
        let stream = consumer.stream();
        tokio::pin!(stream);

        loop {
            // Check shutdown signal
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Check if we're in a rebalance
            if rebalancing.load(Ordering::Relaxed) {
                warn!("Consumer rebalancing in progress, waiting...");
                sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Get next message with timeout
            let message_result = tokio::select! {
                msg = stream.next() => msg,
                _ = sleep(Duration::from_secs(1)) => {
                    // Periodic commit check
                    if messages_since_commit > 0 && last_commit.elapsed() >= commit_interval {
                        consumer
                            .commit(&processed_offsets, rdkafka::consumer::CommitMode::Sync)
                            .map_err(|e| anyhow!("Failed to commit offsets: {}", e))?;
                        processed_offsets = TopicPartitionList::new();
                        messages_since_commit = 0;
                        last_commit = tokio::time::Instant::now();
                    }
                    continue;
                }
            };

            let message = match message_result {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    error!("Kafka consumer error: {}", e);
                    continue;
                },
                None => {
                    // No message available - continue polling
                    continue;
                },
            };

            // Acquire permit for concurrent processing
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| anyhow!("Failed to acquire semaphore: {}", e))?;

            // Process the message
            let processing_result =
                Self::process_single_message(&message, &processor, &dlq_producer).await;

            // Handle processing result
            match processing_result {
                Ok((partition, offset)) => {
                    // Track offset for commit
                    processed_offsets
                        .add_partition_offset(
                            &config.events_topic,
                            partition,
                            Offset::Offset(offset + 1),
                        )
                        .map_err(|e| anyhow!("Failed to track offset: {}", e))?;

                    messages_since_commit += 1;
                },
                Err(e) => {
                    error!("Message processing failed: {}", e);
                },
            }

            // Release permit
            drop(permit);

            // Commit offsets if batch size reached
            if messages_since_commit >= config.batch_size {
                consumer
                    .commit(&processed_offsets, rdkafka::consumer::CommitMode::Sync)
                    .map_err(|e| anyhow!("Failed to commit offsets: {}", e))?;
                processed_offsets = TopicPartitionList::new();
                messages_since_commit = 0;
                last_commit = tokio::time::Instant::now();
            }
        }

        // Final commit before shutdown
        if messages_since_commit > 0 {
            info!(
                "Committing {} pending offsets before shutdown",
                messages_since_commit
            );
            consumer
                .commit(&processed_offsets, rdkafka::consumer::CommitMode::Sync)
                .map_err(|e| anyhow!("Failed to commit offsets: {}", e))?;
        }

        info!("Kafka consumer stopped");
        Ok(())
    }

    /// Process a single message
    async fn process_single_message(
        message: &BorrowedMessage<'_>,
        processor: &MessageProcessor,
        dlq_producer: &DlqProducer,
    ) -> Result<(i32, i64)> {
        let partition = message.partition();
        let offset = message.offset();

        let result = processor.process_message(message).await;

        // Handle processing result
        match result {
            ProcessingResult::Success => {
                debug!(
                    "Successfully processed message from partition {} offset {}",
                    partition, offset
                );
                Ok((partition, offset))
            },
            ProcessingResult::RetryableError(error) => {
                warn!(
                    "Retryable error for partition {} offset {}: {}",
                    partition, offset, error
                );
                // Could implement retry logic here
                Err(crate::error::Error::internal(error))
            },
            ProcessingResult::PermanentError(error) => {
                error!(
                    "Permanent error for partition {} offset {}: {}",
                    partition, offset, error
                );

                // Send to DLQ
                let dlq_message = processor.create_dlq_message(
                    message,
                    error.clone(),
                    "PermanentError".to_string(),
                    0,
                );

                if let Err(e) = dlq_producer.send(dlq_message).await {
                    error!("Failed to send message to DLQ: {}", e);
                }

                // Even permanent errors should commit offset to avoid reprocessing
                Ok((partition, offset))
            },
        }
    }

    /// Gracefully shutdown the consumer
    pub async fn shutdown(&self) -> Result<()> {
        info!("Initiating consumer shutdown");
        self.shutdown.store(true, Ordering::Relaxed);

        // Wait for active processing to complete
        let timeout = Duration::from_secs(30);
        let start = tokio::time::Instant::now();

        while self.semaphore.available_permits() < self.config.batch_size {
            if start.elapsed() > timeout {
                warn!("Timeout waiting for active processing to complete");
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // Flush DLQ producer
        self.dlq_producer.flush()?;

        info!("Consumer shutdown complete");
        Ok(())
    }

    /// Handle consumer group rebalance
    pub fn mark_rebalancing(&self, is_rebalancing: bool) {
        self.rebalancing.store(is_rebalancing, Ordering::Relaxed);
        if is_rebalancing {
            warn!("Consumer group rebalance started");
        } else {
            info!("Consumer group rebalance completed");
        }
    }
}

impl Drop for EventConsumer {
    fn drop(&mut self) {
        // Ensure shutdown on drop
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MockEventRepository;

    #[tokio::test]
    async fn test_consumer_creation() {
        let config = KafkaConfig::default();
        let mock_repo = Arc::new(MockEventRepository::new());

        let result = EventConsumer::new(config, mock_repo);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_consumer_shutdown() {
        let config = KafkaConfig::default();
        let mock_repo = Arc::new(MockEventRepository::new());

        let consumer = EventConsumer::new(config, mock_repo).unwrap();

        // Trigger shutdown
        let shutdown_result = consumer.shutdown().await;
        assert!(shutdown_result.is_ok());

        // Check shutdown flag
        assert!(consumer.shutdown.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_rebalancing_flag() {
        let config = KafkaConfig::default();
        let mock_repo = Arc::new(MockEventRepository::new());

        let consumer = EventConsumer::new(config, mock_repo).unwrap();

        // Initially not rebalancing
        assert!(!consumer.rebalancing.load(Ordering::Relaxed));

        // Mark as rebalancing
        consumer.mark_rebalancing(true);
        assert!(consumer.rebalancing.load(Ordering::Relaxed));

        // Mark as not rebalancing
        consumer.mark_rebalancing(false);
        assert!(!consumer.rebalancing.load(Ordering::Relaxed));
    }
}
