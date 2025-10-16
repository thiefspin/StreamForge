//! Kafka configuration module

use envconfig::Envconfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Kafka configuration settings
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct KafkaConfig {
    /// Kafka broker addresses (comma-separated)
    #[serde(default = "default_brokers")]
    #[envconfig(from = "KAFKA_BROKERS", default = "localhost:9092")]
    pub brokers: String,

    /// Consumer group ID
    #[serde(default = "default_consumer_group")]
    #[envconfig(from = "KAFKA_CONSUMER_GROUP", default = "streamforge-consumer")]
    pub consumer_group: String,

    /// Topic to consume events from
    #[serde(default = "default_events_topic")]
    #[envconfig(from = "KAFKA_EVENTS_TOPIC", default = "events")]
    pub events_topic: String,

    /// Dead Letter Queue topic
    #[serde(default = "default_dlq_topic")]
    #[envconfig(from = "KAFKA_DLQ_TOPIC", default = "events-dlq")]
    pub dlq_topic: String,

    /// Enable auto-commit (should be false for manual offset management)
    #[serde(default = "default_auto_commit")]
    #[envconfig(from = "KAFKA_AUTO_COMMIT", default = "false")]
    pub auto_commit: bool,

    /// Session timeout in milliseconds
    #[serde(default = "default_session_timeout")]
    #[envconfig(from = "KAFKA_SESSION_TIMEOUT_MS", default = "30000")]
    pub session_timeout_ms: u32,

    /// Maximum poll interval in milliseconds
    #[serde(default = "default_max_poll_interval")]
    #[envconfig(from = "KAFKA_MAX_POLL_INTERVAL_MS", default = "300000")]
    pub max_poll_interval_ms: u32,

    /// Batch size for processing
    #[serde(default = "default_batch_size")]
    #[envconfig(from = "KAFKA_BATCH_SIZE", default = "100")]
    pub batch_size: usize,

    /// Maximum retries for transient errors
    #[serde(default = "default_max_retries")]
    #[envconfig(from = "KAFKA_MAX_RETRIES", default = "3")]
    pub max_retries: u32,

    /// Retry backoff duration in milliseconds
    #[serde(default = "default_retry_backoff_ms")]
    #[envconfig(from = "KAFKA_RETRY_BACKOFF_MS", default = "1000")]
    pub retry_backoff_ms: u64,

    /// Enable idempotent producer for DLQ
    #[serde(default = "default_idempotent_producer")]
    #[envconfig(from = "KAFKA_IDEMPOTENT_PRODUCER", default = "true")]
    pub idempotent_producer: bool,

    /// Compression type for DLQ producer
    #[serde(default = "default_compression_type")]
    #[envconfig(from = "KAFKA_COMPRESSION_TYPE", default = "snappy")]
    pub compression_type: String,

    /// Fetch min bytes
    #[serde(default = "default_fetch_min_bytes")]
    #[envconfig(from = "KAFKA_FETCH_MIN_BYTES", default = "1024")]
    pub fetch_min_bytes: i32,

    /// Fetch max wait ms
    #[serde(default = "default_fetch_max_wait_ms")]
    #[envconfig(from = "KAFKA_FETCH_MAX_WAIT_MS", default = "500")]
    pub fetch_max_wait_ms: i32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: default_brokers(),
            consumer_group: default_consumer_group(),
            events_topic: default_events_topic(),
            dlq_topic: default_dlq_topic(),
            auto_commit: default_auto_commit(),
            session_timeout_ms: default_session_timeout(),
            max_poll_interval_ms: default_max_poll_interval(),
            batch_size: default_batch_size(),
            max_retries: default_max_retries(),
            retry_backoff_ms: default_retry_backoff_ms(),
            idempotent_producer: default_idempotent_producer(),
            compression_type: default_compression_type(),
            fetch_min_bytes: default_fetch_min_bytes(),
            fetch_max_wait_ms: default_fetch_max_wait_ms(),
        }
    }
}

impl KafkaConfig {
    /// Create a new KafkaConfig from environment variables
    pub fn from_env() -> Result<Self, envconfig::Error> {
        <Self as envconfig::Envconfig>::init_from_env()
    }

    /// Get session timeout as Duration
    pub fn session_timeout(&self) -> Duration {
        Duration::from_millis(self.session_timeout_ms as u64)
    }

    /// Get max poll interval as Duration
    pub fn max_poll_interval(&self) -> Duration {
        Duration::from_millis(self.max_poll_interval_ms as u64)
    }

    /// Get retry backoff as Duration
    pub fn retry_backoff(&self) -> Duration {
        Duration::from_millis(self.retry_backoff_ms)
    }

    /// Build rdkafka consumer configuration
    pub fn build_consumer_config(&self) -> rdkafka::ClientConfig {
        let mut config = rdkafka::ClientConfig::new();

        config
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.consumer_group)
            .set("enable.auto.commit", self.auto_commit.to_string())
            .set("session.timeout.ms", self.session_timeout_ms.to_string())
            .set(
                "max.poll.interval.ms",
                self.max_poll_interval_ms.to_string(),
            )
            .set("fetch.min.bytes", self.fetch_min_bytes.to_string())
            .set("fetch.wait.max.ms", self.fetch_max_wait_ms.to_string())
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed");

        config
    }

    /// Build rdkafka producer configuration for DLQ
    pub fn build_producer_config(&self) -> rdkafka::ClientConfig {
        let mut config = rdkafka::ClientConfig::new();

        config
            .set("bootstrap.servers", &self.brokers)
            .set("message.timeout.ms", "30000")
            .set("compression.type", &self.compression_type);

        if self.idempotent_producer {
            config
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set("retries", "10")
                .set("max.in.flight.requests.per.connection", "5");
        } else {
            config.set("acks", "1");
        }

        config
    }
}

// Default value functions
fn default_brokers() -> String {
    "localhost:9092".to_string()
}

fn default_consumer_group() -> String {
    "streamforge-consumer".to_string()
}

fn default_events_topic() -> String {
    "events".to_string()
}

fn default_dlq_topic() -> String {
    "events-dlq".to_string()
}

fn default_auto_commit() -> bool {
    false
}

fn default_session_timeout() -> u32 {
    30000 // 30 seconds
}

fn default_max_poll_interval() -> u32 {
    300000 // 5 minutes
}

fn default_batch_size() -> usize {
    100
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_backoff_ms() -> u64 {
    1000
}

fn default_idempotent_producer() -> bool {
    true
}

fn default_compression_type() -> String {
    "snappy".to_string()
}

fn default_fetch_min_bytes() -> i32 {
    1024
}

fn default_fetch_max_wait_ms() -> i32 {
    500
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KafkaConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.consumer_group, "streamforge-consumer");
        assert_eq!(config.events_topic, "events");
        assert_eq!(config.dlq_topic, "events-dlq");
        assert!(!config.auto_commit);
    }

    #[test]
    fn test_duration_conversions() {
        let config = KafkaConfig::default();
        assert_eq!(config.session_timeout(), Duration::from_secs(30));
        assert_eq!(config.max_poll_interval(), Duration::from_secs(300));
        assert_eq!(config.retry_backoff(), Duration::from_secs(1));
    }

    #[test]
    fn test_consumer_config_build() {
        let config = KafkaConfig::default();
        let _consumer_config = config.build_consumer_config();

        // Just verify that the config can be built without errors
        // The actual configuration values are tested through the defaults
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.consumer_group, "streamforge-consumer");
    }
}
