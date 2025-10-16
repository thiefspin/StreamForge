//! Integration tests for StreamForge data models
//!
//! These tests verify the end-to-end behavior of event validation,
//! transformation, and serialization.

use chrono::Utc;
use serde_json::json;
use streamforge::{EventType, NormalizedEvent, RawEvent};
use uuid::Uuid;

/// Helper to create a valid raw event JSON
fn valid_event_json() -> serde_json::Value {
    json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "CLICK",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "amount_cents": null,
        "path": "/home",
        "referrer": "https://google.com"
    })
}

#[test]
fn test_raw_event_deserialization_valid() {
    let json = valid_event_json();
    let raw: RawEvent = serde_json::from_value(json).expect("Should deserialize valid event");

    assert_eq!(raw.event_type, "CLICK");
    assert_eq!(raw.path, Some("/home".to_string()));
    assert_eq!(raw.referrer, Some("https://google.com".to_string()));
    assert!(raw.amount_cents.is_none());
}

#[test]
fn test_raw_event_deserialization_missing_optional_fields() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "VIEW",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string()
    });

    let raw: RawEvent =
        serde_json::from_value(json).expect("Should deserialize with missing optional fields");

    assert_eq!(raw.event_type, "VIEW");
    assert!(raw.path.is_none());
    assert!(raw.referrer.is_none());
    assert!(raw.amount_cents.is_none());
}

#[test]
fn test_raw_event_deserialization_invalid_uuid() {
    let json = json!({
        "event_id": "not-a-uuid",
        "event_type": "CLICK",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string()
    });

    let raw: RawEvent =
        serde_json::from_value(json).expect("Should deserialize even with invalid UUID");

    // Validation should fail when we try to convert to normalized
    let result = NormalizedEvent::try_from(raw);
    assert!(result.is_err());
}

#[test]
fn test_purchase_event_requires_amount() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "PURCHASE",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "amount_cents": null
    });

    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let result = NormalizedEvent::try_from(raw);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Purchase events must have an amount"));
}

#[test]
fn test_purchase_event_with_amount() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "PURCHASE",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "amount_cents": 2500,
        "path": "/checkout"
    });

    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let normalized = NormalizedEvent::try_from(raw).expect("Should convert valid purchase event");

    assert_eq!(normalized.event_type, EventType::Purchase);
    assert_eq!(normalized.amount_cents, Some(2500));
    assert!(normalized.is_purchase());
    assert_eq!(normalized.amount_dollars(), Some(25.0));
}

#[test]
fn test_event_type_case_insensitive() {
    let test_cases = vec![
        ("click", EventType::Click),
        ("CLICK", EventType::Click),
        ("Click", EventType::Click),
        ("view", EventType::View),
        ("VIEW", EventType::View),
        ("purchase", EventType::Purchase),
        ("PURCHASE", EventType::Purchase),
    ];

    for (input, expected) in test_cases {
        let mut json = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": input,
            "occurred_at": Utc::now().to_rfc3339(),
            "user_id": Uuid::new_v4().to_string()
        });

        // Add amount for purchase events
        if input.to_uppercase() == "PURCHASE" {
            json["amount_cents"] = json!(1000);
        }

        let raw: RawEvent = serde_json::from_value(json).unwrap();
        let normalized = NormalizedEvent::try_from(raw).unwrap();
        assert_eq!(normalized.event_type, expected);
    }
}

#[test]
fn test_path_normalization_integration() {
    let test_cases = vec![
        ("home", "/home"),
        ("/about", "/about"),
        ("  /contact  ", "/contact"),
        ("", "/"),
    ];

    for (input, expected) in test_cases {
        let json = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": "CLICK",
            "occurred_at": Utc::now().to_rfc3339(),
            "user_id": Uuid::new_v4().to_string(),
            "path": input
        });

        let raw: RawEvent = serde_json::from_value(json).unwrap();
        let normalized = NormalizedEvent::try_from(raw).unwrap();
        assert_eq!(normalized.path, Some(expected.to_string()));
    }
}

#[test]
fn test_referrer_cleaning_integration() {
    let test_cases = vec![
        (Some("https://google.com"), Some("https://google.com")),
        (Some("null"), None),
        (Some("undefined"), None),
        (Some("   "), None),
        (None, None),
    ];

    for (input, expected) in test_cases {
        let mut json = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": "CLICK",
            "occurred_at": Utc::now().to_rfc3339(),
            "user_id": Uuid::new_v4().to_string()
        });

        if let Some(referrer) = input {
            json["referrer"] = json!(referrer);
        }

        let raw: RawEvent = serde_json::from_value(json).unwrap();
        let normalized = NormalizedEvent::try_from(raw).unwrap();
        assert_eq!(normalized.referrer, expected.map(|s| s.to_string()));
    }
}

#[test]
fn test_negative_amount_rejected() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "PURCHASE",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "amount_cents": -100
    });

    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let result = NormalizedEvent::try_from(raw);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("non-negative"));
}

#[test]
fn test_invalid_event_type_rejected() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "INVALID",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string()
    });

    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let result = NormalizedEvent::try_from(raw);

    assert!(result.is_err());
}

#[test]
fn test_invalid_timestamp_rejected() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "CLICK",
        "occurred_at": "2023-01-01 12:00:00",  // Not RFC3339
        "user_id": Uuid::new_v4().to_string()
    });

    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let result = NormalizedEvent::try_from(raw);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("RFC3339"));
}

#[test]
fn test_normalized_event_serialization() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "VIEW",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "path": "/products"
    });

    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let normalized = NormalizedEvent::try_from(raw).unwrap();

    // Serialize normalized event
    let serialized = serde_json::to_value(&normalized).unwrap();

    // Check that all fields are present
    assert!(serialized["event_id"].is_string());
    assert_eq!(serialized["event_type"], "VIEW");
    assert!(serialized["occurred_at"].is_string());
    assert!(serialized["user_id"].is_string());
    assert_eq!(serialized["path"], "/products");
    assert!(serialized["kafka_partition"].is_i64());
    assert!(serialized["kafka_offset"].is_i64());
    assert!(serialized["processed_at"].is_string());
}

#[test]
fn test_kafka_metadata_setting() {
    let json = valid_event_json();
    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let normalized = NormalizedEvent::try_from(raw).unwrap().with_kafka_metadata(5, 123456);

    assert_eq!(normalized.kafka_partition, 5);
    assert_eq!(normalized.kafka_offset, 123456);
}

#[test]
fn test_all_event_types_round_trip() {
    let event_types = vec!["CLICK", "VIEW", "PURCHASE"];

    for event_type in event_types {
        let mut json = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": event_type,
            "occurred_at": Utc::now().to_rfc3339(),
            "user_id": Uuid::new_v4().to_string()
        });

        // PURCHASE needs an amount
        if event_type == "PURCHASE" {
            json["amount_cents"] = json!(1000);
        }

        // Deserialize from JSON
        let raw: RawEvent = serde_json::from_value(json.clone()).unwrap();

        // Convert to normalized
        let normalized = NormalizedEvent::try_from(raw.clone()).unwrap();

        // Serialize raw back to JSON
        let raw_json = serde_json::to_value(&raw).unwrap();
        assert_eq!(raw_json["event_type"], event_type);

        // Serialize normalized to JSON
        let normalized_json = serde_json::to_value(&normalized).unwrap();
        assert!(normalized_json["event_type"].is_string());
    }
}

#[test]
fn test_validation_with_validator_crate() {
    use validator::Validate;

    let valid = RawEvent {
        event_id: Uuid::new_v4().to_string(),
        event_type: "CLICK".to_string(),
        occurred_at: Utc::now().to_rfc3339(),
        user_id: Uuid::new_v4().to_string(),
        amount_cents: Some(100),
        path: Some("/home".to_string()),
        referrer: None,
    };

    assert!(valid.validate().is_ok());

    let invalid = RawEvent {
        event_id: "not-a-uuid".to_string(),
        event_type: "INVALID".to_string(),
        occurred_at: "not-a-timestamp".to_string(),
        user_id: "not-a-uuid".to_string(),
        amount_cents: Some(-100),
        path: None,
        referrer: None,
    };

    assert!(invalid.validate().is_err());
}

#[test]
fn test_large_amount_values() {
    let json = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "PURCHASE",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "amount_cents": i64::MAX
    });

    let raw: RawEvent = serde_json::from_value(json).unwrap();
    let normalized = NormalizedEvent::try_from(raw).unwrap();

    assert_eq!(normalized.amount_cents, Some(i64::MAX));
    assert!(normalized.amount_dollars().is_some());
}

#[test]
fn test_timezone_handling() {
    let timestamps = vec![
        "2023-01-01T12:00:00Z",
        "2023-01-01T12:00:00+00:00",
        "2023-01-01T07:00:00-05:00",
        "2023-01-01T20:00:00+08:00",
    ];

    for timestamp in timestamps {
        let json = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": "CLICK",
            "occurred_at": timestamp,
            "user_id": Uuid::new_v4().to_string()
        });

        let raw: RawEvent = serde_json::from_value(json).unwrap();
        let normalized = NormalizedEvent::try_from(raw).unwrap();

        // All timestamps should be converted to UTC
        assert_eq!(normalized.occurred_at.timezone(), Utc);
    }
}
