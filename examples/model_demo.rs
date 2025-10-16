//! Demonstration of StreamForge data models and validation
//!
//! Run with: cargo run --example model_demo

use chrono::Utc;
use serde_json::json;
use streamforge::{EventType, NormalizedEvent, RawEvent};
use uuid::Uuid;

fn main() {
    println!("=== StreamForge Data Models Demo ===\n");

    // Demo 1: Valid Click Event
    demo_valid_click_event();
    println!();

    // Demo 2: Valid Purchase Event
    demo_valid_purchase_event();
    println!();

    // Demo 3: Invalid Events
    demo_invalid_events();
    println!();

    // Demo 4: Data Transformation
    demo_data_transformation();
    println!();

    // Demo 5: JSON Round-Trip
    demo_json_round_trip();
}

fn demo_valid_click_event() {
    println!("📍 Demo 1: Valid Click Event");
    println!("----------------------------");

    let json_data = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "CLICK",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "path": "/products/laptop",
        "referrer": "https://google.com"
    });

    println!("Input JSON:");
    println!("{}", serde_json::to_string_pretty(&json_data).unwrap());

    let raw_event: RawEvent = serde_json::from_value(json_data).unwrap();
    let normalized = NormalizedEvent::try_from(raw_event).unwrap();

    println!("\nNormalized Event:");
    println!("  Event ID: {}", normalized.event_id);
    println!("  Type: {}", normalized.event_type);
    println!("  User ID: {}", normalized.user_id);
    println!("  Path: {:?}", normalized.path);
    println!("  Referrer: {:?}", normalized.referrer);
    println!("  Processed At: {}", normalized.processed_at);
}

fn demo_valid_purchase_event() {
    println!("💰 Demo 2: Valid Purchase Event");
    println!("-------------------------------");

    let json_data = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "PURCHASE",
        "occurred_at": "2024-01-15T10:30:00Z",
        "user_id": Uuid::new_v4().to_string(),
        "amount_cents": 2499,
        "path": "/checkout/complete"
    });

    let raw_event: RawEvent = serde_json::from_value(json_data).unwrap();
    let normalized = NormalizedEvent::try_from(raw_event).unwrap();

    println!("Purchase Event Details:");
    println!("  Event ID: {}", normalized.event_id);
    println!("  Type: {}", normalized.event_type);
    println!("  Amount: ${:.2}", normalized.amount_dollars().unwrap());
    println!("  Is Purchase: {}", normalized.is_purchase());
    println!("  Path: {:?}", normalized.path);
}

fn demo_invalid_events() {
    println!("❌ Demo 3: Invalid Events");
    println!("-------------------------");

    // Invalid UUID
    let invalid_uuid = json!({
        "event_id": "not-a-uuid",
        "event_type": "CLICK",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string()
    });

    let raw: RawEvent = serde_json::from_value(invalid_uuid).unwrap();
    match NormalizedEvent::try_from(raw) {
        Err(e) => println!("Invalid UUID Error: {}", e),
        Ok(_) => println!("Unexpected success!"),
    }

    // Invalid Event Type
    let invalid_type = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "INVALID_TYPE",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string()
    });

    let raw: RawEvent = serde_json::from_value(invalid_type).unwrap();
    match NormalizedEvent::try_from(raw) {
        Err(e) => println!("Invalid Type Error: {}", e),
        Ok(_) => println!("Unexpected success!"),
    }

    // Purchase without amount
    let purchase_no_amount = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "PURCHASE",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string()
    });

    let raw: RawEvent = serde_json::from_value(purchase_no_amount).unwrap();
    match NormalizedEvent::try_from(raw) {
        Err(e) => println!("Purchase No Amount Error: {}", e),
        Ok(_) => println!("Unexpected success!"),
    }

    // Negative amount
    let negative_amount = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "PURCHASE",
        "occurred_at": Utc::now().to_rfc3339(),
        "user_id": Uuid::new_v4().to_string(),
        "amount_cents": -100
    });

    let raw: RawEvent = serde_json::from_value(negative_amount).unwrap();
    match NormalizedEvent::try_from(raw) {
        Err(e) => println!("Negative Amount Error: {}", e),
        Ok(_) => println!("Unexpected success!"),
    }
}

fn demo_data_transformation() {
    println!("🔄 Demo 4: Data Transformation");
    println!("------------------------------");

    // Path normalization
    let paths = vec!["home", "/about", "  /contact  ", ""];
    println!("Path Normalization:");
    for path in paths {
        let json_data = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": "VIEW",
            "occurred_at": Utc::now().to_rfc3339(),
            "user_id": Uuid::new_v4().to_string(),
            "path": path
        });

        let raw: RawEvent = serde_json::from_value(json_data).unwrap();
        let normalized = NormalizedEvent::try_from(raw).unwrap();
        println!("  '{}' -> {:?}", path, normalized.path);
    }

    // Referrer cleaning
    let referrers = vec![
        Some("https://example.com"),
        Some("null"),
        Some("undefined"),
        Some("   "),
        None,
    ];
    println!("\nReferrer Cleaning:");
    for referrer in referrers {
        let mut json_data = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": "CLICK",
            "occurred_at": Utc::now().to_rfc3339(),
            "user_id": Uuid::new_v4().to_string()
        });

        if let Some(r) = referrer {
            json_data["referrer"] = json!(r);
        }

        let raw: RawEvent = serde_json::from_value(json_data).unwrap();
        let normalized = NormalizedEvent::try_from(raw).unwrap();
        println!("  {:?} -> {:?}", referrer, normalized.referrer);
    }

    // Case-insensitive event types
    let event_types = vec!["click", "CLICK", "Click", "VIEW", "view", "PURCHASE"];
    println!("\nCase-Insensitive Event Types:");
    for event_type in event_types {
        let mut json_data = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": event_type,
            "occurred_at": Utc::now().to_rfc3339(),
            "user_id": Uuid::new_v4().to_string()
        });

        if event_type.to_uppercase() == "PURCHASE" {
            json_data["amount_cents"] = json!(1000);
        }

        let raw: RawEvent = serde_json::from_value(json_data).unwrap();
        let normalized = NormalizedEvent::try_from(raw).unwrap();
        println!("  '{}' -> {}", event_type, normalized.event_type);
    }
}

fn demo_json_round_trip() {
    println!("🔁 Demo 5: JSON Round-Trip");
    println!("--------------------------");

    // Create a normalized event
    let json_data = json!({
        "event_id": Uuid::new_v4().to_string(),
        "event_type": "VIEW",
        "occurred_at": "2024-01-15T14:30:00+00:00",
        "user_id": Uuid::new_v4().to_string(),
        "path": "/blog/rust-tutorial",
        "referrer": "https://reddit.com/r/rust"
    });

    let raw: RawEvent = serde_json::from_value(json_data.clone()).unwrap();
    let mut normalized = NormalizedEvent::try_from(raw).unwrap();

    // Add Kafka metadata
    normalized = normalized.with_kafka_metadata(3, 12345);

    // Serialize to JSON
    let serialized = serde_json::to_value(&normalized).unwrap();

    println!("Original JSON:");
    println!("{}", serde_json::to_string_pretty(&json_data).unwrap());

    println!("\nNormalized JSON:");
    println!("{}", serde_json::to_string_pretty(&serialized).unwrap());

    // Verify all fields are present
    println!("\nField Verification:");
    println!("  ✅ event_id: {}", serialized["event_id"].is_string());
    println!(
        "  ✅ event_type: {} ({})",
        serialized["event_type"].is_string(),
        serialized["event_type"]
    );
    println!(
        "  ✅ occurred_at: {}",
        serialized["occurred_at"].is_string()
    );
    println!("  ✅ user_id: {}", serialized["user_id"].is_string());
    println!("  ✅ path: {}", serialized["path"].is_string());
    println!("  ✅ referrer: {}", serialized["referrer"].is_string());
    println!(
        "  ✅ kafka_partition: {} ({})",
        serialized["kafka_partition"].is_i64(),
        serialized["kafka_partition"]
    );
    println!(
        "  ✅ kafka_offset: {} ({})",
        serialized["kafka_offset"].is_i64(),
        serialized["kafka_offset"]
    );
    println!(
        "  ✅ processed_at: {}",
        serialized["processed_at"].is_string()
    );
}
