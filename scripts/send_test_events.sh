#!/bin/bash

# Script to send test events to Kafka for StreamForge testing
# Usage: ./send_test_events.sh [number_of_events] [topic_name]

set -e

# Configuration
KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
TOPIC="${2:-events}"
NUM_EVENTS="${1:-10}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}StreamForge Test Event Generator${NC}"
echo "================================"
echo "Kafka Broker: $KAFKA_BROKER"
echo "Topic: $TOPIC"
echo "Number of events: $NUM_EVENTS"
echo ""

# Check if kafka-console-producer is available
if ! command -v kafka-console-producer &> /dev/null; then
    echo -e "${YELLOW}kafka-console-producer not found. Trying docker...${NC}"
    USE_DOCKER=true
else
    USE_DOCKER=false
fi

# Generate and send events
echo -e "${GREEN}Sending test events...${NC}"

for i in $(seq 1 $NUM_EVENTS); do
    # Generate random event data
    EVENT_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
    USER_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')

    # Randomly select event type
    TYPES=("CLICK" "VIEW" "PURCHASE" "SIGNUP")
    EVENT_TYPE=${TYPES[$RANDOM % ${#TYPES[@]}]}

    # Generate amount for purchase events
    if [ "$EVENT_TYPE" = "PURCHASE" ]; then
        AMOUNT_CENTS=$((RANDOM % 100000 + 100))
        AMOUNT_JSON=",\"amount_cents\":$AMOUNT_CENTS"
    else
        AMOUNT_JSON=""
    fi

    # Generate paths and referrers
    PATHS=("/home" "/products" "/checkout" "/about" "/contact")
    PATH=${PATHS[$RANDOM % ${#PATHS[@]}]}

    REFERRERS=("google.com" "facebook.com" "twitter.com" "direct" "reddit.com")
    REFERRER=${REFERRERS[$RANDOM % ${#REFERRERS[@]}]}

    # Create JSON event
    EVENT_JSON=$(cat <<EOF
{
  "event_id": "$EVENT_ID",
  "event_type": "$EVENT_TYPE",
  "occurred_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "user_id": "$USER_ID"$AMOUNT_JSON,
  "path": "$PATH",
  "referrer": "$REFERRER"
}
EOF
)

    # Send event to Kafka
    if [ "$USE_DOCKER" = true ]; then
        echo "$EVENT_JSON" | docker run --rm -i --network streamforge_streamforge-test \
            confluentinc/cp-kafka:7.5.0 \
            kafka-console-producer --broker-list kafka:29092 --topic "$TOPIC"
    else
        echo "$EVENT_JSON" | kafka-console-producer --broker-list "$KAFKA_BROKER" --topic "$TOPIC"
    fi

    echo -e "${GREEN}✓${NC} Sent event $i: $EVENT_TYPE (id: ${EVENT_ID:0:8}...)"

    # Small delay between events
    sleep 0.1
done

echo ""
echo -e "${GREEN}Successfully sent $NUM_EVENTS events to topic '$TOPIC'${NC}"

# Send some invalid events for DLQ testing
if [ "$3" = "--with-invalid" ]; then
    echo ""
    echo -e "${YELLOW}Sending invalid events for DLQ testing...${NC}"

    # Invalid UUID
    INVALID_EVENT1='{"event_id":"not-a-uuid","event_type":"CLICK","occurred_at":"2024-01-15T10:30:00Z","user_id":"550e8400-e29b-41d4-a716-446655440000"}'

    # Invalid event type
    INVALID_EVENT2='{"event_id":"550e8400-e29b-41d4-a716-446655440000","event_type":"INVALID_TYPE","occurred_at":"2024-01-15T10:30:00Z","user_id":"550e8400-e29b-41d4-a716-446655440000"}'

    # Missing required fields
    INVALID_EVENT3='{"event_id":"550e8400-e29b-41d4-a716-446655440000"}'

    # Invalid JSON
    INVALID_EVENT4='{"event_id": broken json'

    for event in "$INVALID_EVENT1" "$INVALID_EVENT2" "$INVALID_EVENT3" "$INVALID_EVENT4"; do
        if [ "$USE_DOCKER" = true ]; then
            echo "$event" | docker run --rm -i --network streamforge_streamforge-test \
                confluentinc/cp-kafka:7.5.0 \
                kafka-console-producer --broker-list kafka:29092 --topic "$TOPIC"
        else
            echo "$event" | kafka-console-producer --broker-list "$KAFKA_BROKER" --topic "$TOPIC"
        fi
        echo -e "${YELLOW}✓${NC} Sent invalid event"
    done

    echo -e "${YELLOW}Sent 4 invalid events for DLQ testing${NC}"
fi

echo ""
echo -e "${GREEN}Test event generation complete!${NC}"
echo ""
echo "To view events in Kafka, run:"
echo "  kafka-console-consumer --bootstrap-server $KAFKA_BROKER --topic $TOPIC --from-beginning"
echo ""
echo "To check the DLQ, run:"
echo "  kafka-console-consumer --bootstrap-server $KAFKA_BROKER --topic ${TOPIC}-dlq --from-beginning"
