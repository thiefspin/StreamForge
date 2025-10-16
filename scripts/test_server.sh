#!/bin/bash

# StreamForge Server Test Script
# This script tests that the server can start with minimal configuration

set -e

echo "=== StreamForge Server Test Script ==="
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Change to project root
cd "$(dirname "$0")/.."

# Check if we have required environment variables
if [ ! -f ".env" ] && [ ! -f ".env.test" ]; then
    echo -e "${YELLOW}Warning: No .env file found. Creating minimal test configuration...${NC}"
    cat > .env.test << EOF
# Minimal test configuration
POSTGRES_URL=postgresql://streamforge:streamforge@localhost:5432/streamforge_test
EOF
fi

# Use test environment if no .env exists
if [ ! -f ".env" ] && [ -f ".env.test" ]; then
    echo -e "${YELLOW}Using .env.test configuration${NC}"
    cp .env.test .env
    CLEANUP_ENV=true
fi

# Build the project
echo "Building StreamForge..."
cargo build --release 2>&1 | grep -E "(Compiling|Finished)" || true
echo

# Start the server in the background
echo "Starting StreamForge server..."
RUST_LOG=info,streamforge=debug timeout 5 cargo run --release 2>&1 | tee server.log &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
sleep 2

# Test health endpoint
echo "Testing health endpoint..."
if curl -f -s http://localhost:8080/healthz > /dev/null; then
    echo -e "${GREEN}✓ Health check passed${NC}"
    HEALTH_RESPONSE=$(curl -s http://localhost:8080/healthz)
    echo "  Response: $HEALTH_RESPONSE"
else
    echo -e "${RED}✗ Health check failed${NC}"
    EXIT_CODE=1
fi

# Test ready endpoint
echo
echo "Testing ready endpoint..."
if curl -f -s http://localhost:8080/readyz > /dev/null; then
    echo -e "${GREEN}✓ Ready check passed${NC}"
    READY_RESPONSE=$(curl -s http://localhost:8080/readyz)
    echo "  Response: $READY_RESPONSE"
else
    echo -e "${RED}✗ Ready check failed${NC}"
    EXIT_CODE=1
fi

# Test build info endpoint
echo
echo "Testing build info endpoint..."
if curl -f -s http://localhost:8080/build > /dev/null; then
    echo -e "${GREEN}✓ Build info check passed${NC}"
    BUILD_RESPONSE=$(curl -s http://localhost:8080/build)
    echo "  Response: $BUILD_RESPONSE"
else
    echo -e "${RED}✗ Build info check failed${NC}"
    EXIT_CODE=1
fi

# Test metrics endpoint (if enabled)
echo
echo "Testing metrics endpoint..."
if curl -f -s http://localhost:8080/metrics > /dev/null; then
    echo -e "${GREEN}✓ Metrics endpoint available${NC}"
else
    echo -e "${YELLOW}! Metrics endpoint not available (might be disabled)${NC}"
fi

# Clean up
echo
echo "Cleaning up..."
if [ ! -z "$SERVER_PID" ]; then
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
fi

if [ "$CLEANUP_ENV" = "true" ]; then
    rm -f .env
fi

# Final status
echo
if [ "$EXIT_CODE" = "1" ]; then
    echo -e "${RED}=== Server test FAILED ===${NC}"
    echo "Check server.log for details"
    exit 1
else
    echo -e "${GREEN}=== Server test PASSED ===${NC}"
    echo "All endpoints responded correctly!"
fi
