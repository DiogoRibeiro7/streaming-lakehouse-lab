#!/usr/bin/env bash
# ============================================================
# Generate Sample Stock Tick Data to Kafka
# ============================================================
# This script generates realistic stock tick data and sends
# it to the 'ticks' Kafka topic for testing the enrichment pipeline.
#
# Usage:
#   bash scripts/generate_ticks.sh [count] [delay_ms]
#
# Arguments:
#   count:    Number of ticks to generate (default: 100)
#   delay_ms: Delay between ticks in milliseconds (default: 100)
#
# Examples:
#   bash scripts/generate_ticks.sh              # 100 ticks, 100ms delay
#   bash scripts/generate_ticks.sh 1000 50      # 1000 ticks, 50ms delay
#   bash scripts/generate_ticks.sh 10 1000      # 10 ticks, 1 second delay
# ============================================================

set -euo pipefail

# Configuration
COUNT=${1:-100}
DELAY_MS=${2:-100}
KAFKA_CONTAINER="kafka"
KAFKA_BROKER="localhost:9092"
TOPIC="ticks"

# Stock symbols (matching bootstrap data)
SYMBOLS=(
  "AAPL" "GOOGL" "MSFT" "TSLA" "AMZN"
  "JPM" "JNJ" "XOM" "WMT" "V"
  "PG" "NVDA" "MA" "HD" "DIS"
  "BAC" "INTC" "CSCO" "NFLX" "PFE"
)

# Base prices for realistic tick data
declare -A BASE_PRICES=(
  ["AAPL"]=175.50
  ["GOOGL"]=140.25
  ["MSFT"]=380.75
  ["TSLA"]=245.30
  ["AMZN"]=155.80
  ["JPM"]=155.20
  ["JNJ"]=160.45
  ["XOM"]=110.35
  ["WMT"]=165.90
  ["V"]=260.15
  ["PG"]=155.30
  ["NVDA"]=495.75
  ["MA"]=425.60
  ["HD"]=355.20
  ["DIS"]=95.85
  ["BAC"]=35.75
  ["INTC"]=45.90
  ["CSCO"]=52.30
  ["NFLX"]=485.25
  ["PFE"]=31.45
)

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Generating $COUNT stock ticks to Kafka topic '$TOPIC'${NC}"
echo -e "${BLUE}Delay: ${DELAY_MS}ms between ticks${NC}"
echo ""

# Check if Kafka is running
if ! docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; then
  echo -e "${YELLOW}Warning: Kafka may not be running or accessible${NC}"
  echo -e "${YELLOW}Run 'make up' to start the stack${NC}"
  exit 1
fi

# Function to generate a random tick
generate_tick() {
  local symbol=${SYMBOLS[$RANDOM % ${#SYMBOLS[@]}]}
  local base_price=${BASE_PRICES[$symbol]}

  # Add random variance (-2% to +2%)
  local variance=$(awk -v base="$base_price" 'BEGIN {
    srand();
    variance = (rand() * 4 - 2) / 100;
    printf "%.2f", base * (1 + variance)
  }')

  # Random volume between 100 and 10000
  local volume=$((RANDOM % 9900 + 100))

  # Current timestamp in milliseconds
  local timestamp_ms=$(date +%s%3N)

  # Generate JSON
  cat <<EOF
{"symbol":"$symbol","price":$variance,"volume":$volume,"timestamp_ms":$timestamp_ms}
EOF
}

# Counter for progress
generated=0

# Generate and send ticks
for ((i=1; i<=COUNT; i++)); do
  tick=$(generate_tick)

  # Send to Kafka
  echo "$tick" | docker exec -i "$KAFKA_CONTAINER" \
    kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    2>/dev/null

  generated=$((generated + 1))

  # Progress indicator every 10 ticks
  if ((i % 10 == 0)); then
    echo -e "${GREEN}Generated $generated/$COUNT ticks...${NC}"
  fi

  # Delay between ticks (convert ms to seconds)
  if [ "$DELAY_MS" -gt 0 ]; then
    sleep "$(awk -v ms="$DELAY_MS" 'BEGIN {printf "%.3f", ms/1000}')"
  fi
done

echo ""
echo -e "${GREEN}✓ Successfully generated $generated ticks to topic '$TOPIC'${NC}"
echo ""
echo -e "${BLUE}To consume the ticks:${NC}"
echo "  docker exec kafka kafka-console-consumer.sh \\"
echo "    --bootstrap-server localhost:9092 \\"
echo "    --topic ticks --from-beginning"
echo ""
echo -e "${BLUE}To consume enriched ticks:${NC}"
echo "  docker exec kafka kafka-console-consumer.sh \\"
echo "    --bootstrap-server localhost:9092 \\"
echo "    --topic ticks_enriched --from-beginning"
