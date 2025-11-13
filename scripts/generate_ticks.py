#!/usr/bin/env python3
"""
Generate Realistic Stock Tick Data to Kafka.

This script generates realistic stock market tick data (price, volume, timestamp)
and publishes them to the 'ticks' Kafka topic for testing the streaming lakehouse
enrichment pipeline. Tick data can be joined with stocks reference data from
datasets/stocks_seed.csv for stream enrichment demonstrations.

Usage:
    python scripts/generate_ticks.py --count 1000 --rate 10
    python scripts/generate_ticks.py --symbols AAPL,GOOGL,MSFT --duration 60

Arguments:
    --count: Number of ticks to generate (default: 100)
    --rate: Ticks per second (default: 10)
    --duration: Generate ticks for N seconds (overrides --count)
    --symbols: Comma-separated list of stock symbols (default: all from seed)
    --topic: Kafka topic name (default: ticks)
    --volatility: Price volatility percentage 0-100 (default: 2.0)
    --seed: Random seed for reproducibility (optional)

Examples:
    # Generate 1000 ticks at 10 ticks/sec
    python scripts/generate_ticks.py --count 1000 --rate 10

    # Generate ticks for 60 seconds at high rate
    python scripts/generate_ticks.py --duration 60 --rate 50

    # Generate only FAANG stocks with high volatility
    python scripts/generate_ticks.py --symbols AAPL,GOOGL,META,AMZN,NFLX --volatility 5.0

    # Reproducible ticks for testing
    python scripts/generate_ticks.py --count 100 --seed 42
"""

import argparse
import csv
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import Field
from pydantic_settings import BaseSettings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================


class TickGeneratorConfig(BaseSettings):
    """Configuration for tick data generator."""

    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_topic_ticks: str = Field(
        default="ticks",
        alias="KAFKA_TOPIC_TICKS",
    )

    class Config:
        env_file = ".env"
        case_sensitive = False


@dataclass
class StockInfo:
    """Stock reference information from seed data."""

    symbol: str
    company_name: str
    sector: str
    industry: str
    base_price: float


# ============================================================
# Stock Reference Data Loading
# ============================================================


def load_stock_reference_data(seed_file: Path) -> dict[str, StockInfo]:
    """
    Load stock reference data from CSV seed file.

    Args:
        seed_file: Path to stocks_seed.csv

    Returns:
        Dictionary mapping symbol to StockInfo

    Raises:
        FileNotFoundError: If seed file doesn't exist
        ValueError: If CSV is malformed
    """
    stocks = {}

    # Base prices for realistic tick generation (approximate market prices)
    base_prices = {
        "AAPL": 175.50,
        "GOOGL": 140.25,
        "MSFT": 380.75,
        "TSLA": 245.30,
        "AMZN": 155.80,
        "JPM": 155.20,
        "JNJ": 160.45,
        "XOM": 110.35,
        "WMT": 165.90,
        "V": 260.15,
        "PG": 155.30,
        "NVDA": 495.75,
        "MA": 425.60,
        "HD": 355.20,
        "DIS": 95.85,
        "BAC": 35.75,
        "INTC": 45.90,
        "CSCO": 52.30,
        "NFLX": 485.25,
        "PFE": 31.45,
        "META": 350.20,
        "ADBE": 560.40,
        "CRM": 270.15,
        "ORCL": 115.80,
        "AMD": 145.60,
    }

    if not seed_file.exists():
        logger.warning("Stock seed file not found: %s, using defaults", seed_file)
        # Return default stocks if seed file doesn't exist
        return {
            symbol: StockInfo(
                symbol=symbol,
                company_name=f"{symbol} Inc.",
                sector="Technology",
                industry="Software",
                base_price=price,
            )
            for symbol, price in base_prices.items()
        }

    with seed_file.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row["symbol"]
            stocks[symbol] = StockInfo(
                symbol=symbol,
                company_name=row["company_name"],
                sector=row["sector"],
                industry=row["industry"],
                base_price=base_prices.get(symbol, 100.0),  # Default price if not in map
            )

    logger.info("Loaded %d stock symbols from seed data", len(stocks))
    return stocks


# ============================================================
# Tick Generation
# ============================================================


class TickGenerator:
    """Generate realistic stock market tick data."""

    def __init__(
        self,
        stocks: dict[str, StockInfo],
        volatility: float = 2.0,
        seed: int | None = None,
    ) -> None:
        """
        Initialize tick generator.

        Args:
            stocks: Dictionary of stock information
            volatility: Price volatility percentage (0-100)
            seed: Random seed for reproducibility
        """
        self.stocks = stocks
        self.volatility = volatility / 100.0  # Convert to decimal
        self.symbols = list(stocks.keys())

        # Price memory for realistic tick sequences
        self.current_prices = {
            symbol: info.base_price for symbol, info in stocks.items()
        }

        if seed is not None:
            random.seed(seed)
            logger.info("Using random seed: %d", seed)

    def generate_tick(self, symbol: str | None = None) -> dict[str, Any]:
        """
        Generate a single stock tick with realistic price movement.

        Args:
            symbol: Stock symbol (random if not specified)

        Returns:
            Dictionary with fields: symbol, price, volume, timestamp_ms

        Example:
            >>> gen = TickGenerator(stocks)
            >>> tick = gen.generate_tick("AAPL")
            >>> print(tick)
            {'symbol': 'AAPL', 'price': 175.82, 'volume': 1523, 'timestamp_ms': 1699564832000}
        """
        # Select symbol
        if symbol is None:
            symbol = random.choice(self.symbols)

        stock_info = self.stocks[symbol]
        current_price = self.current_prices[symbol]

        # Generate price with random walk (mean-reverting to base price)
        price_change_pct = random.uniform(-self.volatility, self.volatility)
        mean_reversion = (stock_info.base_price - current_price) * 0.05

        new_price = current_price * (1 + price_change_pct) + mean_reversion
        new_price = max(new_price, stock_info.base_price * 0.5)  # Floor at 50% of base
        new_price = round(new_price, 2)

        # Update price memory
        self.current_prices[symbol] = new_price

        # Generate realistic volume (log-normal distribution)
        base_volume = 1000
        volume = int(random.lognormvariate(0, 1.5) * base_volume)
        volume = max(100, min(volume, 50000))  # Clamp between 100 and 50k

        # Current timestamp
        timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

        return {
            "symbol": symbol,
            "price": new_price,
            "volume": volume,
            "timestamp_ms": timestamp_ms,
        }


# ============================================================
# Kafka Producer
# ============================================================


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create and configure Kafka producer for tick data.

    Args:
        bootstrap_servers: Kafka broker addresses

    Returns:
        Configured KafkaProducer instance
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        compression_type="snappy",
    )


def send_tick(
    producer: KafkaProducer,
    topic: str,
    tick: dict[str, Any],
) -> None:
    """
    Send tick to Kafka topic.

    Args:
        producer: Kafka producer instance
        topic: Target Kafka topic
        tick: Tick data to send

    Raises:
        KafkaError: If sending fails after retries
    """
    try:
        # Use symbol as key for partitioning
        future = producer.send(
            topic,
            key=tick["symbol"],
            value=tick,
        )

        # Non-blocking send
        future.get(timeout=5)

    except KafkaError:
        logger.exception("Failed to send tick: %s", tick["symbol"])
        raise


# ============================================================
# Main Generation Loop
# ============================================================


def generate_and_send_ticks(  # noqa: PLR0913
    generator: TickGenerator,
    producer: KafkaProducer,
    topic: str,
    count: int | None = None,
    duration: int | None = None,
    rate: int = 10,
    symbols: list[str] | None = None,
) -> None:
    """
    Generate and send stock ticks to Kafka.

    Args:
        generator: TickGenerator instance
        producer: Kafka producer
        topic: Kafka topic name
        count: Number of ticks to generate (mutually exclusive with duration)
        duration: Generate ticks for N seconds (mutually exclusive with count)
        rate: Target ticks per second
        symbols: List of symbols to generate (None = all)
    """
    if count is None and duration is None:
        count = 100

    if count is not None and duration is not None:
        msg = "Cannot specify both count and duration"
        raise ValueError(msg)

    mode = f"{count} ticks" if count else f"{duration} seconds"
    logger.info("Starting tick generation: %s at %d ticks/sec", mode, rate)
    logger.info("Kafka: %s, Topic: %s", producer.config["bootstrap_servers"], topic)

    sleep_time = 1.0 / rate if rate > 0 else 0
    start_time = time.time()
    ticks_sent = 0

    try:
        while True:
            # Check termination condition
            if count and ticks_sent >= count:
                break
            if duration and (time.time() - start_time) >= duration:
                break

            # Generate tick
            symbol = random.choice(symbols) if symbols else None
            tick = generator.generate_tick(symbol)

            # Send to Kafka
            send_tick(producer, topic, tick)
            ticks_sent += 1

            # Progress logging
            if ticks_sent % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = ticks_sent / elapsed if elapsed > 0 else 0
                logger.info(
                    "Sent %d ticks (%.1f ticks/sec)",
                    ticks_sent,
                    actual_rate,
                )

            # Rate limiting
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Flush remaining messages
        producer.flush()

        elapsed = time.time() - start_time
        actual_rate = ticks_sent / elapsed if elapsed > 0 else 0
        logger.info(
            "Successfully sent %d ticks in %.1f seconds (%.1f ticks/sec)",
            ticks_sent,
            elapsed,
            actual_rate,
        )

    except KeyboardInterrupt:
        logger.warning("Generation interrupted by user")

    except Exception:
        logger.exception("Error during tick generation")
        raise


# ============================================================
# CLI Interface
# ============================================================


def main() -> None:
    """Main entry point for the tick generator."""
    parser = argparse.ArgumentParser(
        description="Generate realistic stock tick data to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Generation parameters
    gen_group = parser.add_mutually_exclusive_group()
    gen_group.add_argument(
        "--count",
        type=int,
        help="Number of ticks to generate (default: 100)",
    )
    gen_group.add_argument(
        "--duration",
        type=int,
        help="Generate ticks for N seconds (overrides --count)",
    )

    parser.add_argument(
        "--rate",
        type=int,
        default=10,
        help="Ticks per second (default: 10)",
    )

    # Symbol selection
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of stock symbols (default: all from seed)",
    )

    # Kafka configuration
    parser.add_argument(
        "--topic",
        type=str,
        help="Kafka topic name (default: ticks)",
    )

    # Tick parameters
    parser.add_argument(
        "--volatility",
        type=float,
        default=2.0,
        help="Price volatility percentage 0-100 (default: 2.0)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducibility",
    )

    args = parser.parse_args()

    # Load configuration
    config = TickGeneratorConfig()
    topic = args.topic or config.kafka_topic_ticks

    # Load stock reference data
    seed_file = Path(__file__).parent.parent / "datasets" / "stocks_seed.csv"
    stocks = load_stock_reference_data(seed_file)

    # Filter symbols if specified
    if args.symbols:
        symbol_list = [s.strip().upper() for s in args.symbols.split(",")]
        stocks = {sym: stocks[sym] for sym in symbol_list if sym in stocks}
        if not stocks:
            logger.error("No valid symbols found: %s", args.symbols)
            return
        logger.info("Using %d symbols: %s", len(stocks), ", ".join(stocks.keys()))

    # Create tick generator
    generator = TickGenerator(stocks, volatility=args.volatility, seed=args.seed)

    # Create Kafka producer
    producer = create_producer(config.kafka_bootstrap_servers)

    try:
        # Generate and send ticks
        symbol_filter = list(stocks.keys()) if args.symbols else None
        generate_and_send_ticks(
            generator=generator,
            producer=producer,
            topic=topic,
            count=args.count,
            duration=args.duration,
            rate=args.rate,
            symbols=symbol_filter,
        )

    finally:
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    main()
