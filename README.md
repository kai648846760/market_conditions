# Market Data Collector

A minimal, scalable market data collection system with WebSocket support for cryptocurrency exchanges.

## Directory Structure

```
market_data_collector/
├── __init__.py           # Main module initialization, MarketDataReader placeholder
├── config.py             # YAML configuration loader with env overrides
└── utils/
    ├── __init__.py
    └── logging.py        # Centralized logging with rotation (10MB x 5)

configs/
└── market.yaml           # Default configuration

logs/                     # Log files (auto-created)
data/                     # Database storage (auto-created)
example.py                # Demo script
```

## Quick Start

### Run the Example

```bash
python example.py
```

Or with uv:

```bash
uv run python example.py
```

This will:
- Load configuration from `configs/market.yaml`
- Initialize logging with rotation
- Print key configuration values
- Demonstrate the MarketDataReader placeholder class

## Configuration

Configuration is loaded from `configs/market.yaml`. The following environment variables can override settings:

- `MDC_EXCHANGE` - Exchange name (e.g., bybit)
- `MDC_SYMBOLS` - Comma-separated list of symbols (e.g., "BTC/USDT:USDT,ETH/USDT:USDT")
- `MDC_LOG_LEVEL` - Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `MDC_DB_ROOT` - Database root directory
- `MDC_LOG_FILE` - Log file path

### Configuration Structure

See `configs/market.yaml` for the complete default configuration including:
- Exchange and symbol settings
- Data collection intervals (ticker, orderbook, trades, OHLCV, funding rate, mark price)
- Storage options (database, WAL, batch size, retention)
- Logging configuration (level, file, rotation)
- Runtime options (auto-start, single instance)

## Logging

Logs are written to `./logs/market_data.log` with:
- Rotating file handler (10MB max size)
- 5 backup files retained
- Console output for real-time monitoring
- Timestamps and log levels

## Development Status

This is a scaffold implementation. The `MarketDataReader` class is a placeholder with context manager support, preparing for future WebSocket collector implementation.
