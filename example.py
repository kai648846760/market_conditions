#!/usr/bin/env python3
"""
Example script to demonstrate market_data_collector initialization and configuration.
"""

from market_data_collector import MarketDataReader, get_config, get_logger


def main():
    logger = get_logger('example')
    logger.info("Starting example.py")
    
    config = get_config()
    
    print("\n=== Market Data Collector Configuration ===")
    print(f"Exchange: {config.exchange}")
    print(f"Type: {config.type}")
    print(f"Symbols: {', '.join(config.symbols)}")
    print(f"\nIntervals:")
    print(f"  Ticker: {config.intervals['ticker']}s")
    print(f"  Orderbook: {config.intervals['orderbook']}s")
    print(f"  Trades: {config.intervals['trades']}s")
    print(f"  OHLCV: {config.intervals['ohlcv']}")
    print(f"  Funding Rate: {config.intervals['funding_rate']}s")
    print(f"  Mark Price: {config.intervals['mark_price']}s")
    print(f"\nOrderbook Depth: {config.orderbook['depth']}")
    print(f"\nStorage:")
    print(f"  DB Root: {config.storage['db_root']}")
    print(f"  WAL: {config.storage['wal']}")
    print(f"  Batch Size: {config.storage['batch_size']}")
    print(f"  Keep Days: {config.storage['keep_days']}")
    print(f"  Fallback Queue Root: {config.storage['fallback_queue_root']}")
    print(f"\nLogging:")
    print(f"  Level: {config.logging['level']}")
    print(f"  File: {config.logging['file']}")
    print(f"  Max Bytes: {config.logging['max_bytes']}")
    print(f"  Backup Count: {config.logging['backup_count']}")
    print(f"\nRuntime:")
    print(f"  Auto Start: {config.runtime['auto_start']}")
    print(f"  Single Instance: {config.runtime['single_instance']}")
    print()
    
    print("=== Testing MarketDataReader ===")
    with MarketDataReader() as reader:
        print("MarketDataReader created successfully (placeholder)")
        reader.start()
    
    logger.info("Example completed successfully")
    print("\n✓ Example completed successfully!")


if __name__ == '__main__':
    main()
