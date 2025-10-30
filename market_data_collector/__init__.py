from .config import get_config, Config
from .utils.logging import setup_logging, get_logger


_config = None
_logger = None


def _initialize():
    """Initialize configuration and logging on first import"""
    global _config, _logger
    
    if _config is None:
        _config = get_config()
    
    if _logger is None:
        logging_config = _config.logging
        _logger = setup_logging(
            log_file=logging_config.get('file', './logs/market_data.log'),
            level=logging_config.get('level', 'INFO'),
            max_bytes=logging_config.get('max_bytes', 10485760),
            backup_count=logging_config.get('backup_count', 5)
        )
        _logger.info("Market Data Collector initialized")


class MarketDataReader:
    """
    Placeholder class for reading market data.
    Context manager support for future resource management.
    """
    
    def __init__(self, config: Config = None):
        self.config = config or _config
        self.logger = get_logger('reader')
        self._active = False
    
    def __enter__(self):
        self.logger.info("MarketDataReader context entered")
        self._active = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("MarketDataReader context exited")
        self._active = False
        return False
    
    def start(self):
        """Placeholder: Start data collection"""
        self.logger.info("MarketDataReader.start() called (placeholder)")
    
    def stop(self):
        """Placeholder: Stop data collection"""
        self.logger.info("MarketDataReader.stop() called (placeholder)")


_initialize()

__all__ = ['MarketDataReader', 'get_config', 'Config', 'get_logger']
