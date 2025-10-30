import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


_logger_initialized = False


def setup_logging(
    log_file: str = './logs/market_data.log',
    level: str = 'INFO',
    max_bytes: int = 10485760,
    backup_count: int = 5,
    logger_name: str = 'market_data_collector'
) -> logging.Logger:
    """
    Initialize centralized logging with rotation.
    
    Args:
        log_file: Path to log file
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        max_bytes: Max size of each log file (default: 10MB)
        backup_count: Number of backup files to keep (default: 5)
        logger_name: Name of the logger
    
    Returns:
        logging.Logger: Configured logger instance
    """
    global _logger_initialized
    
    logger = logging.getLogger(logger_name)
    
    if _logger_initialized:
        return logger
    
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
    
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setLevel(logger.level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logger.level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    _logger_initialized = True
    logger.info(f"Logging initialized: {log_file} (level={level}, max_bytes={max_bytes}, backup_count={backup_count})")
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get logger instance.
    
    Args:
        name: Optional logger name. If None, returns root market_data_collector logger
    
    Returns:
        logging.Logger: Logger instance
    """
    if name:
        return logging.getLogger(f'market_data_collector.{name}')
    return logging.getLogger('market_data_collector')
