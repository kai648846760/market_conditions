"""
日志管理模块
提供统一的日志记录功能，支持日志轮转和备份
"""

import os
import logging
import logging.handlers
from pathlib import Path
from typing import Optional


class Logger:
    """
    日志管理器类
    负责初始化和管理应用程序的日志记录
    """
    
    _instance = None
    _logger = None
    
    def __new__(cls):
        """单例模式，确保只有一个日志管理器实例"""
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初始化日志管理器"""
        if Logger._logger is None:
            self._setup_logger()
    
    def _setup_logger(self, config: Optional[dict] = None):
        """
        设置日志记录器
        
        Args:
            config: 日志配置字典，包含日志级别、文件路径等设置
        """
        # 默认配置
        default_config = {
            'level': 'INFO',
            'log_file': './logs/market_conditions.log',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'rotation': {
                'max_size': 10,  # MB
                'backup_count': 5,
                'when': 'midnight'
            }
        }
        
        # 合并用户配置
        if config:
            default_config.update(config)
            if 'rotation' in config and isinstance(config['rotation'], dict):
                default_config['rotation'].update(config['rotation'])
        
        # 创建日志目录
        log_file = Path(default_config['log_file'])
        log_dir = log_file.parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建备份目录
        backup_dir = log_dir / 'backups'
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置日志级别
        level = getattr(logging, default_config['level'].upper(), logging.INFO)
        
        # 创建日志记录器
        Logger._logger = logging.getLogger('market_conditions')
        Logger._logger.setLevel(level)
        
        # 避免重复添加处理器
        if Logger._logger.handlers:
            return
        
        # 创建格式化器
        formatter = logging.Formatter(default_config['format'])
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        Logger._logger.addHandler(console_handler)
        
        # 文件处理器（按大小轮转）
        rotation_config = default_config['rotation']
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=rotation_config['max_size'] * 1024 * 1024,  # MB to bytes
            backupCount=rotation_config['backup_count'],
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        Logger._logger.addHandler(file_handler)
    
    @classmethod
    def get_logger(cls, name: str = None) -> logging.Logger:
        """
        获取日志记录器实例
        
        Args:
            name: 日志记录器名称，默认使用'market_conditions'
            
        Returns:
            logging.Logger: 日志记录器实例
        """
        if cls._logger is None:
            cls()
        
        if name:
            return logging.getLogger(f'market_conditions.{name}')
        return cls._logger
    
    @classmethod
    def debug(cls, message: str, *args, **kwargs):
        """记录调试信息"""
        cls.get_logger().debug(message, *args, **kwargs)
    
    @classmethod
    def info(cls, message: str, *args, **kwargs):
        """记录一般信息"""
        cls.get_logger().info(message, *args, **kwargs)
    
    @classmethod
    def warning(cls, message: str, *args, **kwargs):
        """记录警告信息"""
        cls.get_logger().warning(message, *args, **kwargs)
    
    @classmethod
    def error(cls, message: str, *args, **kwargs):
        """记录错误信息"""
        cls.get_logger().error(message, *args, **kwargs)
    
    @classmethod
    def critical(cls, message: str, *args, **kwargs):
        """记录严重错误信息"""
        cls.get_logger().critical(message, *args, **kwargs)


def init_logger(config: dict):
    """
    初始化日志系统
    
    Args:
        config: 日志配置字典
    """
    logger = Logger()
    logger._setup_logger(config)