"""
私有数据读取器模块
负责从数据库中读取私有数据（余额、订单、持仓等）
"""

import os
import sqlite3
import json
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path

from .database import DatabaseManager
from .logger import Logger
from .utils import timestamp_to_datetime, get_current_timestamp_ms


class PrivateDataReader:
    """
    私有数据读取器类
    提供读取私有数据（余额、订单、持仓等）的方法
    """
    
    def __init__(self, config_path: str = None):
        """
        初始化私有数据读取器
        
        Args:
            config_path: 配置文件路径
        """
        # 加载配置
        if config_path is None:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
        
        from .utils import load_config
        self.config = load_config(config_path)
        
        # 初始化数据库管理器
        self.db_manager = DatabaseManager(self.config.get('database', {}))
        
        # 初始化日志记录器
        self.logger = Logger.get_logger('private_reader')
    
    def get_balance(self, exchange: str, user_id: str) -> Optional[Dict]:
        """
        获取账户余额
        
        Args:
            exchange: 交易所名称
            user_id: 用户ID
            
        Returns:
            Optional[Dict]: 余额数据，如果没有找到则返回None
        """
        try:
            data = self.db_manager.get_data(
                exchange=exchange,
                symbol='',  # 余额数据不使用symbol
                data_type='balance',
                limit=1,
                user_id=user_id
            )
            
            if data:
                return data[0]
            return None
            
        except Exception as e:
            self.logger.error(f"获取账户余额失败: {str(e)}")
            return None
    
    def get_orders(self, exchange: str, user_id: str, limit: int = 10, symbol: str = None) -> List[Dict]:
        """
        获取订单
        
        Args:
            exchange: 交易所名称
            user_id: 用户ID
            limit: 获取数据条数限制
            symbol: 交易对符号（可选）
            
        Returns:
            List[Dict]: 订单数据列表
        """
        try:
            data = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol or '',
                data_type='orders',
                limit=limit,
                user_id=user_id
            )
            
            return data
            
        except Exception as e:
            self.logger.error(f"获取订单失败: {str(e)}")
            return []
    
    def get_my_trades(self, exchange: str, user_id: str, limit: int = 10, symbol: str = None) -> List[Dict]:
        """
        获取我的成交
        
        Args:
            exchange: 交易所名称
            user_id: 用户ID
            limit: 获取数据条数限制
            symbol: 交易对符号（可选）
            
        Returns:
            List[Dict]: 成交数据列表
        """
        try:
            data = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol or '',
                data_type='mytrades',
                limit=limit,
                user_id=user_id
            )
            
            return data
            
        except Exception as e:
            self.logger.error(f"获取我的成交失败: {str(e)}")
            return []
    
    def get_positions(self, exchange: str, user_id: str, symbol: str = None) -> List[Dict]:
        """
        获取持仓
        
        Args:
            exchange: 交易所名称
            user_id: 用户ID
            symbol: 交易对符号（可选）
            
        Returns:
            List[Dict]: 持仓数据列表
        """
        try:
            data = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol or '',
                data_type='positions',
                limit=10,  # 获取最近10条持仓记录
                user_id=user_id
            )
            
            return data
            
        except Exception as e:
            self.logger.error(f"获取持仓失败: {str(e)}")
            return []