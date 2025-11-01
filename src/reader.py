"""
数据读取器模块
提供从数据库读取市场数据的功能
"""

import sqlite3
import os
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import json
import ccxt

from .logger import Logger
from .database import DatabaseManager
from .utils import (
    load_config, format_symbol, is_futures_symbol,
    get_symbol_parts, get_current_timestamp_ms, timestamp_to_datetime
)


class DataReader:
    """
    数据读取器类
    提供从数据库读取市场数据的功能
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化数据读取器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = load_config(config_path)
        self.db_manager = DatabaseManager(self.config)
        
        Logger.info("数据读取器初始化完成")
    
    def get_ohlcv(self, exchange: str, symbol: str, timeframe: str = '1m', 
                 limit: int = 100, since: Optional[int] = None) -> List[List]:
        """
        获取K线数据(OHLCV)
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            timeframe: 时间框架，默认1m
            limit: 获取数量，默认100
            since: 开始时间戳（毫秒）
            
        Returns:
            List[List]: K线数据列表，每个元素为[timestamp, open, high, low, close, volume]
        """
        try:
            # 先从数据库获取数据
            data_list = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol,
                data_type='ohlcv',
                limit=limit,
                timeframe=timeframe,
                start_time=since
            )
            
            # 解析数据
            ohlcv_data = []
            for item in data_list:
                if isinstance(item, dict) and 'data' in item:
                    # 如果数据是嵌套的，提取实际数据
                    item = item['data']
                
                if isinstance(item, list) and len(item) >= 6:
                    ohlcv_data.append(item)
            
            # 如果数据库中的数据不足，从交易所获取
            if len(ohlcv_data) < limit:
                # 初始化交易所
                exchange_class = getattr(ccxt, exchange)
                
                # 判断是否为期货符号，决定交易所类型
                default_type = 'future' if is_futures_symbol(symbol) else 'spot'
                
                exchange_instance = exchange_class({
                    'options': {'defaultType': default_type},
                    'enableRateLimit': True,
                })
                
                # 获取K线数据
                ohlcv = exchange_instance.fetch_ohlcv(symbol, timeframe, since, limit)
                
                # 存储到数据库
                for candle in ohlcv:
                    # 将列表数据转换为字典格式，符合数据库插入要求
                    data = {
                        'timestamp': candle[0],  # 添加原始时间戳
                        'open': candle[1],
                        'high': candle[2],
                        'low': candle[3],
                        'close': candle[4],
                        'volume': candle[5]
                    }
                    
                    self.db_manager.insert_data(
                        exchange=exchange,
                        symbol=symbol,
                        data_type='ohlcv',
                        data=data,
                        timeframe=timeframe
                    )
                
                ohlcv_data = ohlcv
            
            return ohlcv_data
            
        except Exception as e:
            Logger.error(f"获取K线数据失败: {str(e)}")
            return []
    
    def get_ticker(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取ticker数据
        
        Args:
            exchange: 交易所名称
            symbol: 交易对
            
        Returns:
            Optional[Dict[str, Any]]: ticker数据，如果没有则返回None
        """
        try:
            # 使用DatabaseManager的get_data方法获取ticker数据
            data_list = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol,
                data_type='ticker',
                limit=1
            )
            
            # 检查数据是否存在且是最新（5分钟内）
            if data_list:
                ticker_data = data_list[0]
                if isinstance(ticker_data, dict) and 'timestamp' in ticker_data:
                    # 检查数据是否是5分钟内的
                    current_time = get_current_timestamp_ms()
                    data_time = ticker_data['timestamp']
                    if current_time - data_time < 5 * 60 * 1000:  # 5分钟
                        Logger.info(f"使用数据库中的ticker数据: {exchange} {symbol}")
                        return ticker_data
            
            # 数据不存在或过期，使用ccxt获取最新数据（降级方案）
            Logger.info(f"从交易所获取ticker数据: {exchange} {symbol}")
            exchange_class = getattr(ccxt, exchange)
            
            # 判断是否为期货符号，决定交易所类型
            default_type = 'future' if is_futures_symbol(symbol) else 'spot'
            
            exchange_instance = exchange_class({
                'options': {'defaultType': default_type},
                'enableRateLimit': True,
            })
            
            ticker = exchange_instance.fetch_ticker(symbol)
            
            # 添加时间戳
            ticker['timestamp'] = get_current_timestamp_ms()
            
            # 存储到数据库
            self.db_manager.insert_data(
                exchange=exchange,
                symbol=symbol,
                data_type='ticker',
                data=ticker
            )
            
            return ticker
            
        except Exception as e:
            Logger.error(f"获取ticker数据失败: {str(e)}")
            return None
    
    def get_order_book(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取orderbook数据
        
        Args:
            exchange: 交易所名称
            symbol: 交易对
            
        Returns:
            Optional[Dict[str, Any]]: orderbook数据，如果没有则返回None
        """
        try:
            # 使用DatabaseManager的get_data方法获取orderbook数据
            data_list = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol,
                data_type='orderbook',
                limit=1
            )
            
            # 检查数据是否存在且是最新（1分钟内）
            if data_list:
                orderbook_data = data_list[0]
                if isinstance(orderbook_data, dict) and 'timestamp' in orderbook_data:
                    # 检查数据是否是1分钟内的
                    current_time = get_current_timestamp_ms()
                    data_time = orderbook_data['timestamp']
                    if current_time - data_time < 60 * 1000:  # 1分钟
                        Logger.info(f"使用数据库中的orderbook数据: {exchange} {symbol}")
                        return orderbook_data
            
            # 数据不存在或过期，使用ccxt获取最新数据（降级方案）
            Logger.info(f"从交易所获取orderbook数据: {exchange} {symbol}")
            exchange_class = getattr(ccxt, exchange)
            
            # 判断是否为期货符号，决定交易所类型
            default_type = 'future' if is_futures_symbol(symbol) else 'spot'
            
            exchange_instance = exchange_class({
                'options': {'defaultType': default_type},
                'enableRateLimit': True,
            })
            
            orderbook = exchange_instance.fetch_order_book(symbol)
            
            # 添加时间戳
            orderbook['timestamp'] = get_current_timestamp_ms()
            
            # 存储到数据库
            self.db_manager.insert_data(
                exchange=exchange,
                symbol=symbol,
                data_type='orderbook',
                data=orderbook
            )
            
            return orderbook
            
        except Exception as e:
            Logger.error(f"获取orderbook数据失败: {str(e)}")
            return None
    
    def get_trades(self, exchange: str, symbol: str, limit: int = 10) -> List[Dict]:
        """
        获取trades数据
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            limit: 获取数据条数限制
            
        Returns:
            List[Dict]: trades数据列表
        """
        try:
            # 使用DatabaseManager的get_data方法获取trades数据
            data_list = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol,
                data_type='trades',  # 注意这里是复数形式
                limit=limit
            )
            
            # 检查数据是否存在且是最新（1分钟内）
            if data_list and len(data_list) > 0:
                # 获取最新一笔交易的时间
                latest_trade = data_list[0]
                if isinstance(latest_trade, dict) and 'timestamp' in latest_trade:
                    # 检查数据是否是1分钟内的
                    current_time = get_current_timestamp_ms()
                    data_time = latest_trade['timestamp']
                    if current_time - data_time < 60 * 1000:  # 1分钟
                        Logger.info(f"使用数据库中的trades数据: {exchange} {symbol}")
                        return data_list
            
            # 数据不存在或过期，使用ccxt获取最新数据（降级方案）
            Logger.info(f"从交易所获取trades数据: {exchange} {symbol}")
            exchange_class = getattr(ccxt, exchange)
            
            # 判断是否为期货符号，决定交易所类型
            default_type = 'future' if is_futures_symbol(symbol) else 'spot'
            
            exchange_instance = exchange_class({
                'options': {'defaultType': default_type},
                'enableRateLimit': True,
            })
            
            trades = exchange_instance.fetch_trades(symbol, limit=limit)
            
            # 存储到数据库
            for trade in trades:
                self.db_manager.insert_data(
                    exchange=exchange,
                    symbol=symbol,
                    data_type='trades',
                    data=trade
                )
            
            return trades
            
        except Exception as e:
            Logger.error(f"获取trades数据失败: {str(e)}")
            return []
    
    # 私有API方法
    def get_balance(self, user_id: str) -> List[Dict]:
        """
        获取用户余额
        
        Args:
            user_id: 用户ID
            
        Returns:
            List[Dict]: 余额数据列表
        """
        try:
            # 使用DatabaseManager的get_data方法获取balance数据
            data_list = self.db_manager.get_data(
                exchange='binance',  # 默认交易所
                symbol='',  # 余额数据不需要symbol
                data_type='balance',
                limit=100,
                user_id=user_id
            )
            
            return data_list
        except Exception as e:
            Logger.error(f"获取余额数据失败: {str(e)}")
            return []
    
    def get_orders(self, user_id: str, symbol: str = None, limit: int = 10) -> List[Dict]:
        """
        获取用户订单
        
        Args:
            user_id: 用户ID
            symbol: 交易对符号，可选
            limit: 获取数据条数限制
            
        Returns:
            List[Dict]: 订单数据列表
        """
        try:
            # 使用DatabaseManager的get_data方法获取orders数据
            data_list = self.db_manager.get_data(
                exchange='binance',  # 默认交易所
                symbol=symbol or '',
                data_type='orders',
                limit=limit,
                user_id=user_id
            )
            
            return data_list
        except Exception as e:
            Logger.error(f"获取订单数据失败: {str(e)}")
            return []
    
    def get_my_trades(self, user_id: str, symbol: str = None, limit: int = 10) -> List[Dict]:
        """
        获取用户交易记录
        
        Args:
            user_id: 用户ID
            symbol: 交易对符号，可选
            limit: 获取数据条数限制
            
        Returns:
            List[Dict]: 交易记录数据列表
        """
        try:
            # 使用DatabaseManager的get_data方法获取mytrades数据
            data_list = self.db_manager.get_data(
                exchange='binance',  # 默认交易所
                symbol=symbol or '',
                data_type='mytrades',
                limit=limit,
                user_id=user_id
            )
            
            return data_list
        except Exception as e:
            Logger.error(f"获取交易记录数据失败: {str(e)}")
            return []
    
    def get_positions(self, user_id: str) -> List[Dict]:
        """
        获取用户持仓
        
        Args:
            user_id: 用户ID
            
        Returns:
            List[Dict]: 持仓数据列表
        """
        try:
            # 使用DatabaseManager的get_data方法获取positions数据
            data_list = self.db_manager.get_data(
                exchange='binance',  # 默认交易所
                symbol='',  # 持仓数据不需要symbol
                data_type='positions',
                limit=100,
                user_id=user_id
            )
            
            return data_list
        except Exception as e:
            Logger.error(f"获取持仓数据失败: {str(e)}")
            return []