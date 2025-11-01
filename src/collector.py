"""
数据收集器模块
基于ccxt pro实现实时市场数据收集
支持公共数据和私有数据的分离管理
"""

import asyncio
import time
import atexit
import signal
import sys
from typing import Dict, List, Any, Optional, Callable, Set
import threading
from datetime import datetime

try:
    import ccxt.pro as ccxtpro
except ImportError:
    import ccxt as ccxtpro

from .logger import Logger
from .database import DatabaseManager
from .utils import (
    load_config, get_enabled_exchanges, get_exchange_symbols,
    get_data_intervals, get_account_config, format_symbol,
    is_futures_symbol, get_symbol_parts, retry_on_failure,
    get_current_timestamp_ms, validate_symbol, get_ohlcv_timeframes
)


class PublicDataCollector:
    """
    公共数据收集器类
    负责收集公共市场数据（ticker、orderbook、trade等）
    不需要API密钥
    """
    
    def __init__(self, exchange_name: str, symbols: List[str], config: Dict):
        """
        初始化公共数据收集器
        
        Args:
            exchange_name: 交易所名称
            symbols: 交易对列表
            config: 配置信息
        """
        self.exchange_name = exchange_name
        self.symbols = symbols
        self.config = config
        self.exchange = None
        self.tasks = []
        self.callbacks = {}
        
        # 初始化数据库管理器
        self.db_manager = DatabaseManager(config)
        
        # 初始化交易所连接
        self._init_exchange()
        
        Logger.info(f"公共数据收集器 {exchange_name} 初始化完成，订阅币对: {len(symbols)} 个")
    
    def _init_exchange(self):
        """初始化交易所连接（公共数据不需要API密钥）"""
        try:
            # 获取交易所类
            exchange_class = getattr(ccxtpro, self.exchange_name)
            
            # 基础配置（不需要API密钥）
            exchange_config = {
                'sandbox': self.config.get('global', {}).get('testnet', False),
                'enableRateLimit': True,
            }
            
            # 创建交易所实例
            self.exchange = exchange_class(exchange_config)
            
            Logger.info(f"交易所 {self.exchange_name} 公共数据连接初始化成功")
            
        except Exception as e:
            Logger.error(f"初始化交易所 {self.exchange_name} 公共数据连接失败: {str(e)}")
            raise
    
    def register_callback(self, data_type: str, callback: Callable):
        """
        注册数据回调函数
        
        Args:
            data_type: 数据类型 (ticker, orderbook, trade, etc.)
            callback: 回调函数，接收 (exchange, symbol, data) 参数
        """
        if data_type not in self.callbacks:
            self.callbacks[data_type] = []
        self.callbacks[data_type].append(callback)
        Logger.debug(f"注册 {data_type} 数据回调函数")
    
    def _notify_callbacks(self, data_type: str, exchange: str, symbol: str, data: Any):
        """
        通知所有回调函数
        
        Args:
            data_type: 数据类型
            exchange: 交易所名称
            symbol: 交易对
            data: 数据
        """
        if data_type in self.callbacks:
            for callback in self.callbacks[data_type]:
                try:
                    callback(exchange, symbol, data)
                except Exception as e:
                    Logger.error(f"回调函数执行失败: {str(e)}")
    
    async def start(self):
        """启动公共数据收集
        
        使用WebSocket实时数据流，interval参数主要用于重试间隔，不需要轮询
        """
        Logger.info(f"启动 {self.exchange_name} 公共数据收集")
        
        # 分离现货和合约符号
        spot_symbols = [s for s in self.symbols if not is_futures_symbol(s)]
        futures_symbols = [s for s in self.symbols if is_futures_symbol(s)]
        
        # 创建现货数据收集任务
        if spot_symbols:
            task = asyncio.create_task(
                self._collect_spot_data(spot_symbols)
            )
            self.tasks.append(task)
        
        # 创建合约数据收集任务
        if futures_symbols:
            task = asyncio.create_task(
                self._collect_futures_data(futures_symbols)
            )
            self.tasks.append(task)
        
        Logger.info(f"启动了 {len(self.tasks)} 个 {self.exchange_name} 公共数据收集任务")
    
    async def stop(self):
        """停止公共数据收集"""
        Logger.info(f"停止 {self.exchange_name} 公共数据收集")
        
        # 取消所有任务
        for task in self.tasks:
            task.cancel()
        
        # 等待任务完成
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # 关闭交易所连接
        if self.exchange:
            try:
                await self.exchange.close()
                Logger.info(f"已关闭交易所 {self.exchange_name} 公共数据连接")
            except Exception as e:
                Logger.error(f"关闭交易所 {self.exchange_name} 公共数据连接失败: {str(e)}")
        
        self.tasks.clear()
        Logger.info(f"{self.exchange_name} 公共数据收集器已停止")
    
    async def _collect_spot_data(self, symbols: List[str]):
        """
        收集现货数据
        
        Args:
            symbols: 现货交易对列表
        """
        Logger.info(f"开始收集 {self.exchange_name} 现货数据: {symbols}")
        
        # 获取数据收集间隔配置
        intervals = get_data_intervals(self.config)
        
        try:
            # 设置现货类型
            if hasattr(self.exchange, 'options'):
                self.exchange.options['defaultType'] = 'spot'
            
            # 为每个符号创建数据监听任务
            for symbol in symbols:
                if not validate_symbol(symbol):
                    Logger.warning(f"无效的交易对符号: {symbol}")
                    continue
                
                # 启动ticker数据收集
                if 'ticker' in intervals:
                    task = asyncio.create_task(
                        self._watch_ticker(symbol, intervals['ticker'])
                    )
                    self.tasks.append(task)
                
                # 启动orderbook数据收集
                if 'orderbook' in intervals:
                    task = asyncio.create_task(
                        self._watch_orderbook(symbol, intervals['orderbook'])
                    )
                    self.tasks.append(task)
                
                # 启动trade数据收集
                if 'trades' in intervals:
                    task = asyncio.create_task(
                        self._watch_trades(symbol, intervals['trades'])
                    )
                    self.tasks.append(task)
                
                # 启动K线数据收集任务（从配置文件读取时间周期）
                timeframes = get_ohlcv_timeframes(self.config)
                for timeframe in timeframes:
                    task = asyncio.create_task(
                        self._watch_ohlcv(symbol, timeframe)
                    )
                    self.tasks.append(task)
                
                # 短暂延迟避免请求过于频繁
                await asyncio.sleep(0.1)
            
            # 保持任务运行
            while True:
                await asyncio.sleep(1)
                
        except Exception as e:
            Logger.error(f"收集 {self.exchange_name} 现货数据失败: {str(e)}")
    
    async def _collect_futures_data(self, symbols: List[str]):
        """
        收集合约数据
        
        Args:
            symbols: 合约交易对列表
        """
        Logger.info(f"开始收集 {self.exchange_name} 合约数据: {symbols}")
        
        # 获取数据收集间隔配置
        intervals = get_data_intervals(self.config)
        
        try:
            # 设置合约类型
            if hasattr(self.exchange, 'options'):
                self.exchange.options['defaultType'] = 'swap'  # 永续合约
            
            # 为每个符号创建数据监听任务
            for symbol in symbols:
                if not validate_symbol(symbol):
                    Logger.warning(f"无效的交易对符号: {symbol}")
                    continue
                
                # 启动ticker数据收集
                if 'ticker' in intervals:
                    task = asyncio.create_task(
                        self._watch_ticker(symbol, intervals['ticker'])
                    )
                    self.tasks.append(task)
                
                # 启动orderbook数据收集
                if 'orderbook' in intervals:
                    task = asyncio.create_task(
                        self._watch_orderbook(symbol, intervals['orderbook'])
                    )
                    self.tasks.append(task)
                
                # 启动trade数据收集
                if 'trades' in intervals:
                    task = asyncio.create_task(
                        self._watch_trades(symbol, intervals['trades'])
                    )
                    self.tasks.append(task)
                
                # 启动K线数据收集任务（从配置文件读取时间周期）
                timeframes = get_ohlcv_timeframes(self.config)
                for timeframe in timeframes:
                    task = asyncio.create_task(
                        self._watch_ohlcv(symbol, timeframe)
                    )
                    self.tasks.append(task)
                
                # 短暂延迟避免请求过于频繁
                await asyncio.sleep(0.1)
            
            # 保持任务运行
            while True:
                await asyncio.sleep(1)
                
        except Exception as e:
            Logger.error(f"收集 {self.exchange_name} 合约数据失败: {str(e)}")
    
    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_ticker(self, symbol: str, interval: float):
        """
        监听ticker数据（使用WebSocket）
        
        Args:
            symbol: 交易对
            interval: 监听间隔(秒) - 对于WebSocket，这个参数主要用于重试间隔
        """
        Logger.debug(f"开始监听 {self.exchange_name} {symbol} ticker数据（WebSocket）")
        
        while True:
            try:
                # 使用WebSocket获取ticker数据
                ticker = await self.exchange.watch_ticker(symbol)
                
                # 添加时间戳
                ticker['timestamp'] = get_current_timestamp_ms()
                
                # 存储到数据库
                self.db_manager.insert_data(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    data_type='ticker',
                    data=ticker
                )
                
                # 通知回调函数
                self._notify_callbacks('ticker', self.exchange_name, symbol, ticker)
                
                Logger.debug(f"收到 {self.exchange_name} {symbol} ticker数据: {ticker.get('last', 'N/A')}")
                
                # WebSocket是持续推送，不需要sleep，但为了防止CPU占用过高，可以添加短暂休眠
                await asyncio.sleep(0.1)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name} {symbol} ticker数据失败: {str(e)}")
                await asyncio.sleep(interval)  # 出错后等待指定时间再重试
    
    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_orderbook(self, symbol: str, interval: float):
        """
        监听orderbook数据（使用WebSocket）
        
        Args:
            symbol: 交易对
            interval: 监听间隔(秒) - 对于WebSocket，这个参数主要用于重试间隔
        """
        Logger.debug(f"开始监听 {self.exchange_name} {symbol} orderbook数据（WebSocket）")
        
        while True:
            try:
                # 使用WebSocket获取orderbook数据，处理不同交易所的limit参数支持
                try:
                    orderbook = await self.exchange.watch_order_book(symbol, limit=20)
                except Exception:
                    # 如果不支持limit参数，尝试不带limit参数调用
                    orderbook = await self.exchange.watch_order_book(symbol)
                
                # 添加时间戳
                orderbook['timestamp'] = get_current_timestamp_ms()
                
                # 存储到数据库
                self.db_manager.insert_data(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    data_type='orderbook',
                    data=orderbook
                )
                
                # 通知回调函数
                self._notify_callbacks('orderbook', self.exchange_name, symbol, orderbook)
                
                Logger.debug(f"收到 {self.exchange_name} {symbol} orderbook数据")
                
                # WebSocket是持续推送，不需要sleep，但为了防止CPU占用过高，可以添加短暂休眠
                await asyncio.sleep(0.1)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name} {symbol} orderbook数据失败: {str(e)}")
                await asyncio.sleep(interval)  # 出错后等待指定时间再重试
    
    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_trades(self, symbol: str, interval: float):
        """
        监听trade数据（使用WebSocket）
        
        Args:
            symbol: 交易对
            interval: 监听间隔(秒) - 对于WebSocket，这个参数主要用于重试间隔
        """
        Logger.debug(f"开始监听 {self.exchange_name} {symbol} trade数据（WebSocket）")
        
        while True:
            try:
                # 使用WebSocket获取trade数据
                trades = await self.exchange.watch_trades(symbol)
                
                # 处理trade数据（可能是单个trade或trade列表）
                if not isinstance(trades, list):
                    trades = [trades]
                
                for trade in trades:
                    # 添加时间戳
                    trade['timestamp'] = get_current_timestamp_ms()
                    
                    # 存储到数据库
                    self.db_manager.insert_data(
                        exchange=self.exchange_name,
                        symbol=symbol,
                        data_type='trades',
                        data=trade
                    )
                    
                    # 通知回调函数
                    self._notify_callbacks('trade', self.exchange_name, symbol, trade)
                
                Logger.debug(f"收到 {self.exchange_name} {symbol} trade数据: {len(trades)} 笔")
                
                # WebSocket是持续推送，不需要sleep，但为了防止CPU占用过高，可以添加短暂休眠
                await asyncio.sleep(0.1)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name} {symbol} trade数据失败: {str(e)}")
                await asyncio.sleep(interval)  # 出错后等待指定时间再重试
    
    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_ohlcv(self, symbol: str, timeframe: str = '1h'):
        """
        监听K线数据（使用WebSocket）
        
        Args:
            symbol: 交易对
            timeframe: 时间周期 (1m, 5m, 15m, 1h, 4h, 1d等)
        """
        Logger.debug(f"开始监听 {self.exchange_name} {symbol} {timeframe} K线数据（WebSocket）")
        
        # 添加调试信息
        Logger.debug(f"Exchange对象: {self.exchange}")
        Logger.debug(f"Exchange类型: {type(self.exchange)}")
        Logger.debug(f"Exchange options: {getattr(self.exchange, 'options', 'No options attribute')}")
        
        # 检查exchange是否支持watch_ohlcv
        if not hasattr(self.exchange, 'watch_ohlcv'):
            Logger.error(f"Exchange {self.exchange_name} 不支持watch_ohlcv方法")
            return
            
        # 检查symbol是否正确
        Logger.debug(f"监听交易对: {symbol}")
        
        # 检查exchange是否已经连接
        if not self.exchange.has['watchOHLCV']:
            Logger.error(f"Exchange {self.exchange_name} 不支持watchOHLCV功能")
            return
        
        while True:
            try:
                Logger.debug(f"尝试获取 {symbol} {timeframe} K线数据...")
                # 使用WebSocket获取K线数据
                ohlcv = await self.exchange.watch_ohlcv(symbol, timeframe, limit=50)
                
                if ohlcv:
                    # 获取最新的K线数据
                    latest_candle = ohlcv[-1]
                    
                    # 准备数据
                    data = {
                        'open': latest_candle[1],
                        'high': latest_candle[2],
                        'low': latest_candle[3],
                        'close': latest_candle[4],
                        'volume': latest_candle[5],
                        'timestamp': latest_candle[0]
                    }
                    
                    # 存储到数据库
                    self.db_manager.insert_data(
                        exchange=self.exchange_name,
                        symbol=symbol,
                        data_type='ohlcv',
                        data=data,
                        timeframe=timeframe
                    )
                    
                    # 通知回调函数
                    self._notify_callbacks('ohlcv', self.exchange_name, symbol, {
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'ohlcv': latest_candle
                    })
                    
                    Logger.debug(f"收到 {self.exchange_name} {symbol} {timeframe} K线数据")
                else:
                    Logger.warning(f"获取到空的K线数据: {ohlcv}")
                
                # WebSocket是持续推送，不需要sleep，但为了防止CPU占用过高，可以添加短暂休眠
                await asyncio.sleep(0.1)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name} {symbol} {timeframe} K线数据失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再重试

    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_mytrades(self, interval: float):
        """
        监听成交记录数据
        
        Args:
            interval: 监听间隔(秒)
        """
        Logger.debug(f"开始监听 {self.exchange_name}:{self.account_name} 成交记录数据")
        
        while True:
            try:
                # 获取成交记录数据
                try:
                    mytrades = await self.exchange.fetch_my_trades()
                except Exception as e:
                    # 尝试使用其他方法获取成交记录
                    Logger.warning(f"标准fetch_my_trades失败，尝试其他方法: {str(e)}")
                    # 创建一个空的成交记录列表
                    mytrades = []
                
                # 处理成交记录数据
                for trade in mytrades:
                    # 添加账户信息，但不覆盖原始数据
                    processed_trade = trade.copy()
                    processed_trade['account'] = self.account_name
                    processed_trade['collector_timestamp'] = get_current_timestamp_ms()
                    
                    # 存储到数据库
                    self.db_manager.insert_data(self.exchange_name, trade.get('symbol', ''), 'mytrades', processed_trade, self.user_id)
                    
                    # 通知回调函数
                    self._notify_callbacks('mytrade', self.exchange_name, self.account_name, trade)
                
                Logger.debug(f"收到 {self.exchange_name}:{self.account_name} 成交记录数据: {len(mytrades)} 笔")
                
                # 等待下一次收集
                await asyncio.sleep(interval)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name}:{self.account_name} 成交记录数据失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再重试
    
    def _timeframe_to_ms(self, timeframe: str) -> int:
        """
        将时间周期转换为毫秒
        
        Args:
            timeframe: 时间周期字符串 (1m, 5m, 15m, 1h, 4h, 1d等)
            
        Returns:
            时间周期对应的毫秒数
        """
        # 提取数字和单位
        import re
        match = re.match(r'(\d+)([mhd])', timeframe)
        if not match:
            raise ValueError(f"无效的时间周期格式: {timeframe}")
        
        value = int(match.group(1))
        unit = match.group(2)
        
        # 转换为毫秒
        multipliers = {
            'm': 60 * 1000,      # 分钟
            'h': 60 * 60 * 1000, # 小时
            'd': 24 * 60 * 60 * 1000  # 天
        }
        
        if unit not in multipliers:
            raise ValueError(f"不支持的时间周期单位: {unit}")
        
        return value * multipliers[unit]


class PrivateDataCollector:
    """
    私有数据收集器类
    负责收集私有数据（余额、订单、持仓等）
    需要API密钥
    """
    
    def __init__(self, exchange_name: str, account_name: str, account_config: Dict, config: Dict):
        """
        初始化私有数据收集器
        
        Args:
            exchange_name: 交易所名称
            account_name: 账户名称
            account_config: 账户配置
            config: 全局配置
        """
        self.exchange_name = exchange_name
        self.account_name = account_name
        self.account_config = account_config
        self.config = config
        self.exchange = None
        self.tasks = []
        self.callbacks = {}
        
        # 初始化数据库管理器
        self.db_manager = DatabaseManager(config)
        
        # 构建用户ID，格式为：API密钥前六位_账户名称
        api_key = self.account_config.get('api_key', '')
        api_prefix = api_key[:6] if api_key else "default"
        self.user_id = f"{api_prefix}_{self.account_name}"
        
        # 初始化交易所连接
        self._init_exchange()
        
        Logger.info(f"私有数据收集器 {exchange_name}:{account_name} 初始化完成，用户ID: {self.user_id}")
    
    def _init_exchange(self):
        """初始化交易所连接（私有数据需要API密钥）"""
        try:
            # 获取交易所类
            exchange_class = getattr(ccxtpro, self.exchange_name)
            
            # 基础配置（需要API密钥）
            exchange_config = {
                'apiKey': self.account_config.get('api_key', ''),
                'secret': self.account_config.get('secret', ''),
                'sandbox': self.config.get('global', {}).get('testnet', False),
                'enableRateLimit': True,
            }
            
            # 如果有密码或UID，也添加到配置中
            if 'password' in self.account_config:
                exchange_config['password'] = self.account_config['password']
            if 'uid' in self.account_config:
                exchange_config['uid'] = self.account_config['uid']
            
            # 创建交易所实例
            self.exchange = exchange_class(exchange_config)
            
            Logger.info(f"交易所 {self.exchange_name}:{self.account_name} 私有数据连接初始化成功")
            
        except Exception as e:
            Logger.error(f"初始化交易所 {self.exchange_name}:{self.account_name} 私有数据连接失败: {str(e)}")
            raise
    
    def register_callback(self, data_type: str, callback: Callable):
        """
        注册数据回调函数
        
        Args:
            data_type: 数据类型 (balance, order, position, etc.)
            callback: 回调函数，接收 (exchange, account, data) 参数
        """
        if data_type not in self.callbacks:
            self.callbacks[data_type] = []
        self.callbacks[data_type].append(callback)
        Logger.debug(f"注册 {data_type} 数据回调函数")
    
    def _notify_callbacks(self, data_type: str, exchange: str, account: str, data: Any):
        """
        通知所有回调函数
        
        Args:
            data_type: 数据类型
            exchange: 交易所名称
            account: 账户名称
            data: 数据
        """
        if data_type in self.callbacks:
            for callback in self.callbacks[data_type]:
                try:
                    callback(exchange, account, data)
                except Exception as e:
                    Logger.error(f"回调函数执行失败: {str(e)}")
    
    async def start(self):
        """启动私有数据收集"""
        Logger.info(f"启动 {self.exchange_name}:{self.account_name} 私有数据收集")
        
        # 获取数据收集间隔配置
        intervals = get_data_intervals(self.config)
        
        # 创建余额数据收集任务
        if 'balance' in intervals:
            task = asyncio.create_task(
                self._watch_balance(intervals['balance'])
            )
            self.tasks.append(task)
        
        # 创建订单数据收集任务
        if 'orders' in intervals:
            task = asyncio.create_task(
                self._watch_orders(intervals['orders'])
            )
            self.tasks.append(task)
        
        # 创建成交记录数据收集任务
        if 'mytrades' in intervals:
            task = asyncio.create_task(
                self._watch_mytrades(intervals['mytrades'])
            )
            self.tasks.append(task)
        
        # 创建持仓数据收集任务
        if 'positions' in intervals:
            task = asyncio.create_task(
                self._watch_positions(intervals['positions'])
            )
            self.tasks.append(task)
        
        Logger.info(f"启动了 {len(self.tasks)} 个 {self.exchange_name}:{self.account_name} 私有数据收集任务")
    
    async def stop(self):
        """停止私有数据收集"""
        Logger.info(f"停止 {self.exchange_name}:{self.account_name} 私有数据收集")
        
        # 取消所有任务
        for task in self.tasks:
            task.cancel()
        
        # 等待任务完成
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # 关闭交易所连接
        if self.exchange:
            try:
                await self.exchange.close()
                Logger.info(f"已关闭交易所 {self.exchange_name}:{self.account_name} 私有数据连接")
            except Exception as e:
                Logger.error(f"关闭交易所 {self.exchange_name}:{self.account_name} 私有数据连接失败: {str(e)}")
        
        self.tasks.clear()
        Logger.info(f"{self.exchange_name}:{self.account_name} 私有数据收集器已停止")
    
    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_balance(self, interval: float):
        """
        监听余额数据
        
        Args:
            interval: 监听间隔(秒)
        """
        Logger.debug(f"开始监听 {self.exchange_name}:{self.account_name} 余额数据")
        
        while True:
            try:
                # 获取余额数据
                try:
                    balance = await self.exchange.fetch_balance()
                except Exception as e:
                    # 尝试使用其他方法获取余额
                    Logger.warning(f"标准fetch_balance失败，尝试其他方法: {str(e)}")
                    # 创建一个最小化的余额对象
                    balance = {
                        'info': {},
                        'timestamp': get_current_timestamp_ms(),
                        'datetime': None,
                        'free': {},
                        'used': {},
                        'total': {}
                    }
                
                # 添加时间戳和账户信息
                balance['timestamp'] = get_current_timestamp_ms()
                balance['account'] = self.account_name
                
                # 存储到数据库
                self.db_manager.insert_data(self.exchange_name, '', 'balance', balance, self.user_id)
                
                # 通知回调函数
                self._notify_callbacks('balance', self.exchange_name, self.account_name, balance)
                
                Logger.debug(f"收到 {self.exchange_name}:{self.account_name} 余额数据")
                
                # 等待下一次收集
                await asyncio.sleep(interval)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name}:{self.account_name} 余额数据失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再重试

    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_orders(self, interval: float):
        """
        监听订单数据
        
        Args:
            interval: 监听间隔(秒)
        """
        Logger.debug(f"开始监听 {self.exchange_name}:{self.account_name} 订单数据")
        
        while True:
            try:
                # 获取订单数据 - 使用fetchOpenOrders替代fetchOrders
                if hasattr(self.exchange, 'fetchOpenOrders'):
                    orders = await self.exchange.fetchOpenOrders()
                else:
                    orders = await self.exchange.fetch_orders()
                
                # 处理订单数据
                for order in orders:
                    # 添加时间戳和账户信息
                    order['timestamp'] = get_current_timestamp_ms()
                    order['account'] = self.account_name
                    
                    # 存储到数据库
                    self.db_manager.insert_data(self.exchange_name, order.get('symbol', ''), 'orders', order, self.user_id)
                    
                    # 通知回调函数
                    self._notify_callbacks('order', self.exchange_name, self.account_name, order)
                
                Logger.debug(f"收到 {self.exchange_name}:{self.account_name} 订单数据: {len(orders)} 笔")
                
                # 等待下一次收集
                await asyncio.sleep(interval)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name}:{self.account_name} 订单数据失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再重试

    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_positions(self, interval: float):
        """
        监听持仓数据
        
        Args:
            interval: 监听间隔(秒)
        """
        Logger.debug(f"开始监听 {self.exchange_name}:{self.account_name} 持仓数据")
        
        while True:
            try:
                # 获取持仓数据
                try:
                    positions = await self.exchange.fetch_positions()
                except Exception as e:
                    # 尝试使用其他方法获取持仓
                    Logger.warning(f"标准fetch_positions失败，尝试其他方法: {str(e)}")
                    # 创建一个空的持仓列表
                    positions = []
                
                # 处理持仓数据
                for position in positions:
                    # 添加时间戳和账户信息
                    position['timestamp'] = get_current_timestamp_ms()
                    position['account'] = self.account_name
                    
                    # 存储到数据库
                    self.db_manager.insert_data(self.exchange_name, position.get('symbol', ''), 'positions', position, self.user_id)
                    
                    # 通知回调函数
                    self._notify_callbacks('position', self.exchange_name, self.account_name, position)
                
                Logger.debug(f"收到 {self.exchange_name}:{self.account_name} 持仓数据: {len(positions)} 笔")
                
                # 等待下一次收集
                await asyncio.sleep(interval)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name}:{self.account_name} 持仓数据失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再重试

    @retry_on_failure(max_retries=3, delay=2.0)
    async def _watch_mytrades(self, interval: float):
        """
        监听成交记录数据
        
        Args:
            interval: 监听间隔(秒)
        """
        Logger.debug(f"开始监听 {self.exchange_name}:{self.account_name} 成交记录数据")
        
        while True:
            try:
                # 获取成交记录数据
                try:
                    mytrades = await self.exchange.fetch_my_trades()
                except Exception as e:
                    # 尝试使用其他方法获取成交记录
                    Logger.warning(f"标准fetch_my_trades失败，尝试其他方法: {str(e)}")
                    # 创建一个空的成交记录列表
                    mytrades = []
                
                # 处理成交记录数据
                for trade in mytrades:
                    # 添加时间戳和账户信息
                    trade['timestamp'] = get_current_timestamp_ms()
                    trade['account'] = self.account_name
                    
                    # 存储到数据库
                    self.db_manager.insert_data(self.exchange_name, trade.get('symbol', ''), 'mytrades', trade, self.user_id)
                    
                    # 通知回调函数
                    self._notify_callbacks('mytrade', self.exchange_name, self.account_name, trade)
                
                Logger.debug(f"收到 {self.exchange_name}:{self.account_name} 成交记录数据: {len(mytrades)} 笔")
                
                # 等待下一次收集
                await asyncio.sleep(interval)
                
            except Exception as e:
                Logger.error(f"监听 {self.exchange_name}:{self.account_name} 成交记录数据失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再重试


class DataCollector:
    """
    数据收集器管理类
    负责管理公共和私有数据收集器
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化数据收集器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = load_config(config_path)
        self.public_collectors = {}  # 公共数据收集器
        self.private_collectors = {}  # 私有数据收集器
        self.is_running = False
        self.callbacks = {}  # 数据回调函数
        
        # 初始化收集器
        self._init_collectors()
        
        Logger.info("数据收集器初始化完成")
    
    def _init_collectors(self):
        """初始化所有收集器"""
        enabled_exchanges = get_enabled_exchanges(self.config)
        
        for exchange_name in enabled_exchanges:
            try:
                # 获取交易所配置
                symbols = get_exchange_symbols(self.config, exchange_name)
                if not symbols:
                    Logger.warning(f"交易所 {exchange_name} 没有配置订阅币对，跳过公共数据收集")
                    continue
                
                # 创建公共数据收集器
                public_collector = PublicDataCollector(exchange_name, symbols, self.config)
                self.public_collectors[exchange_name] = public_collector
                
                # 获取账户配置
                accounts = self.config.get('exchanges', {}).get(exchange_name, {}).get('accounts', {})
                
                # 为每个账户创建私有数据收集器
                for account_name, account_config in accounts.items():
                    if not account_config.get('api_key') or not account_config.get('secret'):
                        Logger.warning(f"账户 {exchange_name}:{account_name} 缺少API密钥，跳过私有数据收集")
                        continue
                    
                    private_collector = PrivateDataCollector(exchange_name, account_name, account_config, self.config)
                    self.private_collectors[f"{exchange_name}:{account_name}"] = private_collector
                
                Logger.info(f"交易所 {exchange_name} 收集器初始化完成")
                
            except Exception as e:
                Logger.error(f"初始化交易所 {exchange_name} 收集器失败: {str(e)}")
    
    def register_callback(self, data_type: str, callback: Callable):
        """
        注册数据回调函数
        
        Args:
            data_type: 数据类型 (ticker, orderbook, trade, balance, order, position, etc.)
            callback: 回调函数，接收 (exchange, symbol/account, data) 参数
        """
        if data_type not in self.callbacks:
            self.callbacks[data_type] = []
        self.callbacks[data_type].append(callback)
        Logger.debug(f"注册 {data_type} 数据回调函数")
        
        # 将回调函数注册到所有相关的收集器
        # 公共数据类型
        if data_type in ['ticker', 'orderbook', 'trade']:
            for collector in self.public_collectors.values():
                collector.register_callback(data_type, callback)
        
        # 私有数据类型
        elif data_type in ['balance', 'order', 'position']:
            for collector in self.private_collectors.values():
                collector.register_callback(data_type, callback)
    
    async def start(self):
        """启动所有数据收集器"""
        if self.is_running:
            Logger.warning("数据收集器已在运行")
            return
        
        self.is_running = True
        Logger.info("启动数据收集器")
        
        tasks = []
        
        # 启动公共数据收集器
        for exchange_name, collector in self.public_collectors.items():
            task = asyncio.create_task(collector.start())
            tasks.append(task)
        
        # 启动私有数据收集器
        for account_key, collector in self.private_collectors.items():
            task = asyncio.create_task(collector.start())
            tasks.append(task)
        
        Logger.info(f"启动了 {len(tasks)} 个数据收集器任务")
        
        # 等待所有任务完成（实际上它们会一直运行）
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def start_public_only(self):
        """只启动公共数据收集器"""
        if self.is_running:
            Logger.warning("数据收集器已在运行")
            return
        
        self.is_running = True
        Logger.info("启动公共数据收集器")
        
        tasks = []
        
        # 启动公共数据收集器
        for exchange_name, collector in self.public_collectors.items():
            task = asyncio.create_task(collector.start())
            tasks.append(task)
        
        Logger.info(f"启动了 {len(tasks)} 个公共数据收集器任务")
        
        # 等待所有任务完成（实际上它们会一直运行）
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop(self):
        """停止所有数据收集器"""
        if not self.is_running:
            return
        
        self.is_running = False
        Logger.info("停止数据收集器")
        
        tasks = []
        
        # 停止公共数据收集器
        for exchange_name, collector in self.public_collectors.items():
            task = asyncio.create_task(collector.stop())
            tasks.append(task)
        
        # 停止私有数据收集器
        for account_key, collector in self.private_collectors.items():
            task = asyncio.create_task(collector.stop())
            tasks.append(task)
        
        # 等待所有任务完成
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        Logger.info("数据收集器已停止")


class CollectorManager:
    """
    收集器管理类
    提供同步接口管理异步数据收集器
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化收集器管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.collector = None
        self.loop = None
        self.thread = None
        self.is_started = False
    
    def start(self):
        """启动数据收集器（同步接口）"""
        if self.is_started:
            Logger.warning("收集器管理器已启动")
            return
        
        # 创建新的事件循环
        self.loop = asyncio.new_event_loop()
        
        # 在新线程中运行事件循环
        self.thread = threading.Thread(target=self._run_loop)
        self.thread.daemon = True
        self.thread.start()
        
        # 等待收集器启动
        while not self.is_started:
            time.sleep(0.1)
        
        Logger.info("收集器管理器已启动")
    
    def start_public_only(self):
        """只启动公共数据收集器（同步接口）"""
        if self.is_started:
            Logger.warning("收集器管理器已启动")
            return
        
        # 创建新的事件循环
        self.loop = asyncio.new_event_loop()
        
        # 在新线程中运行事件循环
        self.thread = threading.Thread(target=self._run_loop_public_only)
        self.thread.daemon = True
        self.thread.start()
        
        # 等待收集器启动
        while not self.is_started:
            time.sleep(0.1)
        
        Logger.info("收集器管理器（仅公共数据）已启动")
    
    def stop(self):
        """停止数据收集器（同步接口）"""
        if not self.is_started:
            return
        
        # 在事件循环中停止收集器
        asyncio.run_coroutine_threadsafe(self._stop_collector(), self.loop).result()
        
        # 等待线程结束
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        # 关闭事件循环
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        
        self.is_started = False
        Logger.info("收集器管理器已停止")
    
    def _run_loop(self):
        """运行事件循环"""
        asyncio.set_event_loop(self.loop)
        
        # 创建并启动收集器
        self.collector = DataCollector(self.config_path)
        
        # 启动收集器
        asyncio.run_coroutine_threadsafe(self.collector.start(), self.loop)
        self.is_started = True
        
        # 运行事件循环
        self.loop.run_forever()
    
    def _run_loop_public_only(self):
        """运行事件循环（仅公共数据）"""
        asyncio.set_event_loop(self.loop)
        
        # 创建并启动收集器
        self.collector = DataCollector(self.config_path)
        
        # 只启动公共数据收集器
        asyncio.run_coroutine_threadsafe(self.collector.start_public_only(), self.loop)
        self.is_started = True
        
        # 运行事件循环
        self.loop.run_forever()
    
    async def _stop_collector(self):
        """停止收集器（异步）"""
        if self.collector:
            await self.collector.stop()
    
    def register_callback(self, data_type: str, callback: Callable):
        """
        注册数据回调函数
        
        Args:
            data_type: 数据类型
            callback: 回调函数
        """
        if self.collector:
            self.collector.register_callback(data_type, callback)
        else:
            Logger.warning("收集器未初始化，无法注册回调函数")
    
    @property
    def is_running(self) -> bool:
        """获取收集器运行状态"""
        return self.is_started


# 全局收集器实例
_global_collector = None

def get_collector(config_path: str = "config.yaml", public_only: bool = True) -> CollectorManager:
    """
    获取全局数据收集器实例
    如果收集器不存在，则创建并自动启动
    
    Args:
        config_path: 配置文件路径
        public_only: 是否只启动公共数据收集器
        
    Returns:
        CollectorManager: 数据收集器管理器实例
    """
    global _global_collector
    
    if _global_collector is None:
        _global_collector = CollectorManager(config_path)
        
        # 注册退出时的清理函数
        atexit.register(_cleanup_collector)
        
        # 注册信号处理函数，确保程序退出时停止收集器
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        
        # 自动启动收集器
        if public_only:
            _global_collector.start_public_only()
        else:
            _global_collector.start()
        
        Logger.info(f"数据收集器已自动启动（仅公共数据: {public_only}）")
    
    return _global_collector


def _cleanup_collector():
    """清理全局收集器实例"""
    global _global_collector
    
    if _global_collector is not None:
        Logger.info("程序退出，正在停止数据收集器...")
        _global_collector.stop()
        _global_collector = None


def _signal_handler(signum, frame):
    """信号处理函数"""
    Logger.info(f"接收到信号 {signum}，正在停止数据收集器...")
    _cleanup_collector()
    sys.exit(0)