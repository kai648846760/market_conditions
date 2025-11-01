"""
数据库操作模块
负责管理SQLite数据库的创建、读写和清理
"""

import os
import sqlite3
import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import threading

from .logger import Logger


class DatabaseManager:
    """
    数据库管理器类
    负责管理SQLite数据库的连接、创建表、数据读写和清理
    """
    
    def __init__(self, config: dict):
        """
        初始化数据库管理器
        
        Args:
            config: 数据库配置字典
        """
        self.config = config
        self.data_dir = Path(config.get('data_dir', './data'))
        self.retention_days = config.get('retention_days', 7)
        self.cleanup_enabled = config.get('cleanup', {}).get('enabled', True)
        self.cleanup_time = config.get('cleanup', {}).get('time', '02:00')
        self.cleanup_interval = config.get('cleanup', {}).get('interval', 1)
        
        # 连接池配置
        self.pool_size = config.get('connection', {}).get('pool_size', 10)
        self.timeout = config.get('connection', {}).get('timeout', 30)
        self.max_retries = config.get('connection', {}).get('max_retries', 3)
        
        # 连接池
        self.connections = {}
        self.lock = threading.Lock()
        
        # 确保数据目录存在
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化日志记录器
        self.logger = Logger.get_logger('database')
        
        # 启动清理任务
        if self.cleanup_enabled:
            self._schedule_cleanup()
    
    def _get_db_path(self, exchange: str, symbol: str, data_type: str = 'public', user_id: str = None) -> Path:
        """
        获取数据库文件路径
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            data_type: 数据类型 (ticker/orderbook/trades/ohlcv/balance/orders/mytrades/positions)
            user_id: 用户ID（私有数据时使用）
            
        Returns:
            Path: 数据库文件路径
        """
        # 处理符号格式，将/替换为_
        safe_symbol = symbol.replace('/', '_')
        
        # 判断是公共数据还是私有数据
        if data_type in ['ticker', 'orderbook', 'trades', 'ohlcv']:
            # 公共数据
            # 判断是现货还是合约
            if ':' in symbol:  # 合约格式，如 BTC/USDT:USDT
                # 合约数据路径: data_dir/exchange/futures/symbol_settlement_currency.db
                parts = symbol.split(':')
                base_quote = parts[0].replace('/', '_')
                settlement = parts[1]
                db_name = f"{base_quote}_{settlement}.db"
                db_path = self.data_dir / exchange / 'futures' / db_name
            else:  # 现货格式，如 BTC/USDT
                # 现货数据路径: data_dir/exchange/spot/symbol.db
                db_name = f"{safe_symbol}.db"
                db_path = self.data_dir / exchange / 'spot' / db_name
        elif data_type in ['balance', 'orders', 'mytrades', 'positions']:
            # 私有数据路径: data_dir/exchange/private/{api_key前六位}_{账户名称}.db
            # 从user_id中提取API密钥前六位和账户名称
            if '_' in user_id:
                api_prefix, account_name = user_id.split('_', 1)
            else:
                # 如果没有下划线，则使用整个user_id作为账户名称，API前缀默认为"default"
                api_prefix = "default"
                account_name = user_id
            
            db_name = f"{api_prefix}_{account_name}.db"
            db_path = self.data_dir / exchange / 'private' / db_name
        else:
            # 其他私有数据类型，使用private作为目录名
            db_name = f"{user_id}_private.db"
            db_path = self.data_dir / exchange / 'private' / db_name
        
        # 确保目录存在
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        return db_path
    
    def _get_connection(self, db_path: Path) -> sqlite3.Connection:
        """
        获取数据库连接（使用连接池）
        
        Args:
            db_path: 数据库文件路径
            
        Returns:
            sqlite3.Connection: 数据库连接
        """
        db_path_str = str(db_path)
        
        with self.lock:
            if db_path_str not in self.connections:
                # 创建新连接
                conn = sqlite3.connect(
                    db_path_str,
                    timeout=self.timeout,
                    check_same_thread=False
                )
                conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
                self.connections[db_path_str] = conn
            
            return self.connections[db_path_str]
    
    def _create_tables(self, conn: sqlite3.Connection, table_name: str):
        """
        创建数据表
        
        Args:
            conn: 数据库连接
            table_name: 表名
        """
        cursor = conn.cursor()
        
        # 创建不同类型的表
        if table_name == 'ticker':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ticker (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    data TEXT NOT NULL,
                    UNIQUE(timestamp, symbol)
                )
            ''')
        elif table_name == 'orderbook':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS orderbook (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    data TEXT NOT NULL,
                    UNIQUE(timestamp, symbol)
                )
            ''')
        elif table_name == 'trades':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    data TEXT NOT NULL,
                    UNIQUE(timestamp, symbol, data)
                )
            ''')
        elif table_name == 'ohlcv':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ohlcv (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    data TEXT NOT NULL,
                    UNIQUE(timestamp, symbol, timeframe)
                )
            ''')
        elif table_name == 'balance':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS balance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    datetime TEXT NOT NULL,
                    usdt REAL NOT NULL DEFAULT 0,
                    data TEXT NOT NULL,
                    UNIQUE(datetime)
                )
            ''')
        elif table_name == 'orders':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    type TEXT NOT NULL,
                    price REAL NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    data TEXT NOT NULL
                )
            ''')
        elif table_name == 'mytrades':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mytrades (
                    id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    type TEXT NOT NULL,
                    price REAL NOT NULL,
                    amount REAL NOT NULL,
                    fee REAL NOT NULL,
                    timestamp INTEGER NOT NULL,
                    data TEXT NOT NULL
                )
            ''')
        elif table_name == 'positions':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT,
                    data TEXT NOT NULL
                )
            ''')
        
        conn.commit()
    
    def insert_data(self, exchange: str, symbol: str, data_type: str, data: Union[Dict, List], 
                   user_id: str = None, timeframe: str = None) -> bool:
        """
        插入数据到数据库
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            data_type: 数据类型 (ticker/orderbook/trades/ohlcv/balance/orders/mytrades/positions)
            data: 要插入的数据（字典或列表）
            user_id: 用户ID（私有数据时使用）
            timeframe: 时间框架（K线数据使用）
            
        Returns:
            bool: 插入是否成功
        """
        try:
            # 获取数据库路径
            db_type = 'private' if data_type in ['balance', 'orders', 'mytrades', 'positions'] else 'public'
            # 直接传递data_type给_get_db_path，而不是db_type
            db_path = self._get_db_path(exchange, symbol, data_type, user_id)
            
            # 获取连接
            conn = self._get_connection(db_path)
            
            # 创建表（如果不存在）
            self._create_tables(conn, data_type)
            
            # 准备数据
            data_json = json.dumps(data)
            
            # 插入数据
            cursor = conn.cursor()
            
            if data_type in ['ticker', 'orderbook']:
                # ticker和orderbook使用唯一约束，替换旧数据
                # 使用原始数据中的时间戳，如果没有则使用当前时间戳
                timestamp = data.get('timestamp', int(time.time() * 1000))
                cursor.execute(f'''
                    INSERT OR REPLACE INTO {data_type} (timestamp, symbol, data)
                    VALUES (?, ?, ?)
                ''', (timestamp, symbol, data_json))
            elif data_type == 'ohlcv':
                # K线数据包含时间框架
                # 使用原始数据中的时间戳，如果没有则使用当前时间戳
                timestamp = data.get('timestamp', int(time.time() * 1000))
                cursor.execute(f'''
                    INSERT OR REPLACE INTO ohlcv (timestamp, symbol, timeframe, data)
                    VALUES (?, ?, ?, ?)
                ''', (timestamp, symbol, timeframe, data_json))
            elif data_type == 'trades':
                # 交易数据使用唯一约束，替换旧数据
                # 对于交易数据，如果是列表，每条交易都有自己的时间戳
                if isinstance(data, list) and data:
                    # 批量插入交易数据
                    for trade in data:
                        timestamp = trade.get('timestamp', int(time.time() * 1000))
                        trade_json = json.dumps(trade)
                        cursor.execute(f'''
                            INSERT OR REPLACE INTO trades (timestamp, symbol, data)
                            VALUES (?, ?, ?)
                        ''', (timestamp, symbol, trade_json))
                else:
                    # 单条交易数据
                    timestamp = data.get('timestamp', int(time.time() * 1000))
                    cursor.execute(f'''
                        INSERT OR REPLACE INTO trades (timestamp, symbol, data)
                        VALUES (?, ?, ?)
                    ''', (timestamp, symbol, data_json))
            else:
                # 私有数据
                # 使用原始数据中的时间戳，如果没有则使用当前时间戳
                timestamp = data.get('timestamp', int(time.time() * 1000))
                
                if data_type == 'balance':
                    # 处理balance数据，每天只保留一条
                    # 从时间戳获取日期
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    date_str = dt.strftime('%Y-%m-%d')
                    
                    # 提取USDT余额
                    usdt_balance = 0
                    if isinstance(data, dict):
                        # 尝试从不同的数据结构中提取USDT余额
                        if 'info' in data and 'result' in data['info']:
                            # Bybit API格式
                            result = data['info']['result']
                            if 'list' in result and result['list']:
                                account = result['list'][0]
                                if 'coin' in account:
                                    for coin in account['coin']:
                                        if coin.get('coin') == 'USDT':
                                            usdt_balance = float(coin.get('walletBalance', 0))
                                            break
                        elif 'coins' in data and 'USDT' in data['coins']:
                            # 已解析的格式
                            usdt_balance = float(data['coins']['USDT'].get('total', 0))
                    
                    # 检查当天是否已有数据，如果有则更新，否则插入
                    cursor.execute('''
                        INSERT OR REPLACE INTO balance (timestamp, datetime, usdt, data)
                        VALUES (?, ?, ?, ?)
                    ''', (timestamp, date_str, usdt_balance, data_json))
                    
                elif data_type == 'orders':
                    # 处理orders数据，根据返回数据的id做唯一处理
                    order_id = data.get('id', '')
                    order_symbol = data.get('symbol', '')
                    order_type = data.get('type', '')
                    order_price = float(data.get('price', 0))
                    order_amount = float(data.get('amount', 0))
                    order_status = data.get('status', '')
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO orders (id, symbol, type, price, amount, status, timestamp, data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (order_id, order_symbol, order_type, order_price, order_amount, order_status, timestamp, data_json))
                    
                else:
                    # 其他私有数据（mytrades, positions）
                    if data_type == 'mytrades':
                        # 处理mytrades数据
                        trade_id = data.get('id', '')
                        trade_symbol = data.get('symbol', '')
                        trade_type = data.get('type', '')
                        
                        # 安全地获取价格、数量和费用
                        trade_price = data.get('price', 0)
                        trade_amount = data.get('amount', 0)
                        trade_fee = data.get('fee', 0)
                        
                        # 如果是字典，尝试获取值
                        if isinstance(trade_price, dict):
                            trade_price = trade_price.get('value', 0)
                        if isinstance(trade_amount, dict):
                            trade_amount = trade_amount.get('value', 0)
                        if isinstance(trade_fee, dict):
                            trade_fee = trade_fee.get('value', 0)
                        
                        # 转换为float
                        trade_price = float(trade_price) if trade_price else 0
                        trade_amount = float(trade_amount) if trade_amount else 0
                        trade_fee = float(trade_fee) if trade_fee else 0
                        
                        cursor.execute('''
                            INSERT OR REPLACE INTO mytrades (id, symbol, type, price, amount, fee, timestamp, data)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (trade_id, trade_symbol, trade_type, trade_price, trade_amount, trade_fee, timestamp, data_json))
                    else:
                        # positions数据
                        position_symbol = data.get('symbol', '')
                        cursor.execute('''
                            INSERT INTO positions (timestamp, symbol, data)
                            VALUES (?, ?, ?)
                        ''', (timestamp, position_symbol, data_json))
            
            conn.commit()
            
            # 记录日志
            self.logger.debug(f"插入{data_type}数据成功: {exchange}/{symbol}")
            
            # 检查是否需要清理旧数据（仅在启用清理时）
            if self.cleanup_enabled:
                self._check_if_cleanup_needed(db_path, data_type)
            
            return True
            
        except Exception as e:
            self.logger.error(f"插入{data_type}数据失败: {str(e)}")
            return False
    
    def get_data(self, exchange: str, symbol: str, data_type: str, limit: int = 100,
                user_id: str = None, timeframe: str = None, 
                start_time: int = None, end_time: int = None) -> List[Dict]:
        """
        从数据库获取数据
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            data_type: 数据类型
            limit: 获取数据条数限制
            user_id: 用户ID（私有数据时使用）
            timeframe: 时间框架（K线数据使用）
            start_time: 开始时间戳（毫秒）
            end_time: 结束时间戳（毫秒）
            
        Returns:
            List[Dict]: 数据列表
        """
        try:
            # 获取数据库路径
            db_type = 'private' if data_type in ['balance', 'orders', 'mytrades', 'positions'] else 'public'
            # 直接传递data_type给_get_db_path，而不是db_type
            db_path = self._get_db_path(exchange, symbol, data_type, user_id)
            
            # 获取连接
            conn = self._get_connection(db_path)
            
            # 创建表（如果不存在）
            self._create_tables(conn, data_type)
            
            # 构建查询
            query = f"SELECT data FROM {data_type} WHERE 1=1"
            params = []
            
            if data_type in ['ticker', 'orderbook', 'ohlcv']:
                query += " AND symbol = ?"
                params.append(symbol)
                
                if data_type == 'ohlcv' and timeframe:
                    query += " AND timeframe = ?"
                    params.append(timeframe)
            elif data_type in ['balance', 'orders', 'mytrades', 'positions']:
                # 私有数据不再需要user_id条件，因为数据已按用户分库存储
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time)
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            # 执行查询
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # 解析数据
            result = []
            for row in rows:
                data = json.loads(row['data'])
                result.append(data)
            
            return result
            
        except Exception as e:
            self.logger.error(f"获取{data_type}数据失败: {str(e)}")
            return []
    
    def _check_if_cleanup_needed(self, db_path: Path, data_type: str):
        """
        检查是否需要清理旧数据
        
        Args:
            db_path: 数据库文件路径
            data_type: 数据类型
        """
        try:
            # 获取当前时间
            now = datetime.now()
            
            # 解析清理时间
            cleanup_hour, cleanup_minute = map(int, self.cleanup_time.split(':'))
            
            # 计算今天的清理时间点
            cleanup_time_today = now.replace(hour=cleanup_hour, minute=cleanup_minute, second=0, microsecond=0)
            
            # 如果当前时间已经过了今天的清理时间点，则检查今天是否已经清理过
            if now >= cleanup_time_today:
                # 使用文件修改时间作为上次清理时间的简单记录
                # 如果数据库文件不存在或修改时间早于今天的清理时间点，则执行清理
                if not db_path.exists():
                    return
                
                # 获取文件最后修改时间
                file_mtime = datetime.fromtimestamp(db_path.stat().st_mtime)
                
                # 如果文件最后修改时间早于今天的清理时间点，说明今天还没有清理过
                if file_mtime < cleanup_time_today:
                    self._check_and_cleanup_old_data(db_path, data_type)
                    self.logger.info(f"在指定时间{self.cleanup_time}执行了{data_type}数据清理")
            
        except Exception as e:
            self.logger.error(f"检查清理需求失败: {str(e)}")
    
    def _check_and_cleanup_old_data(self, db_path: Path, data_type: str):
        """
        检查并清理旧数据
        
        Args:
            db_path: 数据库文件路径
            data_type: 数据类型
        """
        try:
            # 计算截止时间戳
            cutoff_time = int((datetime.now() - timedelta(days=self.retention_days)).timestamp() * 1000)
            
            # 获取连接
            conn = self._get_connection(db_path)
            
            # 删除旧数据
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {data_type} WHERE timestamp < ?", (cutoff_time,))
            deleted_rows = cursor.rowcount
            conn.commit()
            
            if deleted_rows > 0:
                self.logger.info(f"清理{data_type}旧数据: {deleted_rows}条")
            
        except Exception as e:
            self.logger.error(f"清理{data_type}旧数据失败: {str(e)}")
    
    def _schedule_cleanup(self):
        """
        调度清理任务
        根据配置文件中的时间和间隔来执行清理
        """
        # 这里简化处理，实际项目中可以使用定时任务（如APScheduler）在指定时间执行
        # 当前实现：每次插入数据时检查是否需要清理
        # 清理逻辑在_check_if_cleanup_needed方法中实现
        pass
    
    def close_all_connections(self):
        """关闭所有数据库连接"""
        with self.lock:
            for conn in self.connections.values():
                conn.close()
            self.connections.clear()
            self.logger.info("所有数据库连接已关闭")