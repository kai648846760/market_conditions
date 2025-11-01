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
    
    def _get_full_user_id(self, exchange: str, user: str = None) -> str:
        """
        将简短用户名转换为完整的user_id
        
        Args:
            exchange: 交易所名称
            user: 用户名（如"default"、"user1"等），如果为None或空字符串，则使用默认账户
            
        Returns:
            str: 完整的user_id（如"Lr9E7W_default"）
        """
        # 如果没有提供用户名，使用默认账户
        if not user:
            user = "default"
            
        # 获取交易所配置
        exchange_config = self.config.get('exchanges', {}).get(exchange, {})
        accounts = exchange_config.get('accounts', {})
        
        # 检查用户名是否存在
        if user not in accounts:
            self.logger.warning(f"用户 '{user}' 在交易所 '{exchange}' 中不存在，使用默认账户")
            user = "default"
            
        # 获取API密钥前缀
        api_key = accounts.get(user, {}).get('api_key', '')
        if not api_key:
            self.logger.error(f"用户 '{user}' 的API密钥为空")
            return ""
            
        # 提取API密钥前缀（前6个字符）
        api_prefix = api_key[:6]
        
        # 构建完整的user_id
        full_user_id = f"{api_prefix}_{user}"
        
        self.logger.debug(f"用户名转换: {user} -> {full_user_id}")
        return full_user_id
    
    def get_balance(self, exchange: str, user: str = None) -> Optional[Dict]:
        """
        获取账户余额
        
        Args:
            exchange: 交易所名称
            user: 用户名（如"default"、"user1"等），如果为None或空字符串，则使用默认账户
            
        Returns:
            Optional[Dict]: 余额数据，如果没有找到则返回None
        """
        try:
            # 转换用户名为完整的user_id
            user_id = self._get_full_user_id(exchange, user)
            if not user_id:
                self.logger.error(f"无法获取用户ID: exchange={exchange}, user={user}")
                return None
                
            # 获取余额数据
            data_list = self.db_manager.get_data(
                exchange=exchange,
                symbol='',
                data_type='balance',
                limit=1,
                user_id=user_id
            )
            
            if data_list and len(data_list) > 0:
                data = data_list[0]
                
                # 如果数据是嵌套的，提取实际余额数据
                if isinstance(data, dict):
                    # 检查是否有info字段，这是bybit API的原始响应格式
                    if 'info' in data and 'result' in data['info']:
                        # 提取bybit格式的余额数据
                        result = data['info']['result']
                        if 'list' in result and result['list']:
                            # 提取第一个账户的余额信息
                            account = result['list'][0]
                            
                            # 计算总余额
                            total_equity = float(account.get('totalEquity', '0'))
                            total_available_balance = float(account.get('totalAvailableBalance', '0'))
                            
                            # 提取各币种余额
                            coin_balances = {}
                            if 'coin' in account:
                                for coin in account['coin']:
                                    coin_symbol = coin.get('coin', '')
                                    if coin_symbol and float(coin.get('walletBalance', '0')) > 0:
                                        coin_balances[coin_symbol] = {
                                            'free': coin.get('availableToWithdraw', '0'),
                                            'used': coin.get('locked', '0'),
                                            'total': coin.get('walletBalance', '0'),
                                            'equity': coin.get('equity', '0'),
                                            'usdValue': coin.get('usdValue', '0')
                                        }
                            
                            return {
                                'total': str(total_equity),
                                'free': str(total_available_balance),
                                'used': str(total_equity - total_available_balance),
                                'account': account.get('accountType', 'default'),
                                'coins': coin_balances,
                                'details': data
                            }
                    
                    # 如果有直接的余额字段
                    if 'total' in data and 'free' in data:
                        return data
                
                # 返回原始数据
                return data
            
            return None
            
        except Exception as e:
            self.logger.error(f"获取账户余额失败: {str(e)}")
            return None
    
    def get_orders(self, exchange: str, user: str = None, symbol: str = None, limit: int = None) -> List[Dict]:
        """
        获取订单数据
        
        Args:
            exchange: 交易所名称
            user: 用户名（如"default"、"user1"等），如果为None或空字符串，则使用默认账户
            symbol: 交易对，可选
            limit: 限制数量，可选
            
        Returns:
            List[Dict]: 订单列表
        """
        try:
            # 转换用户名为完整的user_id
            user_id = self._get_full_user_id(exchange, user)
            if not user_id:
                self.logger.error(f"无法获取用户ID: exchange={exchange}, user={user}")
                return []
                
            # 获取订单数据
            data_list = self.db_manager.get_data(
                exchange=exchange,
                symbol=symbol or '',
                data_type='orders',
                limit=limit or 100,
                user_id=user_id
            )
            
            if data_list:
                # 如果数据是嵌套的，提取实际订单数据
                if len(data_list) > 0 and isinstance(data_list[0], dict):
                    # 检查是否有info字段，这是bybit API的原始响应格式
                    if 'info' in data_list[0] and 'result' in data_list[0]['info']:
                        # 提取bybit格式的订单数据
                        orders = []
                        for item in data_list:
                            result = item['info']['result']
                            if 'list' in result:
                                for order in result['list']:
                                    orders.append({
                                        'id': order.get('orderId', ''),
                                        'symbol': order.get('symbol', ''),
                                        'type': order.get('orderType', ''),
                                        'price': order.get('price', '0'),
                                        'amount': order.get('qty', '0'),
                                        'status': order.get('orderStatus', ''),
                                        'timestamp': order.get('createdTime', ''),
                                        'details': order
                                    })
                        return orders
                    
                    # 如果有直接的订单字段
                    if 'id' in data_list[0] and 'symbol' in data_list[0]:
                        return data_list
                
                # 返回原始数据
                return data_list
            
            return []
            
        except Exception as e:
            self.logger.error(f"获取订单数据失败: {str(e)}")
            return []
    
    def get_my_trades(self, exchange: str, user: str = None, limit: int = 10, symbol: str = None) -> List[Dict]:
        """
        获取我的成交
        
        Args:
            exchange: 交易所名称
            user: 用户名（如"default"、"user1"等），如果为None或空字符串，则使用默认账户
            limit: 获取数据条数限制
            symbol: 交易对符号（可选）
            
        Returns:
            List[Dict]: 成交数据列表
        """
        try:
            # 转换用户名为完整的user_id
            user_id = self._get_full_user_id(exchange, user)
            if not user_id:
                self.logger.error(f"无法获取用户ID: exchange={exchange}, user={user}")
                return []
                
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
    
    def get_positions(self, exchange: str, user: str = None, symbol: str = None) -> List[Dict]:
        """
        获取持仓
        
        Args:
            exchange: 交易所名称
            user: 用户名（如"default"、"user1"等），如果为None或空字符串，则使用默认账户
            symbol: 交易对符号（可选）
            
        Returns:
            List[Dict]: 持仓数据列表
        """
        try:
            # 转换用户名为完整的user_id
            user_id = self._get_full_user_id(exchange, user)
            if not user_id:
                self.logger.error(f"无法获取用户ID: exchange={exchange}, user={user}")
                return []
                
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