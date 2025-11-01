"""
工具函数模块
提供共享的辅助功能
"""

import os
import yaml
import time
from typing import Dict, Any, Optional, List
from pathlib import Path

from .logger import Logger


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        Dict[str, Any]: 配置字典
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        Logger.error(f"加载配置文件失败: {str(e)}")
        return {}


def get_enabled_exchanges(config: Dict[str, Any]) -> List[str]:
    """
    获取启用的交易所列表
    
    Args:
        config: 配置字典
        
    Returns:
        List[str]: 启用的交易所名称列表
    """
    exchanges_config = config.get('exchanges', {})
    return [name for name, exchange_config in exchanges_config.items() 
            if exchange_config.get('enabled', False)]


def get_exchange_symbols(config: Dict[str, Any], exchange_name: str) -> List[str]:
    """
    获取指定交易所的订阅币对列表
    
    Args:
        config: 配置字典
        exchange_name: 交易所名称
        
    Returns:
        List[str]: 币对列表
    """
    exchanges_config = config.get('exchanges', {})
    exchange_config = exchanges_config.get(exchange_name, {})
    return exchange_config.get('symbols', [])


def get_data_intervals(config: Dict[str, Any]) -> Dict[str, float]:
    """
    获取数据收集间隔配置
    
    Args:
        config: 配置字典
        
    Returns:
        Dict[str, float]: 数据类型与间隔的映射
    """
    data_collection_config = config.get('data_collection', {})
    return data_collection_config.get('intervals', {})


def get_account_config(config: Dict[str, Any], exchange_name: str, user_id: str = 'default') -> Dict[str, str]:
    """
    获取指定交易所和用户的账号配置
    
    Args:
        config: 配置字典
        exchange_name: 交易所名称
        user_id: 用户ID
        
    Returns:
        Dict[str, str]: 账号配置，包含api_key和secret
    """
    exchanges_config = config.get('exchanges', {})
    exchange_config = exchanges_config.get(exchange_name, {})
    accounts_config = exchange_config.get('accounts', {})
    return accounts_config.get(user_id, {})


def format_symbol(symbol: str) -> str:
    """
    格式化交易对符号，用于文件名
    
    Args:
        symbol: 原始符号，如 "BTC/USDT" 或 "BTC/USDT:USDT"
        
    Returns:
        str: 格式化后的符号，如 "BTC_USDT" 或 "BTC_USDT_USDT"
    """
    return symbol.replace('/', '_')


def is_futures_symbol(symbol: str) -> bool:
    """
    判断是否为合约符号
    
    Args:
        symbol: 交易对符号
        
    Returns:
        bool: 是否为合约符号
    """
    return ':' in symbol


def get_symbol_parts(symbol: str) -> tuple:
    """
    获取交易对符号的组成部分
    
    Args:
        symbol: 交易对符号，如 "BTC/USDT" 或 "BTC/USDT:USDT"
        
    Returns:
        tuple: (base, quote, settlement) settlement为None表示现货
    """
    if ':' in symbol:  # 合约格式
        parts = symbol.split(':')
        base_quote = parts[0].split('/')
        settlement = parts[1]
        return base_quote[0], base_quote[1], settlement
    else:  # 现货格式
        parts = symbol.split('/')
        return parts[0], parts[1], None


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 重试间隔（秒）
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        Logger.warning(f"函数 {func.__name__} 执行失败，{delay}秒后重试 ({attempt + 1}/{max_retries}): {str(e)}")
                        time.sleep(delay)
                    else:
                        Logger.error(f"函数 {func.__name__} 执行失败，已达到最大重试次数: {str(e)}")
            
            raise last_exception
        return wrapper
    return decorator


def safe_get_nested_value(data: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    """
    安全获取嵌套字典中的值
    
    Args:
        data: 字典数据
        keys: 键路径列表
        default: 默认值
        
    Returns:
        Any: 获取到的值或默认值
    """
    try:
        current = data
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current
    except Exception:
        return default


def ensure_dir_exists(dir_path: str) -> Path:
    """
    确保目录存在，不存在则创建
    
    Args:
        dir_path: 目录路径
        
    Returns:
        Path: 目录路径对象
    """
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_current_timestamp_ms() -> int:
    """
    获取当前时间戳（毫秒）
    
    Returns:
        int: 当前时间戳（毫秒）
    """
    return int(time.time() * 1000)


def timestamp_to_datetime(timestamp_ms: int) -> str:
    """
    将时间戳转换为可读的日期时间字符串
    
    Args:
        timestamp_ms: 时间戳（毫秒）
        
    Returns:
        str: 日期时间字符串
    """
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp_ms / 1000))


def validate_symbol(symbol: str) -> bool:
    """
    验证交易对符号格式
    
    Args:
        symbol: 交易对符号
        
    Returns:
        bool: 是否有效
    """
    if not symbol:
        return False
    
    # 现货格式: BASE/QUOTE
    if '/' in symbol and ':' not in symbol:
        parts = symbol.split('/')
        return len(parts) == 2 and all(part.strip() for part in parts)
    
    # 合约格式: BASE/QUOTE:SETTLEMENT
    elif ':' in symbol:
        parts = symbol.split(':')
        if len(parts) != 2:
            return False
        
        base_quote = parts[0].split('/')
        settlement = parts[1]
        
        return (len(base_quote) == 2 and 
                all(part.strip() for part in base_quote) and 
                settlement.strip())
    
    return False


def get_timeframe_seconds(timeframe: str) -> int:
    """
    获取时间框架对应的秒数
    
    Args:
        timeframe: 时间框架，如 "1m", "5m", "1h", "1d"
        
    Returns:
        int: 秒数
    """
    if not timeframe:
        return 60  # 默认1分钟
    
    # 提取数字和单位
    import re
    match = re.match(r'^(\d+)([smhd])$', timeframe.lower())
    if not match:
        return 60  # 默认1分钟
    
    value = int(match.group(1))
    unit = match.group(2)
    
    # 单位转换
    multipliers = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400
    }
    
    return value * multipliers.get(unit, 60)


def get_ohlcv_timeframes(config: Dict[str, Any]) -> List[str]:
    """
    获取K线时间周期配置
    
    Args:
        config: 配置字典
        
    Returns:
        List[str]: K线时间周期列表，如 ["1m", "5m", "15m"]
    """
    data_collection_config = config.get('data_collection', {})
    return data_collection_config.get('timeframes', ["1h", "4h"])  # 默认1小时和4小时