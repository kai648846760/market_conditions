#!/usr/bin/env python3
"""
市场行情数据读取示例
演示导入即自动启动数据收集器的功能
包含公共API的使用示例
"""

import sys
import os
from datetime import datetime

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# 导入collector模块，数据收集器将自动启动
from src.collector import get_collector
from src.reader import DataReader

def main():
    # 获取自动启动的数据收集器
    collector = get_collector()
    
    # 初始化数据读取器
    reader = DataReader()
    
    # 设置参数
    exchange = "bybit"
    symbol = "XRP/USDT:USDT"
    
    print(f"=== {exchange} {symbol} 市场数据 ===\n")
    print(f"数据收集器状态: {'运行中' if collector.is_running else '已停止'}")
    print()
    
    # 等待数据收集器收集数据
    print("等待30秒钟，让数据收集器收集数据...")
    import time
    time.sleep(30)
    print("数据收集完成，开始读取数据...")
    print()
    
    # ===== 公共API数据示例 =====
    print("===== 公共API数据示例 =====\n")
    
    # 1. 获取K线数据
    print("1. 获取K线数据(OHLCV):")
    ohlcv_data = reader.get_ohlcv(exchange, symbol, timeframe='1m', limit=10)
    if ohlcv_data:
        print(f"获取到 {len(ohlcv_data)} 条K线数据（从数据库）")
        print("最新5条K线数据:")
        for i, candle in enumerate(ohlcv_data[-5:]):
            timestamp = datetime.fromtimestamp(candle[0] / 1000)
            print(f"  {i+1}. 时间: {timestamp}, 开盘: {candle[1]}, 最高: {candle[2]}, 最低: {candle[3]}, 收盘: {candle[4]}, 成交量: {candle[5]}")
    else:
        print("未获取到K线数据")
    print()

    # 1. 获取K线数据【验证兜底】
    print("1. 获取K线数据(OHLCV):")
    ohlcv_data = reader.get_ohlcv(exchange, symbol, timeframe='1h', limit=3)
    if ohlcv_data:
        print(f"获取到 {len(ohlcv_data)} 条K线数据（从交易所）")
        print("最新3条K线数据:")
        for i, candle in enumerate(ohlcv_data[-5:]):
            timestamp = datetime.fromtimestamp(candle[0] / 1000)
            print(f"  {i+1}. 时间: {timestamp}, 开盘: {candle[1]}, 最高: {candle[2]}, 最低: {candle[3]}, 收盘: {candle[4]}, 成交量: {candle[5]}")
    else:
        print("未获取到K线数据")
    print()

    for _ in range(30):
        time.sleep(1)
        # 2. 获取ticker数据
        print("2. 获取ticker数据:")
        ticker_data = reader.get_ticker(exchange, symbol)
        if ticker_data:
            print(f"最新价格: {ticker_data.get('close', 'N/A')}")
            print(f"24h变化: {ticker_data.get('change', 'N/A')}")
            print(f"24h成交量: {ticker_data.get('quoteVolume', 'N/A')}")
            print(f"数据来源: 数据库")
        else:
            print("未获取到ticker数据")
        print()
    
    for _ in range(20):
        time.sleep(1)
        # 3. 获取orderbook数据
        print("3. 获取orderbook数据:")
        orderbook_data = reader.get_order_book(exchange, symbol)
        if orderbook_data:
            bids = orderbook_data.get('bids', [])
            asks = orderbook_data.get('asks', [])
            if bids and asks:
                print(f"最佳买价: {bids[0][0]} (数量: {bids[0][1]})")
                print(f"最佳卖价: {asks[0][0]} (数量: {asks[0][1]})")
            print(f"价差: {asks[0][0] - bids[0][0]}")
            print(f"数据来源: 数据库")
        else:
            print("未获取到orderbook数据")
        print()

        
    for _ in range(20):
        time.sleep(1)
        # 4. 获取trades数据
        print("4. 获取trades数据:")
        trades_data = reader.get_trades(exchange, symbol, limit=10)
        if trades_data:
            print(f"最新10笔交易（从数据库）:")
            for i, trade in enumerate(trades_data[:10]):
                timestamp = datetime.fromtimestamp(trade['timestamp'] / 1000)
                price = trade['price']
                amount = trade['amount']
                side = trade['side']
                print(f"  {i+1}. 时间: {timestamp}, 价格: {price}, 数量: {amount}, 方向: {side}")
        else:
            print("未获取到trades数据")
        print()

if __name__ == "__main__":
    main()