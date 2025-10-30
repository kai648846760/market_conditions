#!/usr/bin/env python3
"""
测试信号处理
"""

import time
import signal
import os
from market_data_collector import MarketDataReader, get_runtime


def main():
    print("测试信号处理...")
    runtime = get_runtime()
    
    print(f"运行时状态: {runtime.is_running()}")
    
    # 创建一个读取器
    with MarketDataReader() as reader:
        print(f"读取器已创建，当前计数: {runtime.get_reader_count()}")
        
        # 模拟一些工作
        print("模拟工作 2 秒...")
        time.sleep(2)
        
        # 发送 SIGINT 给自己
        print("发送 SIGINT 信号...")
        os.kill(os.getpid(), signal.SIGINT)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n捕获到 KeyboardInterrupt")
        time.sleep(0.5)
        print("程序即将退出...")
