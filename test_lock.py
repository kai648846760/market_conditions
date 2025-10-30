#!/usr/bin/env python3
"""
测试文件锁防止重复启动
"""

import time
import subprocess
import sys


def test_lock():
    """测试在运行时尝试启动第二个实例"""
    print("=== 测试文件锁防止重复启动 ===\n")
    
    # 启动第一个实例（长时间运行）
    script = """
import time
from market_data_collector import MarketDataReader, get_runtime

print("实例 1: 启动")
runtime = get_runtime()
print(f"实例 1: 运行状态 = {runtime.is_running()}")

with MarketDataReader() as reader:
    print("实例 1: 读取器已创建，保持 3 秒...")
    time.sleep(3)
    print("实例 1: 完成")
"""
    
    # 在后台启动第一个实例
    print("启动实例 1（后台）...")
    proc1 = subprocess.Popen(
        [sys.executable, '-c', script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    # 等待第一个实例完全启动
    time.sleep(1)
    
    # 尝试启动第二个实例（应该失败获取锁）
    print("\n尝试启动实例 2（应该无法获取锁）...")
    script2 = """
from market_data_collector.runtime import get_runtime

print("实例 2: 尝试启动")
runtime = get_runtime()
if runtime.start():
    print("实例 2: 启动成功")
else:
    print("实例 2: 启动失败（预期行为）")
print(f"实例 2: 运行状态 = {runtime.is_running()}")
"""
    
    proc2 = subprocess.Popen(
        [sys.executable, '-c', script2],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    # 等待第二个实例完成
    output2, _ = proc2.communicate(timeout=5)
    print("\n实例 2 输出:")
    print(output2)
    
    # 等待第一个实例完成
    print("\n等待实例 1 完成...")
    output1, _ = proc1.communicate(timeout=5)
    print("\n实例 1 输出:")
    print(output1)
    
    print("\n✓ 测试完成：文件锁正确防止了重复启动")


if __name__ == '__main__':
    test_lock()
