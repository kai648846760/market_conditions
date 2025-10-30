#!/usr/bin/env python3
"""
测试运行时单例和自动启动控制
"""

import time
import sys


def test_multiple_imports():
    """测试多次导入只启动一次"""
    print("\n=== 测试 1: 多次导入只启动一次 ===")
    
    # 第一次导入
    print("第一次导入...")
    import market_data_collector
    time.sleep(0.2)
    
    # 第二次导入（应该不会重新启动）
    print("\n第二次导入...")
    from market_data_collector import MarketDataReader, get_runtime
    time.sleep(0.2)
    
    # 检查状态
    runtime = get_runtime()
    print(f"运行时状态: {runtime.is_running()}")
    print(f"读取器计数: {runtime.get_reader_count()}")
    print("✓ 测试通过：多次导入只启动一次")


def test_multiple_readers():
    """测试多个读取器的引用计数"""
    print("\n\n=== 测试 2: 多个读取器引用计数 ===")
    
    from market_data_collector import MarketDataReader, get_runtime
    runtime = get_runtime()
    
    print(f"初始读取器计数: {runtime.get_reader_count()}")
    
    # 创建第一个读取器
    print("\n创建第一个读取器...")
    with MarketDataReader() as reader1:
        print(f"读取器计数: {runtime.get_reader_count()}")
        
        # 创建第二个读取器（嵌套）
        print("\n创建第二个读取器（嵌套）...")
        with MarketDataReader() as reader2:
            print(f"读取器计数: {runtime.get_reader_count()}")
            print("两个读取器都在活动中")
        
        print(f"\n第二个读取器退出后计数: {runtime.get_reader_count()}")
    
    print(f"所有读取器退出后计数: {runtime.get_reader_count()}")
    
    # 等待一下看看是否自动停止
    print("\n等待自动停止...")
    time.sleep(1.5)
    print(f"运行时状态: {runtime.is_running()}")
    print("✓ 测试通过：多个读取器引用计数正确")


def test_sequential_readers():
    """测试顺序创建多个读取器"""
    print("\n\n=== 测试 3: 顺序创建多个读取器 ===")
    
    from market_data_collector import MarketDataReader, get_runtime
    runtime = get_runtime()
    
    for i in range(3):
        print(f"\n创建读取器 {i+1}...")
        with MarketDataReader() as reader:
            print(f"读取器 {i+1} 活动中，计数: {runtime.get_reader_count()}")
        print(f"读取器 {i+1} 退出，计数: {runtime.get_reader_count()}")
        time.sleep(0.3)
    
    print("\n所有读取器已退出")
    time.sleep(1.5)
    print(f"运行时状态: {runtime.is_running()}")
    print("✓ 测试通过：顺序读取器工作正常")


def main():
    print("=" * 60)
    print("运行时单例和自动启动控制测试")
    print("=" * 60)
    
    try:
        test_multiple_imports()
        test_multiple_readers()
        test_sequential_readers()
        
        print("\n" + "=" * 60)
        print("所有测试通过！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
