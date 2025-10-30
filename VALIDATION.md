# Market Data Collector - Runtime Singleton Validation

## 实现概述

本项目实现了市场数据采集器的运行时单例控制和自动启动功能。

### 核心文件

1. **market_data_collector/runtime.py**
   - 进程级别的单例运行时控制器
   - 使用文件锁 `.mdc.lock`（位于 `./data/` 目录）防止重复启动
   - 提供 `start()`, `stop()`, `is_running()` API
   - 读取器引用计数：`register_reader()`, `unregister_reader()`
   - 信号处理：SIGINT/SIGTERM 和 atexit 优雅关闭
   - 异步事件循环支持，线程安全设计

2. **market_data_collector/__init__.py**
   - 导入时自动初始化配置和日志
   - 如果 `config.runtime.auto_start` 为 true，自动启动运行时（幂等）
   - MarketDataReader 集成运行时引用计数
   - 上下文管理器支持自动注册/注销

### 关键特性

- **单例控制**：进程内唯一实例，文件锁防止跨进程重复
- **自动启动**：首次导入时根据配置自动启动，幂等操作
- **引用计数**：跟踪活跃读取器数量，无读取器时自动停止
- **优雅关闭**：捕获信号和 atexit 事件，确保资源清理
- **线程安全**：使用锁保护共享状态
- **中文注释**：代码注释使用中文

## 验收标准验证

### 1. 重复导入只启动一次

**测试命令**：
```bash
python test_runtime.py
```

**验证点**：
- 多次 `import market_data_collector` 只会看到一次 "运行时启动中" 日志
- 运行时状态保持为 True
- 日志显示 "运行时已在运行，跳过启动"

**结果**：✓ 通过

### 2. 多个 MarketDataReader 实例增加引用计数

**测试命令**：
```bash
python test_runtime.py
```

**验证点**：
- 创建第一个读取器：计数变为 1
- 创建第二个读取器：计数变为 2
- 第二个读取器退出：计数变为 1
- 第一个读取器退出：计数变为 0

**结果**：✓ 通过

### 3. 所有读取器退出后自动停止

**测试命令**：
```bash
python test_runtime.py
```

**验证点**：
- 所有读取器退出后，日志显示 "所有读取器已注销，准备停止运行时"
- 0.5 秒延迟后执行自动停止
- 进程退出前释放所有资源

**结果**：✓ 通过

### 4. KeyboardInterrupt/信号触发优雅关闭

**测试命令**：
```bash
python test_signal.py
```

**验证点**：
- 发送 SIGINT 信号
- 日志显示 "收到信号 SIGINT，准备优雅关闭"
- 日志显示 "运行时停止中..."
- 日志显示 "停止市场数据采集器"
- 日志显示 "释放文件锁"
- 日志显示 "运行时已停止"

**结果**：✓ 通过

### 5. 锁文件清理

**测试命令**：
```bash
python example.py
ls -la data/.mdc.lock
```

**验证点**：
- 运行时启动时创建 `data/.mdc.lock`
- 运行时停止时删除锁文件
- 进程退出后锁文件不存在

**结果**：✓ 通过

### 6. example.py 正常运行

**测试命令**：
```bash
python example.py
```

**验证点**：
- 无错误退出
- 显示 "✓ Example completed successfully!"
- 日志显示完整的启动-运行-停止流程

**结果**：✓ 通过

## 额外验证

### 文件锁防止重复启动

**测试命令**：
```bash
python test_lock.py
```

**验证点**：
- 第一个实例成功获取锁
- 第二个实例无法获取锁，日志显示警告
- 第一个实例退出后释放锁

**结果**：✓ 通过

## 运行所有测试

```bash
# 基础功能测试
python example.py

# 运行时控制测试
python test_runtime.py

# 信号处理测试
python test_signal.py

# 文件锁测试
python test_lock.py
```

## 架构说明

### 线程安全设计

- `threading.Lock` 保护读取器计数
- `threading.Lock` 保护停止操作
- `asyncio.Lock` 保护异步启动操作

### 事件循环管理

- 优先使用现有事件循环
- 如果不存在，创建新线程运行事件循环
- 守护线程确保进程正常退出

### 资源清理

1. 读取器退出 → 引用计数减少
2. 计数归零 → 调度自动停止（0.5秒延迟）
3. 自动停止 → 停止采集器、释放锁、停止事件循环
4. atexit 兜底 → 确保进程退出时清理

## 总结

所有验收标准均已满足：

✓ 重复导入只启动一次  
✓ 多读取器引用计数正确  
✓ 所有读取器退出后自动停止  
✓ 信号触发优雅关闭  
✓ 锁文件正确清理  
✓ example.py 正常运行  

实现遵循以下原则：
- 中文注释，代码清晰易读
- 最小抽象，直接明了
- 线程安全，资源正确清理
- 幂等操作，多次调用安全
