# 市场行情数据收集系统

一个基于ccxt库的实时行情数据收集和读取工具，支持多交易所、多币对的行情数据收集、存储和查询。

## 功能特性

- **多交易所支持**: 支持Bybit、Binance等主流交易所
- **多数据类型**: 收集ticker、orderbook、trades、ohlcv等实时行情数据
- **数据持久化**: 使用SQLite数据库存储历史数据
- **高效查询**: 提供灵活的数据查询接口
- **异步处理**: 基于asyncio的高性能异步数据收集
- **配置灵活**: 通过YAML配置文件轻松管理交易所和币对
- **日志记录**: 完整的日志记录和轮转机制
- **自动启动**: 导入即自动启动数据收集器，无需手动管理

## 项目结构

```
market_conditions/
├── src/                     # 源代码目录
│   ├── __init__.py          # 模块初始化文件
│   ├── logger.py            # 日志管理模块
│   ├── database.py          # 数据库操作模块
│   ├── utils.py             # 工具函数模块
│   ├── collector.py         # 数据收集器模块
│   ├── reader.py            # 数据读取器模块
│   └── private_reader.py    # 私有数据读取器模块(暂未启用)
├── data/                    # 数据存储目录
├── logs/                    # 日志目录
├── config.yaml              # 配置文件
├── pyproject.toml           # 项目配置文件
├── example.py               # 示例脚本
├── .gitignore               # Git忽略文件
└── README.md                # 项目说明文档
```

## 安装与配置

### 环境要求

- Python 3.9+
- uv (用于依赖管理)

### 安装步骤

1. 克隆项目到本地
2. 创建虚拟环境并激活:
   ```bash
   uv venv
   source .venv/bin/activate  # Linux/macOS
   .venv\Scripts\activate     # Windows
   ```
3. 安装依赖:
   ```bash
   uv sync
   ```

### 配置文件

编辑 `config.yaml` 文件，配置交易所和币对信息:

```yaml
exchanges:
  bybit:
    enabled: true    
    symbols:
      - "XRP/USDT:USDT"  # 合约格式
    
  binance:
    enabled: true
    symbols:
      - "BTC/USDT"       # 现货
      - "ETH/USDT"        # 现货
      - "XRP/USDT:USDT"   # 合约

data_collection:
  intervals:
    ticker: 1.0      # 行情数据间隔
    orderbook: 1.0   # 订单簿数据间隔
    trades: 1.5      # 交易数据间隔
    ohlcv: 20.0      # K线数据间隔
  timeframes:
    - "1m"   # 1分钟K线
    - "5m"   # 5分钟K线
    - "15m"  # 15分钟K线

global:
  database:
    data_dir: "./data"
    retention_days: 7
  logging:
    level: "INFO" 
    log_file: "./logs/market_conditions.log"
```

## 使用方法

### 1. 数据收集与读取

```python
from src.collector import get_collector
from src.reader import DataReader

# 获取自动启动的数据收集器
collector = get_collector()
print(f"数据收集器状态: {'运行中' if collector.is_running else '已停止'}")

# 初始化数据读取器
reader = DataReader()

# 获取K线数据
ohlcv_data = reader.get_ohlcv("bybit", "XRP/USDT:USDT", timeframe='1m', limit=10)

# 获取ticker数据
ticker_data = reader.get_ticker("bybit", "XRP/USDT:USDT")

# 获取orderbook数据
orderbook_data = reader.get_order_book("bybit", "XRP/USDT:USDT")

# 获取trades数据
trades_data = reader.get_trades("bybit", "XRP/USDT:USDT", limit=10)
```

### 2. 运行示例脚本

```bash
uv run example.py
```

## API参考

### Collector

数据收集器类，负责从交易所收集实时数据。

#### 方法

- `get_collector()`: 获取全局数据收集器实例
- `is_running`: 检查收集器是否正在运行

### DataReader

数据读取器类，提供数据查询功能。

#### 方法

- `get_ticker(exchange, symbol)`: 获取最新ticker数据
- `get_order_book(exchange, symbol)`: 获取最新orderbook数据
- `get_trades(exchange, symbol, limit=10)`: 获取最新trade数据
- `get_ohlcv(exchange, symbol, timeframe='1m', limit=100)`: 获取K线数据
- `get_available_symbols(exchange)`: 获取可用交易对列表

## 数据结构

### Ticker数据

```json
{
  "symbol": "XRP/USDT:USDT",
  "timestamp": 1640995200000,
  "close": 0.5,
  "change": 0.01,
  "quoteVolume": 1234567.89
}
```

### Orderbook数据

```json
{
  "symbol": "XRP/USDT:USDT",
  "timestamp": 1640995200000,
  "bids": [[0.499, 1000], [0.498, 2000]],
  "asks": [[0.501, 1000], [0.502, 2000]]
}
```

### Trade数据

```json
{
  "symbol": "XRP/USDT:USDT",
  "timestamp": 1640995200000,
  "price": 0.5,
  "amount": 1000,
  "side": "buy"
}
```

### OHLCV数据

```json
[
  [1640995200000, 0.49, 0.51, 0.48, 0.5, 10000],
  [1640995260000, 0.5, 0.52, 0.49, 0.51, 11000]
]
```

## 开发指南

### 添加新的交易所支持

1. 在 `config.yaml` 中添加新交易所配置
2. 在 `src/utils.py` 中添加交易所特定的工具函数
3. 在 `src/collector.py` 中添加新交易所的数据收集逻辑

### 添加新的数据类型

1. 在 `src/database.py` 中添加新数据类型的表创建和存储方法
2. 在 `src/collector.py` 中添加新数据类型的收集逻辑
3. 在 `src/reader.py` 中添加新数据类型的查询方法

## 测试

运行测试:

```bash
uv run pytest
```

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 更新日志

### v1.0.0
- 初始版本发布
- 支持Bybit和Binance交易所
- 实现ticker、orderbook、trades、ohlcv数据收集和查询
- 提供导入即自动启动的数据收集器
- 完善的配置文件和日志系统