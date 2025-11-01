# 市场行情数据收集与读取系统

基于ccxt pro封装的实时行情数据收集和读取工具，支持多交易所、多币对的行情数据收集、存储和查询。

## 功能特性

- **多交易所支持**: 支持Bybit、Binance等主流交易所
- **多数据类型**: 收集ticker、orderbook、trade等实时行情数据
- **私有数据支持**: 支持收集余额、订单、持仓等私有数据
- **多账号管理**: 支持配置多个交易所账号，分别收集私有数据
- **数据持久化**: 使用SQLite数据库存储历史数据
- **高效查询**: 提供灵活的数据查询和分析接口
- **异步处理**: 基于asyncio的高性能异步数据收集
- **配置灵活**: 通过YAML配置文件轻松管理交易所和币对
- **日志记录**: 完整的日志记录和轮转机制
- **数据分析**: 支持将数据转换为pandas DataFrame进行分析

## 项目结构

```
market_conditions/
├── src/                     # 源代码目录
│   ├── __init__.py          # 模块初始化文件
│   ├── logger.py            # 日志管理模块
│   ├── database.py          # 数据库操作模块
│   ├── utils.py             # 工具函数模块
│   ├── collector.py         # 数据收集器模块
│   └── reader.py            # 数据读取器模块
├── data/                    # 数据存储目录
│   └── bybit/               # Bybit交易所数据
│       ├── spot/            # 现货数据
│       ├── futures/         # 合约数据
│       └── private/         # 私有数据
├── logs/                    # 日志目录
│   └── backups/             # 日志备份目录
├── tests/                   # 测试目录
├── config.yaml              # 配置文件
├── pyproject.toml           # 项目配置文件
├── example.py               # 示例脚本
└── README.md                # 项目说明文档
```

## 安装与配置

### 环境要求

- Python 3.8+
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
      - BTC/USDT
      - ETH/USDT
      - SOL/USDT
    accounts:
      - name: "main"
        api_key: "your_api_key"
        secret: "your_secret"
        sandbox: false

  binance:
    enabled: false
    symbols:
      - BTC/USDT
      - ETH/USDT

data_collection:
  interval: 1  # 数据收集间隔(秒)

global:
  data_dir: "data"
  log_level: "INFO"
  log_rotation: "daily"
```

## 使用方法

### 1. 数据收集

```python
from src.collector import CollectorManager

# 创建收集器管理器
collector_manager = CollectorManager()

# 启动数据收集
collector_manager.start()

# 停止数据收集
collector_manager.stop()
```

### 2. 数据读取

```python
from src.reader import DataReader

# 创建数据读取器
reader = DataReader()

# 获取最新ticker数据
ticker = reader.get_latest_ticker("bybit", "BTC/USDT")

# 获取历史数据
end_time = get_current_timestamp_ms()
start_time = end_time - (60 * 60 * 1000)  # 1小时前
ticker_history = reader.get_ticker_history("bybit", "BTC/USDT", start_time, end_time)

# 获取DataFrame格式数据
ticker_df = reader.get_ticker_dataframe("bybit", "BTC/USDT", start_time, end_time)
```

### 3. 私有数据收集

```python
from src.collector import PrivateDataCollector

# 创建私有数据收集器
private_collector = PrivateDataCollector()

# 启动私有数据收集
private_collector.start()

# 停止私有数据收集
private_collector.stop()
```

### 4. 私有数据读取

```python
from src.reader import PrivateDataReader

# 创建私有数据读取器
private_reader = PrivateDataReader()

# 获取最新余额数据
balance = private_reader.get_latest_balance("bybit", "main")

# 获取历史订单数据
end_time = get_current_timestamp_ms()
start_time = end_time - (24 * 60 * 60 * 1000)  # 24小时前
orders_history = private_reader.get_orders_history("bybit", "main", start_time, end_time)

# 获取持仓数据
positions = private_reader.get_latest_positions("bybit", "main")

# 获取DataFrame格式数据
balance_df = private_reader.get_balance_dataframe("bybit", "main", start_time, end_time)
```

### 5. 运行示例脚本

```bash
uv run example.py
```

## API参考

### DataCollector

数据收集器类，负责从交易所收集实时数据。

#### 方法

- `start()`: 启动数据收集
- `stop()`: 停止数据收集
- `register_callback(data_type, callback)`: 注册数据回调函数

### DataReader

数据读取器类，提供数据查询和分析功能。

#### 方法

- `get_latest_ticker(exchange, symbol)`: 获取最新ticker数据
- `get_latest_orderbook(exchange, symbol)`: 获取最新orderbook数据
- `get_latest_trades(exchange, symbol, limit=10)`: 获取最新trade数据
- `get_ticker_history(exchange, symbol, start_time, end_time, limit=100)`: 获取历史ticker数据
- `get_ticker_dataframe(exchange, symbol, start_time, end_time, limit=100)`: 获取ticker数据的DataFrame
- `get_available_symbols(exchange)`: 获取可用交易对列表
- `get_data_summary(exchange, symbol)`: 获取数据摘要信息

### PrivateDataReader

私有数据读取器类，提供私有数据查询和分析功能。

#### 方法

- `get_latest_balance(exchange, user_id)`: 获取最新余额数据
- `get_balance_history(exchange, user_id, start_time, end_time, limit=100)`: 获取历史余额数据
- `get_latest_orders(exchange, user_id, limit=10, symbol=None)`: 获取最新订单数据
- `get_orders_history(exchange, user_id, start_time, end_time, limit=100, symbol=None)`: 获取历史订单数据
- `get_latest_positions(exchange, user_id, symbol=None)`: 获取最新持仓数据
- `get_positions_history(exchange, user_id, start_time, end_time, limit=100, symbol=None)`: 获取历史持仓数据
- `get_latest_mytrades(exchange, user_id, limit=10, symbol=None)`: 获取最新成交数据
- `get_mytrades_history(exchange, user_id, start_time, end_time, limit=100, symbol=None)`: 获取历史成交数据
- `get_balance_dataframe(exchange, user_id, start_time, end_time)`: 获取余额数据的DataFrame
- `get_orders_dataframe(exchange, user_id, start_time, end_time, symbol=None)`: 获取订单数据的DataFrame
- `get_positions_dataframe(exchange, user_id, start_time, end_time, symbol=None)`: 获取持仓数据的DataFrame
- `get_mytrades_dataframe(exchange, user_id, start_time, end_time, symbol=None)`: 获取成交数据的DataFrame
- `get_private_data_summary(exchange, user_id)`: 获取私有数据摘要

## 数据结构

### Ticker数据

```json
{
  "symbol": "BTC/USDT",
  "timestamp": 1640995200000,
  "datetime": "2022-01-01T00:00:00.000Z",
  "last": 47000.0,
  "bid": 46999.0,
  "ask": 47001.0,
  "baseVolume": 12345.67,
  "quoteVolume": 580123456.78,
  "change": 500.0,
  "percentage": 1.07,
  "average": 46500.0
}
```

### Orderbook数据

```json
{
  "symbol": "BTC/USDT",
  "timestamp": 1640995200000,
  "datetime": "2022-01-01T00:00:00.000Z",
  "bids": [
    [46999.0, 1.2345],
    [46998.0, 2.3456],
    ...
  ],
  "asks": [
    [47001.0, 1.2345],
    [47002.0, 2.3456],
    ...
  ]
}
```

### Trade数据

```json
{
  "symbol": "BTC/USDT",
  "timestamp": 1640995200000,
  "datetime": "2022-01-01T00:00:00.000Z",
  "id": "123456789",
  "price": 47000.0,
  "amount": 0.1234,
  "side": "buy",
  "cost": 5801.2,
  "takerOrMaker": "taker"
}
```

### 余额数据

```json
{
  "user_id": "main",
  "timestamp": 1640995200000,
  "datetime": "2022-01-01T00:00:00.000Z",
  "info": {...},
  "BTC": {
    "total": 1.2345,
    "free": 1.0,
    "used": 0.2345
  },
  "USDT": {
    "total": 10000.0,
    "free": 9500.0,
    "used": 500.0
  }
}
```

### 订单数据

```json
{
  "user_id": "main",
  "timestamp": 1640995200000,
  "datetime": "2022-01-01T00:00:00.000Z",
  "id": "123456789",
  "symbol": "BTC/USDT",
  "type": "limit",
  "side": "buy",
  "amount": 0.1234,
  "price": 47000.0,
  "filled": 0.0,
  "remaining": 0.1234,
  "status": "open",
  "fee": {...},
  "info": {...}
}
```

### 持仓数据

```json
{
  "user_id": "main",
  "timestamp": 1640995200000,
  "datetime": "2022-01-01T00:00:00.000Z",
  "info": {...},
  "BTC/USDT": {
    "contracts": 1.2345,
    "contractSize": 0.001,
    "side": "long",
    "size": 1.2345,
    "notional": 58012.3,
    "leverage": 10.0,
    "entryPrice": 47000.0,
    "markPrice": 47010.0,
    "unrealizedPnl": 12.34,
    "percentage": 0.02
  }
}
```

### 成交数据

```json
{
  "user_id": "main",
  "timestamp": 1640995200000,
  "datetime": "2022-01-01T00:00:00.000Z",
  "id": "123456789",
  "order": "987654321",
  "symbol": "BTC/USDT",
  "type": "limit",
  "side": "buy",
  "amount": 0.1234,
  "price": 47000.0,
  "cost": 5801.2,
  "fee": {...},
  "info": {...}
}
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

### v1.1.0
- 新增私有数据支持（余额、订单、持仓、成交）
- 实现多账号管理功能
- 分离公共数据和私有数据的连接管理
- 新增PrivateDataCollector和PrivateDataReader类
- 完善配置文件，支持多账号API配置

### v1.0.0
- 初始版本发布
- 支持Bybit和Binance交易所
- 实现ticker、orderbook、trade数据收集和查询
- 提供DataFrame接口用于数据分析