from .config import get_config, Config
from .utils.logging import setup_logging, get_logger
from .runtime import get_runtime


_config = None
_logger = None
_runtime_started = False


def _initialize():
    """
    初始化配置和日志
    首次导入时执行：加载配置、设置日志
    如果配置了 auto_start，则启动运行时（幂等）
    """
    global _config, _logger, _runtime_started
    
    if _config is None:
        _config = get_config()
    
    if _logger is None:
        logging_config = _config.logging
        _logger = setup_logging(
            log_file=logging_config.get('file', './logs/market_data.log'),
            level=logging_config.get('level', 'INFO'),
            max_bytes=logging_config.get('max_bytes', 10485760),
            backup_count=logging_config.get('backup_count', 5)
        )
        _logger.info("Market Data Collector 初始化完成")
    
    # 检查是否需要自动启动
    if not _runtime_started:
        runtime_config = _config.runtime
        if runtime_config and runtime_config.get('auto_start', False):
            _logger.info("配置启用 auto_start，准备启动运行时")
            runtime = get_runtime()
            if runtime.start():
                _logger.info("运行时自动启动成功")
            else:
                _logger.info("运行时已在运行或启动失败")
            _runtime_started = True


class MarketDataReader:
    """
    市场数据读取器
    - 与运行时控制器集成，支持引用计数
    - 上下文管理器支持，进入时注册，退出时注销
    - 当所有读取器退出时，运行时将自动停止
    """
    
    def __init__(self, config: Config = None):
        self.config = config or _config
        self.logger = get_logger('reader')
        self._active = False
        self._runtime = get_runtime()
    
    def __enter__(self):
        """进入上下文：注册读取器"""
        self.logger.info("MarketDataReader 上下文进入")
        self._active = True
        self._runtime.register_reader()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文：注销读取器"""
        self.logger.info("MarketDataReader 上下文退出")
        self._active = False
        self._runtime.unregister_reader()
        return False
    
    def start(self):
        """占位符：启动数据采集"""
        self.logger.info("MarketDataReader.start() 调用（占位符）")
    
    def stop(self):
        """占位符：停止数据采集"""
        self.logger.info("MarketDataReader.stop() 调用（占位符）")


_initialize()

__all__ = ['MarketDataReader', 'get_config', 'Config', 'get_logger', 'get_runtime']
