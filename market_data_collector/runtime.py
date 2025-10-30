import asyncio
import atexit
import fcntl
import signal
import threading
from pathlib import Path
from typing import Optional

from .utils.logging import get_logger


class RuntimeController:
    """
    运行时单例控制器
    - 进程级别的单实例控制，使用文件锁防止重复启动
    - 提供启动/停止/状态查询 API
    - 读取器引用计数：决定何时自动停止
    - 信号处理和 atexit 清理
    """
    
    _instance: Optional['RuntimeController'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """单例模式：确保进程内只有一个实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初始化运行时控制器"""
        # 避免重复初始化
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self.logger = get_logger('runtime')
        
        # 文件锁路径：./data/.mdc.lock
        self._lock_dir = Path(__file__).parent.parent / 'data'
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        self._lock_file_path = self._lock_dir / '.mdc.lock'
        self._lock_file: Optional[object] = None
        
        # 运行状态
        self._running = False
        self._start_lock = asyncio.Lock()
        self._stop_lock = threading.Lock()
        self._reader_count = 0
        self._reader_lock = threading.Lock()
        self._auto_stop_scheduled = False
        
        # 异步相关
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._background_task: Optional[asyncio.Task] = None
        
        # 注册信号处理和退出清理
        self._register_signal_handlers()
        atexit.register(self._cleanup_on_exit)
        
        self.logger.info("运行时控制器初始化完成")
    
    def _register_signal_handlers(self):
        """注册信号处理器：SIGINT 和 SIGTERM"""
        def signal_handler(signum, frame):
            sig_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
            self.logger.info(f"收到信号 {sig_name}，准备优雅关闭")
            self.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _cleanup_on_exit(self):
        """退出时清理资源"""
        if self._running:
            self.logger.info("进程退出，执行清理")
            self.stop()
    
    def _acquire_lock(self) -> bool:
        """
        获取文件锁
        Returns:
            bool: 成功获取锁返回 True，否则返回 False
        """
        try:
            self._lock_file = open(self._lock_file_path, 'w')
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._lock_file.write(str(threading.get_ident()))
            self._lock_file.flush()
            self.logger.info(f"成功获取文件锁: {self._lock_file_path}")
            return True
        except (IOError, OSError) as e:
            self.logger.warning(f"无法获取文件锁: {e}")
            if self._lock_file:
                self._lock_file.close()
                self._lock_file = None
            return False
    
    def _release_lock(self):
        """释放文件锁"""
        if self._lock_file:
            try:
                # 检查文件是否还打开
                if not self._lock_file.closed:
                    fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                    self._lock_file.close()
                self._lock_file = None
                # 删除锁文件
                if self._lock_file_path.exists():
                    self._lock_file_path.unlink()
                self.logger.info(f"释放文件锁: {self._lock_file_path}")
            except Exception as e:
                self.logger.error(f"释放文件锁失败: {e}")
    
    def _ensure_event_loop(self):
        """
        确保事件循环存在
        如果当前线程没有事件循环，创建新线程运行事件循环
        """
        try:
            self._loop = asyncio.get_running_loop()
            self.logger.debug("使用现有的事件循环")
        except RuntimeError:
            # 没有运行中的事件循环，需要在新线程中创建
            if self._loop_thread is None or not self._loop_thread.is_alive():
                self.logger.info("创建新的事件循环线程")
                self._loop = asyncio.new_event_loop()
                
                def run_loop():
                    asyncio.set_event_loop(self._loop)
                    self._loop.run_forever()
                
                self._loop_thread = threading.Thread(target=run_loop, daemon=True)
                self._loop_thread.start()
    
    async def _start_collectors_async(self):
        """
        异步启动数据采集器
        这是实际启动采集器的地方（当前为占位符）
        """
        self.logger.info("启动市场数据采集器（占位符）")
        # TODO: 在这里启动实际的 WebSocket 采集器
        await asyncio.sleep(0.1)  # 模拟启动过程
        self.logger.info("市场数据采集器启动完成")
    
    async def _stop_collectors_async(self):
        """
        异步停止数据采集器
        这是实际停止采集器的地方（当前为占位符）
        """
        self.logger.info("停止市场数据采集器（占位符）")
        # TODO: 在这里停止实际的 WebSocket 采集器
        await asyncio.sleep(0.1)  # 模拟停止过程
        self.logger.info("市场数据采集器停止完成")
    
    def start(self) -> bool:
        """
        启动运行时
        幂等操作：多次调用只会启动一次
        
        Returns:
            bool: 成功启动返回 True，已经在运行或启动失败返回 False
        """
        if self._running:
            self.logger.debug("运行时已在运行，跳过启动")
            return False
        
        # 获取文件锁
        if not self._acquire_lock():
            self.logger.error("无法获取文件锁，可能有其他实例正在运行")
            return False
        
        self._running = True
        self.logger.info("运行时启动中...")
        
        # 确保有事件循环
        self._ensure_event_loop()
        
        # 在事件循环中启动采集器
        if self._loop:
            future = asyncio.run_coroutine_threadsafe(
                self._start_with_lock(),
                self._loop
            )
            try:
                future.result(timeout=5.0)
            except Exception as e:
                self.logger.error(f"启动采集器失败: {e}")
                self._running = False
                self._release_lock()
                return False
        
        return True
    
    async def _start_with_lock(self):
        """使用锁保护的异步启动"""
        async with self._start_lock:
            if self._background_task is None or self._background_task.done():
                self._background_task = asyncio.create_task(self._start_collectors_async())
                await self._background_task
    
    def stop(self) -> bool:
        """
        停止运行时
        
        Returns:
            bool: 成功停止返回 True，未在运行返回 False
        """
        # 使用锁防止并发停止
        with self._stop_lock:
            if not self._running:
                self.logger.debug("运行时未运行，跳过停止")
                return False
            
            self.logger.info("运行时停止中...")
            
            # 在事件循环中停止采集器
            if self._loop and self._loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._stop_collectors_async(),
                    self._loop
                )
                try:
                    future.result(timeout=5.0)
                except Exception as e:
                    self.logger.error(f"停止采集器失败: {e}")
            
            self._running = False
            
            # 释放文件锁
            self._release_lock()
            
            # 停止事件循环
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            
            self.logger.info("运行时已停止")
            return True
    
    def is_running(self) -> bool:
        """
        检查运行时是否在运行
        
        Returns:
            bool: 运行中返回 True，否则返回 False
        """
        return self._running
    
    def register_reader(self):
        """
        注册读取器
        增加引用计数
        """
        with self._reader_lock:
            self._reader_count += 1
            self.logger.info(f"注册读取器，当前计数: {self._reader_count}")
    
    def unregister_reader(self):
        """
        注销读取器
        减少引用计数，如果计数归零则自动停止运行时
        """
        with self._reader_lock:
            self._reader_count -= 1
            self.logger.info(f"注销读取器，当前计数: {self._reader_count}")
            
            if self._reader_count <= 0:
                self._reader_count = 0
                self.logger.info("所有读取器已注销，准备停止运行时")
                # 延迟一小段时间后停止，避免频繁启停
                # 使用标志位避免多次调度
                if not self._auto_stop_scheduled and self._loop and self._loop.is_running():
                    self._auto_stop_scheduled = True
                    self._loop.call_later(0.5, self._auto_stop_if_no_readers)
    
    def _auto_stop_if_no_readers(self):
        """如果没有读取器则自动停止"""
        with self._reader_lock:
            self._auto_stop_scheduled = False
            if self._reader_count == 0 and self._running:
                self.logger.info("无活跃读取器，自动停止运行时")
                # 使用线程调用 stop，避免在事件循环中直接调用
                import threading
                threading.Thread(target=self.stop, daemon=True).start()
    
    def get_reader_count(self) -> int:
        """
        获取当前读取器数量
        
        Returns:
            int: 当前读取器数量
        """
        return self._reader_count


# 全局运行时实例
_runtime: Optional[RuntimeController] = None
_runtime_init_lock = threading.Lock()


def get_runtime() -> RuntimeController:
    """
    获取全局运行时实例（单例）
    
    Returns:
        RuntimeController: 运行时控制器实例
    """
    global _runtime
    if _runtime is None:
        with _runtime_init_lock:
            if _runtime is None:
                _runtime = RuntimeController()
    return _runtime
