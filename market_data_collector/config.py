import os
import yaml
from pathlib import Path
from typing import Any, Dict


class Config:
    """Immutable configuration object"""
    
    def __init__(self, data: Dict[str, Any]):
        self._data = data
    
    def __getattr__(self, key: str) -> Any:
        if key.startswith('_'):
            return super().__getattribute__(key)
        return self._data.get(key)
    
    def __getitem__(self, key: str) -> Any:
        return self._data[key]
    
    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        return self._data.copy()


def _load_yaml_config(config_path: Path) -> Dict[str, Any]:
    """Load YAML configuration file"""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f) or {}


def _apply_env_overrides(config: Dict[str, Any]) -> None:
    """Apply environment variable overrides to config"""
    if exchange := os.getenv('MDC_EXCHANGE'):
        config['exchange'] = exchange
    
    if symbols := os.getenv('MDC_SYMBOLS'):
        config['symbols'] = [s.strip() for s in symbols.split(',')]
    
    if log_level := os.getenv('MDC_LOG_LEVEL'):
        if 'logging' not in config:
            config['logging'] = {}
        config['logging']['level'] = log_level.upper()
    
    if db_root := os.getenv('MDC_DB_ROOT'):
        if 'storage' not in config:
            config['storage'] = {}
        config['storage']['db_root'] = db_root
    
    if log_file := os.getenv('MDC_LOG_FILE'):
        if 'logging' not in config:
            config['logging'] = {}
        config['logging']['file'] = log_file


def _validate_config(config: Dict[str, Any]) -> None:
    """Basic validation of configuration structure"""
    required_keys = ['exchange', 'type', 'symbols', 'intervals', 'storage', 'logging']
    
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")
    
    if not isinstance(config['symbols'], list) or not config['symbols']:
        raise ValueError("Config 'symbols' must be a non-empty list")
    
    if not isinstance(config['intervals'], dict):
        raise ValueError("Config 'intervals' must be a dictionary")
    
    if not isinstance(config['storage'], dict):
        raise ValueError("Config 'storage' must be a dictionary")
    
    if not isinstance(config['logging'], dict):
        raise ValueError("Config 'logging' must be a dictionary")


def get_config(config_path: str = None) -> Config:
    """
    Load and return immutable configuration object.
    
    Args:
        config_path: Path to YAML config file. Defaults to configs/market.yaml
    
    Returns:
        Config: Immutable configuration object
    """
    if config_path is None:
        project_root = Path(__file__).parent.parent
        config_path = project_root / 'configs' / 'market.yaml'
    else:
        config_path = Path(config_path)
    
    config_dict = _load_yaml_config(config_path)
    _apply_env_overrides(config_dict)
    _validate_config(config_dict)
    
    return Config(config_dict)
