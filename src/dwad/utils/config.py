import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger


class ConfigManager:
    """配置管理器"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器

        Args:
            config_path: 配置文件路径，如果为None则使用默认路径
        """
        if config_path is None:
            # 获取项目根目录
            current_dir = Path(__file__).parent
            project_root = current_dir.parent.parent.parent
            config_path = project_root / "config" / "config.yaml"

        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """加载配置文件"""
        try:
            if not self.config_path.exists():
                logger.warning(f"配置文件不存在: {self.config_path}")
                self._config = {}
                return

            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f) or {}

            logger.info(f"成功加载配置文件: {self.config_path}")

        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            self._config = {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值，支持点号分隔的嵌套键

        Args:
            key: 配置键，如 'goldminer.token' 或 'data_storage.base_path'
            default: 默认值

        Returns:
            配置值或默认值
        """
        keys = key.split('.')
        value = self._config

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key: str, value: Any) -> None:
        """
        设置配置值

        Args:
            key: 配置键
            value: 配置值
        """
        keys = key.split('.')
        config = self._config

        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value

    def save(self) -> None:
        """保存配置到文件"""
        try:
            # 确保目录存在
            self.config_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self._config, f, default_flow_style=False,
                         allow_unicode=True, indent=2)

            logger.info(f"配置已保存到: {self.config_path}")

        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")

    def get_goldminer_token(self) -> str:
        """获取掘金API token"""
        token = self.get('goldminer.token', '')
        if not token:
            logger.warning("掘金API token未配置")
        return token

    def get_goldminer_serv_addr(self) -> str:
        """获取掘金终端服务地址（Linux环境需要指向Windows终端）"""
        return self.get('goldminer.serv_addr', '')

    def get_data_paths(self) -> Dict[str, str]:
        """获取数据存储路径配置"""
        return {
            'base_path': self.get('data_storage.base_path', './data'),
            'stocks_path': self.get('data_storage.stocks_path', './data/stocks'),
            'indices_path': self.get('data_storage.indices_path', './data/indices'),
            'metadata_path': self.get('data_storage.metadata_path', './data/metadata')
        }

    def get_market_data_fields(self) -> list:
        """获取行情数据字段配置"""
        return self.get('data_fetcher.market_data_fields',
                       ['open', 'high', 'low', 'close', 'volume', 'turnover'])

    def get_rate_limit_config(self) -> Dict[str, Any]:
        """获取API调用频率限制配置"""
        return {
            'requests_per_second': self.get('data_fetcher.rate_limit.requests_per_second', 10),
            'requests_per_minute': self.get('data_fetcher.rate_limit.requests_per_minute', 500),
            'retry_times': self.get('data_fetcher.rate_limit.retry_times', 3),
            'retry_delay': self.get('data_fetcher.rate_limit.retry_delay', 1)
        }


# 全局配置实例
config = ConfigManager()