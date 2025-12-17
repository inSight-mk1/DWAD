"""
实时价格数据获取模块

该模块用于从掘金API获取股票的实时价格数据
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from loguru import logger

from ..utils.config import config
from .goldminer_fetcher import _gm_api_lock


@dataclass
class RealtimePrice:
    """实时价格数据"""
    symbol: str          # 股票代码
    price: float         # 最新价
    created_at: datetime # 创建时间


class RealtimePriceFetcher:
    """实时价格获取器"""

    def __init__(self, token: Optional[str] = None):
        """
        初始化实时价格获取器

        Args:
            token: 掘金API token，如果为None则从配置文件获取
        """
        # 重新加载配置文件，确保获取最新配置
        config.reload()
        
        self.token = token or config.get_goldminer_token()
        if not self.token:
            raise ValueError("掘金API token未配置，请在config.yaml中设置goldminer.token")

        # 初始化掘金API连接
        self._init_gm_api()

    def _init_gm_api(self):
        """初始化掘金API"""
        try:
            # 导入掘金API
            from gm.api import set_token, set_serv_addr

            # 使用全局锁保护掘金 API 初始化，避免并发冲突
            with _gm_api_lock:
                # 设置终端服务地址（Linux环境需要指向Windows终端）
                serv_addr = config.get_goldminer_serv_addr()
                if serv_addr:
                    set_serv_addr(serv_addr)
                    logger.info(f"掘金终端地址设置为: {serv_addr}（实时价格模块）")

                # 设置token
                set_token(self.token)

            logger.info("掘金API连接成功（实时价格模块）")

        except ImportError:
            raise ImportError("掘金SDK未安装，请运行: pip install gm3")
        except Exception as e:
            logger.error(f"掘金API初始化失败: {e}")
            raise

    def get_current_prices(self, symbols: List[str]) -> Dict[str, RealtimePrice]:
        """
        获取多个股票的实时价格

        Args:
            symbols: 股票代码列表，格式为['SHSE.600000', 'SZSE.000001']

        Returns:
            字典，键为股票代码，值为RealtimePrice对象
        """
        from gm.api import current_price

        try:
            if not symbols:
                logger.warning("获取实时价格：股票代码列表为空")
                return {}

            logger.info(f"正在获取 {len(symbols)} 只股票的实时价格...")

            # 使用全局锁保护掘金 API 调用，避免并发冲突
            with _gm_api_lock:
                # 调用掘金API获取实时价格
                # 支持两种格式：字符串（逗号分隔）或列表
                current_data = current_price(symbols=symbols)

            if not current_data:
                logger.warning("未获取到任何实时价格数据")
                return {}

            # 转换为RealtimePrice对象
            result = {}
            for item in current_data:
                symbol = item['symbol']
                price = item['price']
                created_at = item['created_at']

                result[symbol] = RealtimePrice(
                    symbol=symbol,
                    price=price,
                    created_at=created_at
                )

            logger.info(f"成功获取 {len(result)} 只股票的实时价格")
            return result

        except Exception as e:
            logger.error(f"获取实时价格失败: {e}")
            return {}

    def get_current_price(self, symbol: str) -> Optional[RealtimePrice]:
        """
        获取单只股票的实时价格

        Args:
            symbol: 股票代码，格式为'SHSE.600000'

        Returns:
            RealtimePrice对象，如果获取失败则返回None
        """
        result = self.get_current_prices([symbol])
        return result.get(symbol)

    def get_pool_current_prices(self, pool_symbols: List[str]) -> tuple[Dict[str, RealtimePrice], datetime]:
        """
        获取股池中所有股票的实时价格

        Args:
            pool_symbols: 股池股票代码列表

        Returns:
            (价格字典, 最早的创建时间)
            价格字典：键为股票代码，值为RealtimePrice对象
            最早的创建时间：所有股票中最早的价格时间戳
        """
        prices = self.get_current_prices(pool_symbols)

        if not prices:
            return {}, None

        # 找出所有股票中最早的价格时间
        earliest_time = min(price.created_at for price in prices.values())

        logger.info(f"股池实时价格获取完成，最早时间: {earliest_time}")

        return prices, earliest_time

    def calculate_realtime_change(self, 
                                  symbols: List[str], 
                                  base_prices: Dict[str, float]) -> Dict[str, float]:
        """
        计算股票相对于基准价格的涨跌幅

        Args:
            symbols: 股票代码列表
            base_prices: 基准价格字典，键为股票代码，值为基准价格

        Returns:
            涨跌幅字典，键为股票代码，值为涨跌幅（%）
        """
        current_prices = self.get_current_prices(symbols)

        changes = {}
        for symbol in symbols:
            if symbol not in current_prices or symbol not in base_prices:
                continue

            current_price = current_prices[symbol].price
            base_price = base_prices[symbol]

            if base_price > 0:
                change_pct = ((current_price / base_price) - 1) * 100
                changes[symbol] = change_pct
            else:
                logger.warning(f"股票 {symbol} 基准价格为0，无法计算涨跌幅")

        return changes

    def get_realtime_dataframe(self, symbols: List[str]) -> pd.DataFrame:
        """
        获取实时价格数据并转换为DataFrame格式

        Args:
            symbols: 股票代码列表

        Returns:
            包含实时价格的DataFrame
        """
        prices = self.get_current_prices(symbols)

        if not prices:
            return pd.DataFrame()

        data = []
        for symbol, price_info in prices.items():
            data.append({
                'symbol': symbol,
                'price': price_info.price,
                'created_at': price_info.created_at
            })

        df = pd.DataFrame(data)
        logger.debug(f"实时价格DataFrame: {len(df)} 条记录")

        return df
