import os
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from loguru import logger

from ..utils.config import config
from ..data_fetcher.goldminer_fetcher import StockInfo, MarketData


class ParquetStorage:
    """Parquet文件存储管理器"""

    def __init__(self, base_path: Optional[str] = None):
        """
        初始化存储管理器

        Args:
            base_path: 数据存储基础路径
        """
        if base_path is None:
            paths = config.get_data_paths()
            base_path = paths['base_path']

        self.base_path = Path(base_path)
        self.stocks_path = self.base_path / "stocks"
        self.indices_path = self.base_path / "indices"
        self.metadata_path = self.base_path / "metadata"

        # 确保目录存在
        self._ensure_directories()

    def _ensure_directories(self):
        """确保所有必要的目录存在"""
        for path in [self.base_path, self.stocks_path, self.indices_path, self.metadata_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _get_stock_file_path(self, symbol: str) -> Path:
        """
        获取股票数据文件路径

        Args:
            symbol: 股票代码

        Returns:
            文件路径
        """
        # 将股票代码中的点号替换为下划线，避免文件名问题
        safe_symbol = symbol.replace('.', '_')
        return self.stocks_path / f"{safe_symbol}.parquet"

    def save_stock_data(self, symbol: str, data: pd.DataFrame) -> bool:
        """
        保存股票历史数据

        Args:
            symbol: 股票代码
            data: 股票数据DataFrame

        Returns:
            是否保存成功
        """
        try:
            if data.empty:
                logger.warning(f"股票{symbol}的数据为空，跳过保存")
                return False

            file_path = self._get_stock_file_path(symbol)

            # 确保数据格式正确
            data = data.copy()
            if 'date' in data.columns:
                data['date'] = pd.to_datetime(data['date']).dt.strftime('%Y-%m-%d')

            # 按日期排序
            if 'date' in data.columns:
                data = data.sort_values('date')

            # 保存为Parquet格式
            data.to_parquet(file_path, index=False, compression='snappy')

            logger.debug(f"成功保存股票{symbol}的{len(data)}条数据到 {file_path}")
            return True

        except Exception as e:
            logger.error(f"保存股票{symbol}数据失败: {e}")
            return False

    def load_stock_data(self, symbol: str) -> pd.DataFrame:
        """
        加载股票历史数据

        Args:
            symbol: 股票代码

        Returns:
            股票数据DataFrame
        """
        try:
            file_path = self._get_stock_file_path(symbol)

            if not file_path.exists():
                logger.debug(f"股票{symbol}的数据文件不存在: {file_path}")
                return pd.DataFrame()

            data = pd.read_parquet(file_path)
            logger.debug(f"成功加载股票{symbol}的{len(data)}条数据")
            return data

        except Exception as e:
            logger.error(f"加载股票{symbol}数据失败: {e}")
            return pd.DataFrame()

    def get_stock_date_range(self, symbol: str) -> tuple:
        """
        获取股票数据的日期范围

        Args:
            symbol: 股票代码

        Returns:
            (最早日期, 最晚日期) 或 (None, None) 如果无数据
        """
        data = self.load_stock_data(symbol)
        if data.empty or 'date' not in data.columns:
            return None, None

        min_date = data['date'].min()
        max_date = data['date'].max()
        return min_date, max_date

    def delete_stock_data(self, symbol: str) -> bool:
        """
        删除股票历史数据文件

        Args:
            symbol: 股票代码

        Returns:
            是否删除成功
        """
        try:
            file_path = self._get_stock_file_path(symbol)

            if not file_path.exists():
                logger.debug(f"股票{symbol}的数据文件不存在，无需删除")
                return True

            # 删除文件
            file_path.unlink()
            logger.debug(f"成功删除股票{symbol}的数据文件")
            return True

        except Exception as e:
            logger.error(f"删除股票{symbol}数据失败: {e}")
            return False

    def append_stock_data(self, symbol: str, new_data: pd.DataFrame) -> bool:
        """
        追加股票数据（用于增量更新，已不推荐）
        
        注意：由于前复权数据的特性，不建议使用增量更新。
        推荐使用全量更新（先删除再重新下载）以确保数据连续性。

        Args:
            symbol: 股票代码
            new_data: 新数据

        Returns:
            是否成功
        """
        try:
            # 加载现有数据
            existing_data = self.load_stock_data(symbol)

            if existing_data.empty:
                # 如果没有现有数据，直接保存新数据
                return self.save_stock_data(symbol, new_data)

            # 合并数据
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)

            # 去重（基于日期）
            if 'date' in combined_data.columns:
                combined_data = combined_data.drop_duplicates(subset=['date'], keep='last')
                combined_data = combined_data.sort_values('date')

            # 保存合并后的数据
            return self.save_stock_data(symbol, combined_data)

        except Exception as e:
            logger.error(f"追加股票{symbol}数据失败: {e}")
            return False

    def save_stock_info(self, stock_list: List[StockInfo]) -> bool:
        """
        保存股票基本信息

        Args:
            stock_list: 股票信息列表

        Returns:
            是否保存成功
        """
        try:
            stock_info_data = []
            for stock in stock_list:
                stock_info_data.append({
                    'symbol': stock.symbol,
                    'name': stock.name,
                    'market': stock.market,
                    'listing_date': stock.listing_date
                })

            # 转换为DataFrame并保存
            df = pd.DataFrame(stock_info_data)
            file_path = self.metadata_path / "stock_info.parquet"
            df.to_parquet(file_path, index=False)

            logger.info(f"成功保存{len(stock_list)}只股票的基本信息")
            return True

        except Exception as e:
            logger.error(f"保存股票基本信息失败: {e}")
            return False

    def load_stock_info(self, as_dataframe: bool = True):
        """
        加载股票基本信息

        Args:
            as_dataframe: 是否返回DataFrame，默认True；False则返回StockInfo对象列表

        Returns:
            股票信息DataFrame或列表
        """
        try:
            file_path = self.metadata_path / "stock_info.parquet"

            if not file_path.exists():
                logger.warning("股票基本信息文件不存在")
                return pd.DataFrame() if as_dataframe else []

            df = pd.read_parquet(file_path)

            if as_dataframe:
                logger.debug(f"成功加载{len(df)}只股票的基本信息")
                return df
            else:
                stock_list = []
                for _, row in df.iterrows():
                    stock = StockInfo(
                        symbol=row['symbol'],
                        name=row['name'],
                        market=row['market'],
                        listing_date=row.get('listing_date', None)
                    )
                    stock_list.append(stock)

                logger.debug(f"成功加载{len(stock_list)}只股票的基本信息")
                return stock_list

        except Exception as e:
            logger.error(f"加载股票基本信息失败: {e}")
            return pd.DataFrame() if as_dataframe else []

    def save_update_log(self, update_info: Dict[str, Any]) -> bool:
        """
        保存数据更新日志

        Args:
            update_info: 更新信息

        Returns:
            是否保存成功
        """
        try:
            log_file = self.metadata_path / "update_log.json"

            # 加载现有日志
            if log_file.exists():
                with open(log_file, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
            else:
                logs = []

            # 添加时间戳
            update_info['timestamp'] = datetime.now().isoformat()

            # 添加新日志
            logs.append(update_info)

            # 保留最近100条记录
            logs = logs[-100:]

            # 保存日志
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, ensure_ascii=False, indent=2)

            return True

        except Exception as e:
            logger.error(f"保存更新日志失败: {e}")
            return False

    def get_latest_update_info(self) -> Optional[Dict[str, Any]]:
        """
        获取最新的更新信息

        Returns:
            最新更新信息或None
        """
        try:
            log_file = self.metadata_path / "update_log.json"

            if not log_file.exists():
                return None

            with open(log_file, 'r', encoding='utf-8') as f:
                logs = json.load(f)

            if not logs:
                return None

            return logs[-1]

        except Exception as e:
            logger.error(f"获取最新更新信息失败: {e}")
            return None

    def list_available_stocks(self) -> List[str]:
        """
        列出所有可用的股票代码

        Returns:
            股票代码列表
        """
        try:
            stock_files = list(self.stocks_path.glob("*.parquet"))
            symbols = []

            for file_path in stock_files:
                # 从文件名恢复股票代码
                symbol = file_path.stem.replace('_', '.')
                symbols.append(symbol)

            return sorted(symbols)

        except Exception as e:
            logger.error(f"列出可用股票失败: {e}")
            return []

    def get_storage_stats(self) -> Dict[str, Any]:
        """
        获取存储统计信息

        Returns:
            存储统计信息
        """
        try:
            stats = {
                'total_stocks': len(self.list_available_stocks()),
                'storage_size_mb': 0,
                'last_update': None
            }

            # 计算存储大小
            total_size = 0
            for file_path in self.base_path.rglob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size

            stats['storage_size_mb'] = round(total_size / (1024 * 1024), 2)

            # 获取最新更新时间
            latest_update = self.get_latest_update_info()
            if latest_update:
                stats['last_update'] = latest_update.get('timestamp')

            return stats

        except Exception as e:
            logger.error(f"获取存储统计信息失败: {e}")
            return {}

    def save_index_data(self, pool_name: str, concept_name: str, index_type: str, data: pd.DataFrame) -> bool:
        """
        保存指数数据

        Args:
            pool_name: 股池名称
            concept_name: 概念名称
            index_type: 指数类型（如 'average', 'market_cap'）
            data: 指数数据DataFrame，应包含'date'和'index_value'列

        Returns:
            是否保存成功
        """
        try:
            if data.empty:
                logger.warning(f"指数数据为空，跳过保存: [{pool_name} - {concept_name}]")
                return False

            # 创建指数数据目录
            index_dir = self.indices_path / pool_name
            index_dir.mkdir(parents=True, exist_ok=True)

            # 构建文件名
            file_name = f"{concept_name}_{index_type}.parquet"
            file_path = index_dir / file_name

            # 确保数据格式正确
            data = data.copy()
            if 'date' in data.columns:
                data['date'] = pd.to_datetime(data['date']).dt.strftime('%Y-%m-%d')

            # 按日期排序
            if 'date' in data.columns:
                data = data.sort_values('date')

            # 保存为Parquet格式
            data.to_parquet(file_path, index=False, compression='snappy')

            logger.info(f"成功保存指数数据到 {file_path}")
            return True

        except Exception as e:
            logger.error(f"保存指数数据失败 [{pool_name} - {concept_name}]: {e}")
            return False

    def load_index_data(self, pool_name: str, concept_name: str, index_type: str) -> pd.DataFrame:
        """
        加载指数数据

        Args:
            pool_name: 股池名称
            concept_name: 概念名称
            index_type: 指数类型（如 'average', 'market_cap'）

        Returns:
            指数数据DataFrame
        """
        try:
            index_dir = self.indices_path / pool_name
            file_name = f"{concept_name}_{index_type}.parquet"
            file_path = index_dir / file_name

            if not file_path.exists():
                logger.debug(f"指数数据文件不存在: {file_path}")
                return pd.DataFrame()

            data = pd.read_parquet(file_path)
            logger.debug(f"成功加载指数数据: [{pool_name} - {concept_name}]，{len(data)}条数据")
            return data

        except Exception as e:
            logger.error(f"加载指数数据失败 [{pool_name} - {concept_name}]: {e}")
            return pd.DataFrame()