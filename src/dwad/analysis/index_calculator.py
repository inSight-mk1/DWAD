#!/usr/bin/env python3
"""
指数计算器模块
"""

import yaml
import pandas as pd
from pathlib import Path
from loguru import logger

from dwad.data_storage.parquet_storage import ParquetStorage
from dwad.utils.config import config


class IndexCalculator:
    """股池指数计算器"""

    def __init__(self, stock_pools_config_path: str = None):
        """
        初始化指数计算器
        
        Args:
            stock_pools_config_path: 股池配置文件路径，如果为None则自动查找
        """
        # 重新加载配置文件，确保获取最新配置
        config.reload()
        
        self.storage = ParquetStorage()
        self.stock_pools = self._load_stock_pools(stock_pools_config_path)
        self.stock_info_df = self.storage.load_stock_info()
        self.name_to_symbol_map = self._create_name_symbol_map()
        
        # 从配置文件读取指数计算参数
        self.base_value = config.get('index_calculator.base_value', 1000.0)
        self.index_start_date = config.get('index_calculator.start_date', None)
        
        if self.index_start_date:
            logger.info(f"指数起始日期（配置）: {self.index_start_date}")
        else:
            logger.info("指数起始日期：使用数据中最早的交易日")

    def _load_stock_pools(self, config_path: str = None) -> dict:
        """
        加载股池配置
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            股池配置字典
        """
        logger.info("加载股池配置...")
        
        # 如果没有指定路径，尝试查找配置文件
        if config_path is None:
            # 获取项目根目录
            current_dir = Path(__file__).parent
            project_root = current_dir.parent.parent.parent
            config_dir = project_root / "config"
            
            # 优先使用 stock_pools.yaml，其次使用 stock_pools_example.yaml
            possible_paths = [
                config_dir / "stock_pools.yaml",
                config_dir / "stock_pools_example.yaml"
            ]
            
            for path in possible_paths:
                if path.exists():
                    config_path = path
                    break
            
            if config_path is None:
                logger.error("未找到股池配置文件")
                return {}
        
        try:
            config_path = Path(config_path)
            if not config_path.exists():
                logger.error(f"股池配置文件不存在: {config_path}")
                return {}
            
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            stock_pools = data.get('stock_pools', {})
            logger.info(f"成功加载股池配置: {config_path}")
            logger.info(f"共有 {len(stock_pools)} 个股池分类")
            
            return stock_pools
            
        except Exception as e:
            logger.error(f"加载股池配置失败: {e}")
            return {}

    def _create_name_symbol_map(self) -> dict:
        """创建股票名称到代码的映射"""
        if self.stock_info_df is None or self.stock_info_df.empty:
            logger.error("股票基本信息未加载，无法创建名称映射")
            return {}
        
        name_map = {}
        for _, row in self.stock_info_df.iterrows():
            name_map[row['name']] = row['symbol']
        
        logger.info(f"成功创建 {len(name_map)} 个股票名称映射")
        return name_map

    def _get_symbols_from_names(self, stock_names: list) -> list:
        """根据股票名称列表获取股票代码列表"""
        symbols = []
        for name in stock_names:
            symbol = self.name_to_symbol_map.get(name)
            if symbol:
                symbols.append(symbol)
            else:
                logger.warning(f"未找到股票 '{name}' 对应的代码")
        return symbols

    def calculate_average_index(self, stock_symbols: list, pool_name: str, concept_name: str) -> pd.DataFrame:
        """
        计算平均价格指数
        
        Args:
            stock_symbols: 股票代码列表
            pool_name: 股池名称
            concept_name: 概念名称
            
        Returns:
            包含日期和指数值的DataFrame
            
        处理逻辑：
            1. 除权除息：使用前复权数据（在数据下载时已处理）
            2. 停牌：使用前向填充，停牌日价格 = 最后交易日价格
            3. 上市晚于指数起始日：直接排除该股票，不参与指数计算
            4. 退市/停牌过久：如果缺失数据超过阈值则给出警告
        """
        if not stock_symbols:
            logger.warning(f"[{pool_name} - {concept_name}] 没有有效的股票代码")
            return pd.DataFrame()

        logger.info(f"  读取 {len(stock_symbols)} 只股票的数据...")
        all_prices = []
        valid_symbols = []
        stock_first_dates = {}  # 记录每只股票的首个交易日
        
        for symbol in stock_symbols:
            df = self.storage.load_stock_data(symbol)
            if df is not None and not df.empty:
                price_series = df.set_index('date')['close_price']
                all_prices.append(price_series)
                valid_symbols.append(symbol)
                stock_first_dates[symbol] = price_series.index[0]
            else:
                logger.warning(f"  股票 {symbol} 数据为空，跳过")

        if not all_prices:
            logger.warning(f"[{pool_name} - {concept_name}] 没有可用的价格数据")
            return pd.DataFrame()

        logger.info(f"  成功读取 {len(valid_symbols)} 只股票的数据")
        
        # 合并所有价格数据
        price_df = pd.concat(all_prices, axis=1)
        price_df.columns = valid_symbols
        
        # 确定指数起始日
        if self.index_start_date:
            # 使用配置的起始日期
            index_start_date = self.index_start_date
            logger.info(f"  指数起始日（配置）: {index_start_date}")
            
            # 检查起始日期是否在数据范围内
            data_start = price_df.index[0]
            data_end = price_df.index[-1]
            if index_start_date < data_start:
                logger.warning(f"  配置的起始日期 {index_start_date} 早于数据起始日 {data_start}，将使用 {data_start}")
                index_start_date = data_start
            elif index_start_date > data_end:
                logger.error(f"  配置的起始日期 {index_start_date} 晚于数据结束日 {data_end}，无法计算指数")
                return pd.DataFrame()
            else:
                # 裁剪数据，只保留起始日期之后的数据
                price_df = price_df[price_df.index >= index_start_date]
                logger.info(f"  已将数据裁剪至起始日期 {index_start_date} 之后")
        else:
            # 使用所有股票数据的最早日期
            index_start_date = price_df.index[0]
            logger.info(f"  指数起始日（自动）: {index_start_date}")
        
        # 关键改进：排除上市晚于指数起始日的股票
        excluded_stocks = []
        valid_stocks_for_index = []
        
        for symbol in valid_symbols:
            first_date = stock_first_dates[symbol]
            if first_date > index_start_date:
                # 上市晚于指数起始日，直接排除
                excluded_stocks.append((symbol, first_date))
                logger.warning(f"  ⚠️  股票 {symbol} 上市日期 {first_date} 晚于指数起始日 {index_start_date}，已排除")
            else:
                valid_stocks_for_index.append(symbol)
        
        # 检查是否有足够的股票参与计算
        if not valid_stocks_for_index:
            logger.error(f"[{pool_name} - {concept_name}] 所有股票上市日期都晚于指数起始日，无法计算指数")
            return pd.DataFrame()
        
        if excluded_stocks:
            logger.warning(f"  共排除 {len(excluded_stocks)} 只上市晚于指数起始日的股票")
            logger.warning(f"  实际参与指数计算的股票数: {len(valid_stocks_for_index)}/{len(valid_symbols)}")
        
        # 只保留有效股票的数据
        price_df = price_df[valid_stocks_for_index]
        
        # 处理停牌：前向填充（停牌期间使用最后交易日价格）
        price_df = price_df.fillna(method='ffill')
        
        # 数据质量检查
        total_dates = len(price_df)
        for symbol in valid_stocks_for_index:
            missing_count = price_df[symbol].isna().sum()
            missing_rate = missing_count / total_dates
            if missing_rate > 0.5:
                logger.warning(f"  股票 {symbol} 缺失数据比例: {missing_rate:.1%}，可能影响指数准确性")
        
        # 计算平均价格（所有有效股票等权重）
        average_price = price_df.mean(axis=1)
        
        # 检查数据完整性
        stocks_count_per_day = price_df.count(axis=1)
        min_stocks = stocks_count_per_day.min()
        if min_stocks < len(valid_stocks_for_index) * 0.8:
            logger.warning(f"  部分交易日参与计算的股票数量过少（最少{min_stocks}只），指数可能不稳定")
        
        # 归一化到基准点（以第一个交易日为基准）
        if len(average_price) > 0 and average_price.iloc[0] > 0:
            normalized_index = (average_price / average_price.iloc[0]) * self.base_value
            logger.info(f"  指数基准值: {self.base_value}")
        else:
            logger.error(f"[{pool_name} - {concept_name}] 基准日数据无效")
            return pd.DataFrame()
        
        # 转换为DataFrame并添加统计信息
        result_df = normalized_index.to_frame(name='index_value')
        result_df['stocks_count'] = stocks_count_per_day  # 每日实际有数据的股票数量
        result_df.index.name = 'date'
        result_df = result_df.reset_index()
        
        logger.info(f"  ✓ 指数计算完成，共 {len(result_df)} 个交易日")
        logger.info(f"  ✓ 成分股数量: {len(valid_stocks_for_index)} 只（排除了 {len(excluded_stocks)} 只）")
        logger.info(f"  ✓ 平均每日有数据的股票数: {stocks_count_per_day.mean():.1f}/{len(valid_stocks_for_index)}")
        
        return result_df

    def calculate_all_indices(self):
        """计算所有股池的指数"""
        if not self.stock_pools:
            logger.warning("股池配置为空，无法计算指数")
            return False

        total_count = sum(len(concepts) for concepts in self.stock_pools.values())
        current = 0
        success_count = 0
        
        for pool_name, concepts in self.stock_pools.items():
            for concept_name, stock_names in concepts.items():
                current += 1
                logger.info(f"[{current}/{total_count}] 正在计算: [{pool_name} - {concept_name}]")
                
                # 获取股票代码
                stock_symbols = self._get_symbols_from_names(stock_names)
                
                if not stock_symbols:
                    logger.warning(f"  概念 '{concept_name}' 中没有有效的股票，跳过计算")
                    continue

                # 计算平均指数
                avg_index = self.calculate_average_index(stock_symbols, pool_name, concept_name)
                
                if not avg_index.empty:
                    # 保存指数数据
                    save_success = self.storage.save_index_data(
                        pool_name, concept_name, 'average', avg_index
                    )
                    if save_success:
                        success_count += 1
                        logger.info(f"  ✓ 平均指数计算并保存成功")
                    else:
                        logger.error(f"  ✗ 指数保存失败")
                else:
                    logger.warning(f"  ✗ 指数计算失败")
        
        logger.info(f"指数计算完成！成功: {success_count}/{total_count}")
        return success_count > 0

    def run(self):
        """执行所有计算任务"""
        logger.info("开始计算股池指数...")
        success = self.calculate_all_indices()
        logger.info("所有指数计算完成！")
        return bool(success)


def main():
    """主函数"""
    calculator = IndexCalculator()
    try:
        success = calculator.run()
        return bool(success)
    except Exception as e:
        logger.error(f"指数计算过程中发生异常: {e}")
        return False
