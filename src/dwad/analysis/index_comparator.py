"""
股池指数比较和排名模块

该模块用于比较多个股池指数的表现，计算排名并生成可视化报告
"""

import pandas as pd
import yaml
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from loguru import logger

from ..data_storage.parquet_storage import ParquetStorage
from ..data_fetcher.realtime_price_fetcher import RealtimePriceFetcher
from ..utils.config import config


class IndexComparator:
    """股池指数比较器"""
    
    def __init__(self, comparison_config_path: Optional[str] = None, enable_realtime: bool = False):
        """
        初始化指数比较器
        
        Args:
            comparison_config_path: 比较配置文件路径，如果为None则使用默认配置
            enable_realtime: 是否启用实时价格功能
        """
        self.storage = ParquetStorage()
        self.comparison_config = self._load_comparison_config(comparison_config_path)
        self.indices_data = {}
        self.comparison_result = None
        self.enable_realtime = enable_realtime
        self.realtime_fetcher = None
        self.realtime_prices_cache = {}  # 缓存实时价格数据，避免重复API调用
        self.stock_pool_cache = {}  # 缓存股池配置，避免重复加载和警告
        
        if enable_realtime:
            try:
                self.realtime_fetcher = RealtimePriceFetcher()
                logger.info("实时价格功能已启用")
            except Exception as e:
                logger.warning(f"实时价格功能初始化失败: {e}，将仅使用历史数据")
                self.enable_realtime = False
        
    def _load_comparison_config(self, config_path: Optional[str] = None) -> dict:
        """
        加载比较配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            配置字典
        """
        if config_path is None:
            # 使用默认配置文件路径
            config_path = Path(__file__).parent.parent.parent.parent / "config" / "index_comparison.yaml"
        else:
            config_path = Path(config_path)
            
        if not config_path.exists():
            raise FileNotFoundError(f"比较配置文件不存在: {config_path}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
            
        logger.info(f"已加载比较配置文件: {config_path}")
        return config_data
    
    def load_indices_data(self) -> bool:
        """
        加载所有需要比较的指数数据
        
        Returns:
            是否成功加载
        """
        indices_to_compare = self.comparison_config.get('indices_to_compare', [])
        
        if not indices_to_compare:
            logger.error("比较配置中没有指定任何指数")
            return False
            
        logger.info(f"开始加载 {len(indices_to_compare)} 个指数数据...")
        
        loaded_count = 0
        for idx_config in indices_to_compare:
            pool_name = idx_config['pool_name']
            concept_name = idx_config['concept_name']
            display_name = idx_config.get('display_name', concept_name)
            
            # 加载指数数据
            index_data = self.storage.load_index_data(
                pool_name=pool_name,
                concept_name=concept_name,
                index_type='average'
            )
            
            if index_data.empty:
                logger.warning(f"未找到指数数据: [{pool_name} - {concept_name}]")
                continue
                
            # 确保日期列是datetime类型
            index_data['date'] = pd.to_datetime(index_data['date'])
            
            # 存储指数数据
            key = f"{pool_name}_{concept_name}"
            self.indices_data[key] = {
                'pool_name': pool_name,
                'concept_name': concept_name,
                'display_name': display_name,
                'data': index_data
            }
            
            loaded_count += 1
            logger.info(f"  ✓ 已加载 [{display_name}]: {len(index_data)} 个交易日")
            
        if loaded_count == 0:
            logger.error("未能加载任何指数数据")
            return False
            
        logger.info(f"成功加载 {loaded_count}/{len(indices_to_compare)} 个指数数据")
        return True
    
    def _normalize_and_align_data(self, period: Optional[int] = None) -> pd.DataFrame:
        """
        归一化并对齐所有指数数据
        
        从比较起点开始，将所有指数归一化到100，并对齐到相同的交易日序列
        
        Args:
            period: 分析周期（交易日数），如果指定则只取最近N个交易日的数据
                   None表示使用全部数据
        
        Returns:
            对齐后的数据DataFrame，包含所有指数的归一化值
        """
        comparison_start_date = self.comparison_config.get('comparison_start_date')
        
        # 如果没有配置起始日期，使用所有指数中最晚的起始日期
        if comparison_start_date is None:
            start_dates = [data['data']['date'].min() for data in self.indices_data.values()]
            comparison_start_date = max(start_dates)
            if period is None:
                logger.info(f"未配置比较起始日期，使用最晚的起始日期: {comparison_start_date}")
        else:
            comparison_start_date = pd.to_datetime(comparison_start_date)
            if period is None:
                logger.info(f"比较起始日期: {comparison_start_date}")
        
        # 收集所有指数在起始日期之后的数据
        aligned_data = {}
        all_dates = set()
        
        for key, idx_info in self.indices_data.items():
            data = idx_info['data'].copy()
            display_name = idx_info['display_name']
            
            # 筛选起始日期之后的数据
            data = data[data['date'] >= comparison_start_date].copy()
            
            if data.empty:
                logger.warning(f"指数 [{display_name}] 在起始日期 {comparison_start_date} 之后没有数据")
                continue
            
            # 如果指定了周期，只取最近N个交易日的数据
            if period is not None and len(data) > period:
                data = data.tail(period).copy()
            
            # 获取周期起始日期的指数值（用于归一化）
            base_value = data.iloc[0]['index_value']
            
            # 归一化到100
            data['normalized_value'] = (data['index_value'] / base_value) * 100
            
            # 计算相对于起始日期的涨跌幅（%）
            data['change_pct'] = ((data['index_value'] / base_value) - 1) * 100
            
            aligned_data[display_name] = data.set_index('date')
            all_dates.update(data['date'].tolist())
            
            logger.debug(f"  {display_name}: {len(data)} 个交易日, 起始值={base_value:.2f}")
        
        if not aligned_data:
            logger.error("没有可用于比较的指数数据")
            return pd.DataFrame()
        
        # 创建统一的日期索引（所有指数的交易日并集）
        all_dates = sorted(all_dates)
        result_df = pd.DataFrame(index=all_dates)
        result_df.index.name = 'date'
        
        # 将各指数的归一化值、原始值和涨跌幅添加到结果DataFrame
        # 使用字典收集所有列，然后一次性添加，避免DataFrame碎片化
        all_columns = {}
        for display_name, data in aligned_data.items():
            all_columns[f'{display_name}_normalized'] = data['normalized_value']
            all_columns[f'{display_name}_change'] = data['change_pct']
            all_columns[f'{display_name}_index_value'] = data['index_value']  # 添加原始指数值
        
        # 一次性添加所有列
        if all_columns:
            result_df = pd.concat([result_df, pd.DataFrame(all_columns, index=result_df.index)], axis=1)
        
        # 前向填充缺失值（某些指数可能在某些日期没有数据）
        result_df = result_df.ffill()
        
        if period is None:
            logger.info(f"数据归一化完成，共 {len(result_df)} 个交易日")
        return result_df
    
    def _resolve_tied_rankings(self, change_data: pd.DataFrame, previous_rankings: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        解决并列排名，确保每个指数有唯一排名
        
        当涨跌幅相同时，通过以下顺序打破并列：
        1. 比较前一天的排名，排名靠前的优先
        2. 如果没有历史排名或仍然相同，按照指数名称字母顺序
        
        Args:
            change_data: 涨跌幅数据 DataFrame
            previous_rankings: 前一天的排名 DataFrame（可选）
        
        Returns:
            解决并列后的排名 DataFrame
        """
        import random
        
        rankings = pd.DataFrame(index=change_data.index, columns=change_data.columns)
        
        for date in change_data.index:
            # 获取该日所有指数的涨跌幅
            day_changes = change_data.loc[date].copy()
            
            # 创建一个列表来存储 (指数名, 涨跌幅, 前一天排名)
            ranking_data = []
            for col in day_changes.index:
                change_pct = day_changes[col]
                prev_rank = None
                
                # 尝试获取前一天的排名
                if previous_rankings is not None:
                    date_idx = change_data.index.get_loc(date)
                    if date_idx > 0:
                        prev_date = change_data.index[date_idx - 1]
                        if prev_date in previous_rankings.index and col in previous_rankings.columns:
                            prev_rank = previous_rankings.loc[prev_date, col]
                
                ranking_data.append((col, change_pct, prev_rank if prev_rank is not None else 999))
            
            # 排序：首先按涨跌幅降序，然后按前一天排名升序，最后按名称
            ranking_data.sort(key=lambda x: (-x[1], x[2], x[0]))
            
            # 分配排名
            for rank, (col, _, _) in enumerate(ranking_data, 1):
                rankings.loc[date, col] = rank
        
        return rankings
    
    def calculate_rankings(self, period: Optional[int] = None) -> pd.DataFrame:
        """
        计算每个交易日的排名
        
        排名基于涨跌幅，涨幅最大的排名第1
        
        Args:
            period: 分析周期（交易日数），如果指定则使用滑动窗口计算每日排名
                   None表示从起始日期累计计算排名
        
        Returns:
            包含排名信息的DataFrame
        """
        if period is None:
            # 没有指定周期，使用原来的逻辑（从起始日期累计）
            aligned_df = self._normalize_and_align_data(period=None)
            
            if aligned_df.empty:
                logger.error("无法计算排名：数据为空")
                return pd.DataFrame()
            
            # 提取涨跌幅列
            change_columns = [col for col in aligned_df.columns if col.endswith('_change')]
            
            if not change_columns:
                logger.error("无法计算排名：没有涨跌幅数据")
                return pd.DataFrame()
            
            # 创建结果DataFrame
            ranking_df = pd.DataFrame(index=aligned_df.index)
            
            # 提取涨跌幅数据用于排名计算
            change_data = aligned_df[change_columns].copy()
            
            # 计算每个交易日的排名，并解决并列问题
            rankings = self._resolve_tied_rankings(change_data)
            
            # 将排名结果、归一化值和涨跌幅添加到结果DataFrame
            # 使用字典收集所有列，然后一次性添加，避免DataFrame碎片化
            result_columns = {}
            for col in change_columns:
                display_name = col.replace('_change', '')
                result_columns[display_name] = rankings[col]
            normalized_columns = [col for col in aligned_df.columns if col.endswith('_normalized')]
            for col in normalized_columns:
                display_name = col.replace('_normalized', '')
                result_columns[f'{display_name}_value'] = aligned_df[col]
            for col in change_columns:
                display_name = col.replace('_change', '')
                result_columns[f'{display_name}_pct'] = aligned_df[col]
            if result_columns:
                ranking_df = pd.concat([ranking_df, pd.DataFrame(result_columns, index=ranking_df.index)], axis=1)
            
            logger.info(f"排名计算完成，共 {len(ranking_df)} 个交易日（全部交易日数据）")
            
            # 显示最新排名
            self._print_latest_ranking(ranking_df)
            
            self.comparison_result = ranking_df
            return ranking_df
        else:
            # 使用滑动窗口计算排名
            return self._calculate_rolling_rankings(period)
    
    def _calculate_rolling_rankings(self, window: int) -> pd.DataFrame:
        """
        使用滑动窗口计算排名
        
        每个交易日的排名基于该日往前推window天的涨跌幅
        例如：20日窗口，T日的排名是基于T-20到T这20天的涨幅排名
        
        Args:
            window: 窗口大小（交易日数）
        
        Returns:
            包含排名信息的DataFrame
        """
        # 获取所有指数的完整数据
        all_data = {}
        for key, idx_info in self.indices_data.items():
            display_name = idx_info['display_name']
            data = idx_info['data'].copy()
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index('date').sort_index()
            all_data[display_name] = data['index_value']
        
        # 合并成一个DataFrame
        df = pd.DataFrame(all_data)
        
        # 计算滑动窗口涨跌幅：每个日期的值除以window天前的值
        rolling_change = {}
        for col in df.columns:
            # 计算N天前的值
            shifted = df[col].shift(window)
            # 计算涨跌幅（百分比）
            rolling_change[f'{col}_pct'] = ((df[col] / shifted) - 1) * 100
        
        change_df = pd.DataFrame(rolling_change)
        
        # 只保留有完整数据的行（前window行会是NaN）
        change_df = change_df.dropna()
        
        # 只取最近的数据用于展示
        if len(change_df) > window:
            change_df = change_df.tail(window)
        
        # 计算每日排名
        ranking_df = pd.DataFrame(index=change_df.index)
        
        # 提取涨跌幅列
        change_columns = [col for col in change_df.columns if col.endswith('_pct')]
        
        # 对每一天的涨跌幅进行排名，并解决并列问题
        rankings = self._resolve_tied_rankings(change_df[change_columns])
        
        # 添加排名和涨跌幅数据
        # 使用字典收集数据，然后一次性添加，避免DataFrame碎片化
        rank_and_change_columns = {}
        for col in change_columns:
            display_name = col.replace('_pct', '')
            rank_and_change_columns[display_name] = rankings[col]
            rank_and_change_columns[f'{display_name}_pct'] = change_df[col]
        
        # 一次性添加所有排名和涨跌幅列
        if rank_and_change_columns:
            ranking_df = pd.concat([ranking_df, pd.DataFrame(rank_and_change_columns, index=ranking_df.index)], axis=1)
        
        # 计算归一化值和原始值（基于窗口起始日期）
        # 使用字典收集所有新列，然后一次性添加，避免DataFrame碎片化
        new_columns = {}
        for display_name in all_data.keys():
            series = df[display_name]
            # 对于ranking_df中的每个日期，计算其相对于window天前的归一化值和原始值
            normalized_values = []
            index_values = []
            base_values = []
            base_dates = []
            for date in ranking_df.index:
                if date in series.index:
                    # 找到window天前的日期
                    idx = series.index.get_loc(date)
                    if idx >= window:
                        base_idx = idx - window
                        base_value = series.iloc[base_idx]
                        base_date = series.index[base_idx]
                        current_value = series.loc[date]
                        normalized = (current_value / base_value) * 100
                        normalized_values.append(normalized)
                        index_values.append(current_value)
                        base_values.append(base_value)
                        base_dates.append(base_date.strftime('%Y-%m-%d'))
                    else:
                        normalized_values.append(None)
                        index_values.append(None)
                        base_values.append(None)
                        base_dates.append(None)
                else:
                    normalized_values.append(None)
                    index_values.append(None)
                    base_values.append(None)
                    base_dates.append(None)
            
            # 将计算结果存入字典
            new_columns[f'{display_name}_value'] = normalized_values
            new_columns[f'{display_name}_index_value'] = index_values
            new_columns[f'{display_name}_base_value'] = base_values
            new_columns[f'{display_name}_base_date'] = base_dates
        
        # 使用pd.concat一次性添加所有列，避免DataFrame碎片化
        if new_columns:
            new_df = pd.DataFrame(new_columns, index=ranking_df.index)
            ranking_df = pd.concat([ranking_df, new_df], axis=1)
        
        logger.info(f"滑动窗口排名计算完成，窗口={window}天，共 {len(ranking_df)} 个交易日")
        
        # 显示最新排名
        self._print_latest_ranking(ranking_df)
        
        return ranking_df
    
    def _print_latest_ranking(self, ranking_df: pd.DataFrame):
        """打印最新排名信息"""
        if not ranking_df.empty:
            latest_date = ranking_df.index[-1]
            logger.info(f"\n最新排名 ({latest_date}):")
            
            # 获取排名列（排除所有辅助列）
            rank_columns = [col for col in ranking_df.columns 
                          if not col.endswith('_value') 
                          and not col.endswith('_pct')
                          and not col.endswith('_index_value')
                          and not col.endswith('_base_value')
                          and not col.endswith('_base_date')]
            latest_ranks = ranking_df.loc[latest_date, rank_columns].sort_values()
            
            for display_name, rank in latest_ranks.items():
                change_pct = ranking_df.loc[latest_date, f'{display_name}_pct']
                logger.info(f"  {int(rank)}. {display_name}: {change_pct:+.2f}%")
    
    def get_ranking_data_for_visualization(self, periods: list = None, include_realtime: bool = None, clear_realtime_cache: bool = True) -> Dict[str, any]:
        """
        获取用于可视化的排名数据（支持多周期）
        
        Args:
            periods: 周期列表，例如 [20, 55, 233] 表示近20、55、233个交易日
                    如果为None，则返回全部数据
            include_realtime: 是否包含实时数据，如果为None则根据enable_realtime自动决定
        
        Returns:
            包含可视化所需数据的字典
        """
        # 确定是否包含实时数据
        if include_realtime is None:
            include_realtime = self.enable_realtime
        
        # 如果没有指定周期，使用全部数据
        if periods is None:
            if self.comparison_result is None:
                logger.warning("尚未计算排名，正在计算...")
                self.calculate_rankings()
            
            if self.comparison_result is None or self.comparison_result.empty:
                logger.error("无法获取可视化数据：排名结果为空")
                return {}
            
            # 提取排名列（不包括_value和_pct后缀的列）
            rank_columns = [col for col in self.comparison_result.columns 
                           if not col.endswith('_value') 
                           and not col.endswith('_pct')
                           and not col.endswith('_index_value')
                           and not col.endswith('_base_value')
                           and not col.endswith('_base_date')]
            
            # 获取总指数数量
            total_indices = len(rank_columns)
            
            dates = self.comparison_result.index.strftime('%Y-%m-%d').tolist()
            series_data = []
            for display_name in rank_columns:
                ranks = self.comparison_result[display_name].tolist()
                changes = self.comparison_result[f'{display_name}_pct'].tolist()
                # 使用原始指数值
                index_values = self.comparison_result[f'{display_name}_index_value'].tolist()
                # 计算基准值和基准日期（第一天的指数值和日期）
                base_value = index_values[0] if index_values else 1000.0
                base_values = [base_value] * len(index_values)
                base_date = dates[0] if dates else ''
                base_dates = [base_date] * len(dates)
                series_data.append({
                    'name': display_name,
                    'dates': dates,
                    'ranks': ranks,
                    'changes': changes,
                    'index_values': index_values,
                    'base_values': base_values,
                    'base_dates': base_dates
                })
            
            result = {
                'dates': dates,
                'series': series_data,
                'total_indices': total_indices,
                'config': self.comparison_config.get('visualization', {})
            }
            
            # 添加实时数据（使用全部数据周期，从起始日期到现在）
            if include_realtime:
                # 计算从起始日期到现在的天数作为period
                realtime_period = len(dates) if dates else None
                realtime_ranking = self.get_realtime_ranking(period=realtime_period)
                if realtime_ranking:
                    result['realtime'] = realtime_ranking
            
            return result
        
        # 对于多周期分析，分别计算每个周期的排名
        periods_data = []
        total_indices = len(self.indices_data)
        
        # 如果启用实时功能，先一次性获取所有实时价格并缓存
        if include_realtime:
            logger.info("批量获取实时价格数据（用于所有周期）...")
            self.realtime_prices_cache, _ = self._fetch_all_realtime_prices()
            if not self.realtime_prices_cache:
                logger.warning("无法获取实时价格，将不包含实时数据")
                include_realtime = False
        
        for period in periods:
            logger.info(f"  计算近{period}个交易日的排名...")
            
            # 为每个周期单独计算排名
            period_df = self.calculate_rankings(period=period)
            
            if period_df.empty:
                logger.warning(f"周期 {period} 天的数据为空")
                continue
            
            # 提取排名列（不包括_value和_pct后缀的列）
            rank_columns = [col for col in period_df.columns 
                           if not col.endswith('_value') 
                           and not col.endswith('_pct')
                           and not col.endswith('_index_value')
                           and not col.endswith('_base_value')
                           and not col.endswith('_base_date')]
            
            dates = period_df.index.strftime('%Y-%m-%d').tolist()
            series_data = []
            
            for display_name in rank_columns:
                ranks = period_df[display_name].tolist()
                changes = period_df[f'{display_name}_pct'].tolist()
                # 使用原始指数值而不是归一化值
                index_values = period_df[f'{display_name}_index_value'].tolist()
                base_values = period_df[f'{display_name}_base_value'].tolist()
                base_dates = period_df[f'{display_name}_base_date'].tolist()
                series_data.append({
                    'name': display_name,
                    'dates': dates,
                    'ranks': ranks,
                    'changes': changes,
                    'index_values': index_values,
                    'base_values': base_values,
                    'base_dates': base_dates,
                    'period': period  # 添加周期信息，用于hover显示
                })
            
            period_data = {
                'period': period,
                'title': f'近{period}个交易日排名趋势（基于该周期内涨跌幅）',
                'dates': dates,
                'series': series_data,
                'total_indices': total_indices
            }
            
            # 如果启用实时功能，计算该周期的实时排名（使用缓存的价格数据）
            if include_realtime:
                realtime_ranking = self.get_realtime_ranking(
                    period=period, 
                    use_cache=True,
                    historical_rankings=period_df  # 传递历史排名数据
                )
                if realtime_ranking:
                    period_data['realtime'] = realtime_ranking
            
            periods_data.append(period_data)
        
        result = {
            'periods': periods_data,
            'total_indices': total_indices,
            'config': self.comparison_config.get('visualization', {})
        }
        
        # 清空缓存，避免下次调用使用过期数据
        if clear_realtime_cache:
            self.realtime_prices_cache = {}
        
        return result
    
    def export_ranking_to_csv(self, output_path: Optional[str] = None) -> bool:
        """
        导出排名结果到CSV文件
        
        Args:
            output_path: 输出文件路径
            
        Returns:
            是否导出成功
        """
        if self.comparison_result is None or self.comparison_result.empty:
            logger.error("无法导出：排名结果为空")
            return False
        
        if output_path is None:
            output_dir = Path(__file__).parent.parent.parent.parent / "reports"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "index_ranking_data.csv"
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.comparison_result.to_csv(output_path, encoding='utf-8-sig')
            logger.info(f"排名数据已导出到: {output_path}")
            return True
        except Exception as e:
            logger.error(f"导出CSV失败: {e}")
            return False
    
    def run(self) -> bool:
        """
        执行完整的指数比较流程
        
        Returns:
            是否执行成功
        """
        logger.info("="*60)
        logger.info("开始股池指数比较分析...")
        logger.info("="*60)
        
        # 1. 加载指数数据
        if not self.load_indices_data():
            logger.error("加载指数数据失败")
            return False
        
        # 2. 计算排名
        ranking_df = self.calculate_rankings()
        
        if ranking_df.empty:
            logger.error("排名计算失败")
            return False
        
        # 3. 导出到CSV
        self.export_ranking_to_csv()
        
        logger.info("="*60)
        logger.info("股池指数比较分析完成")
        logger.info("="*60)
        
        return True
    
    def _load_stock_pool_config(self, pool_name: str, concept_name: str) -> List[str]:
        """
        加载股池配置，返回股票代码列表
        
        Args:
            pool_name: 股池名称
            concept_name: 概念名称
            
        Returns:
            股票代码列表
        """
        # 检查缓存
        cache_key = f"{pool_name}_{concept_name}"
        if cache_key in self.stock_pool_cache:
            return self.stock_pool_cache[cache_key]
        
        try:
            # 读取stock_pools.yaml配置文件
            config_path = Path(__file__).parent.parent.parent.parent / "config" / "stock_pools.yaml"
            if not config_path.exists():
                logger.error(f"股池配置文件不存在: {config_path}")
                self.stock_pool_cache[cache_key] = []
                return []
            
            with open(config_path, 'r', encoding='utf-8') as f:
                pools_config = yaml.safe_load(f)
            
            # 从配置中获取股票名称列表
            if 'stock_pools' not in pools_config:
                logger.error(f"股池配置文件格式错误，缺少'stock_pools'节点")
                self.stock_pool_cache[cache_key] = []
                return []
            
            if pool_name not in pools_config['stock_pools']:
                logger.error(f"未找到股池: {pool_name}")
                self.stock_pool_cache[cache_key] = []
                return []
            
            if concept_name not in pools_config['stock_pools'][pool_name]:
                logger.error(f"未找到概念: {pool_name} - {concept_name}")
                self.stock_pool_cache[cache_key] = []
                return []
            
            stock_names = pools_config['stock_pools'][pool_name][concept_name]
            if not stock_names:
                logger.warning(f"股池 [{pool_name} - {concept_name}] 没有股票")
                self.stock_pool_cache[cache_key] = []
                return []
            
            # 加载股票基本信息以获取股票代码
            stock_info_df = self.storage.load_stock_info()
            if stock_info_df.empty:
                logger.error("无法加载股票基本信息")
                self.stock_pool_cache[cache_key] = []
                return []
            
            symbols = []
            for name in stock_names:
                # 使用 regex=False 避免特殊字符（如 *ST 中的 *）被当作正则表达式
                matched = stock_info_df[stock_info_df['name'].str.contains(name, na=False, regex=False)]
                if not matched.empty:
                    symbols.append(matched.iloc[0]['symbol'])
                else:
                    logger.warning(f"未找到股票 '{name}' 对应的代码")
            
            # 缓存结果
            self.stock_pool_cache[cache_key] = symbols
            return symbols
            
        except Exception as e:
            logger.error(f"加载股池配置失败: {e}")
            self.stock_pool_cache[cache_key] = []
            return []
    
    def _fetch_all_realtime_prices(self) -> Tuple[Dict, str]:
        """
        一次性获取所有股池的实时价格（用于缓存）
        
        Returns:
            (所有股池的实时价格字典, 最早时间戳)
            格式: {display_name: {symbol: StockPrice, ...}, ...}
        """
        if not self.enable_realtime or self.realtime_fetcher is None:
            logger.warning("实时价格功能未启用")
            return {}, None
        
        if not self.indices_data:
            logger.error("未加载指数数据，无法获取实时价格")
            return {}, None
        
        logger.info("开始批量获取所有股池的实时价格（缓存用）...")
        
        all_prices = {}
        all_timestamps = []
        
        for key, idx_info in self.indices_data.items():
            pool_name = idx_info['pool_name']
            concept_name = idx_info['concept_name']
            display_name = idx_info['display_name']
            
            # 获取股池中的股票代码
            symbols = self._load_stock_pool_config(pool_name, concept_name)
            if not symbols:
                logger.warning(f"股池 [{pool_name} - {concept_name}] 没有股票")
                continue
            
            # 获取实时价格
            prices, earliest_time = self.realtime_fetcher.get_pool_current_prices(symbols)
            if not prices:
                logger.warning(f"无法获取 [{display_name}] 的实时价格")
                continue
            
            all_prices[display_name] = prices
            all_timestamps.append(earliest_time)
        
        earliest_timestamp = min(all_timestamps) if all_timestamps else None
        logger.info(f"批量获取实时价格完成，共 {len(all_prices)} 个股池")
        
        return all_prices, earliest_timestamp
    
    def get_realtime_ranking(self, period: Optional[int] = None, use_cache: bool = False, historical_rankings: Optional[pd.DataFrame] = None) -> Optional[Dict]:
        """
        获取实时排名数据
        
        Args:
            period: 计算涨跌幅的周期（交易日数），如果为None则使用昨日收盘价作为基准
                   如果指定，则使用period天前的收盘价作为基准
            use_cache: 是否使用缓存的实时价格数据（用于多周期场景，避免重复API调用）
            historical_rankings: 历史排名 DataFrame，用于打破实时排名并列
        
        Returns:
            包含实时排名的字典，如果获取失败则返回None
        """
        if not self.enable_realtime or self.realtime_fetcher is None:
            logger.warning("实时价格功能未启用")
            return None
        
        if not self.indices_data:
            logger.error("未加载指数数据，无法获取实时排名")
            return None
        
        # 如果使用缓存且缓存为空，先获取实时价格
        if use_cache and not self.realtime_prices_cache:
            self.realtime_prices_cache, cached_timestamp = self._fetch_all_realtime_prices()
            if not self.realtime_prices_cache:
                logger.warning("无法获取实时价格缓存")
                return None
        
        # 如果不使用缓存，直接获取实时价格
        if not use_cache:
            logger.info("开始获取实时排名...")
        
        realtime_data = {}
        all_timestamps = []
        
        # 为每个指数获取实时价格并计算涨跌幅
        for key, idx_info in self.indices_data.items():
            pool_name = idx_info['pool_name']
            concept_name = idx_info['concept_name']
            display_name = idx_info['display_name']
            
            # 获取股池中的股票代码
            symbols = self._load_stock_pool_config(pool_name, concept_name)
            if not symbols:
                logger.warning(f"股池 [{pool_name} - {concept_name}] 没有股票")
                continue
            
            # 获取实时价格（使用缓存或直接获取）
            if use_cache:
                if display_name not in self.realtime_prices_cache:
                    logger.warning(f"缓存中没有 [{display_name}] 的实时价格")
                    continue
                prices = self.realtime_prices_cache[display_name]
                # 使用缓存时，时间戳从缓存数据中获取
                if prices:
                    earliest_time = list(prices.values())[0].created_at
                    all_timestamps.append(earliest_time)
            else:
                prices, earliest_time = self.realtime_fetcher.get_pool_current_prices(symbols)
                if not prices:
                    logger.warning(f"无法获取 [{display_name}] 的实时价格")
                    continue
                all_timestamps.append(earliest_time)
            
            # 获取历史数据
            historical_data = idx_info['data'].copy()
            if historical_data.empty:
                continue
            
            historical_data['date'] = pd.to_datetime(historical_data['date'])
            historical_data = historical_data.sort_values('date')
            
            # 实时指数始终基于昨日收盘计算
            last_date = historical_data['date'].max()
            last_index_value = historical_data[historical_data['date'] == last_date]['index_value'].iloc[0]
            logger.debug(f"[{display_name}] 使用昨日({last_date.strftime('%Y-%m-%d')})的指数值: {last_index_value:.2f}")
            
            # 如果指定了period，还需要获取period天前的指数值（用于计算排名涨跌幅）
            period_base_index_value = None
            period_base_date = None
            if period is not None:
                if len(historical_data) >= period:
                    period_base_data = historical_data.iloc[-period]
                    period_base_index_value = period_base_data['index_value']
                    period_base_date = period_base_data['date']
                    logger.debug(f"[{display_name}] {period}天前({period_base_date.strftime('%Y-%m-%d')})的指数值: {period_base_index_value:.2f}")
                else:
                    logger.warning(f"[{display_name}] 历史数据不足{period}天，跳过")
                    continue
            
            # 计算实时指数值
            # 获取昨日收盘价（实时涨跌幅的基准）
            stock_data = self.storage.load_stock_info()
            
            yesterday_prices = {}
            for symbol in symbols:
                stock_df = self.storage.load_stock_data(symbol)
                if not stock_df.empty:
                    stock_df['date'] = pd.to_datetime(stock_df['date'])
                    yesterday_data = stock_df[stock_df['date'] == last_date]
                    if not yesterday_data.empty:
                        yesterday_prices[symbol] = yesterday_data.iloc[0]['close_price']
            
            # 计算今日实时涨跌幅（相对于昨日收盘）
            if yesterday_prices:
                today_changes = []
                for symbol in symbols:
                    if symbol in prices and symbol in yesterday_prices:
                        current_price = prices[symbol].price
                        yesterday_price = yesterday_prices[symbol]
                        if yesterday_price > 0:
                            change = ((current_price / yesterday_price) - 1) * 100
                            today_changes.append(change)

                if today_changes:
                    # 今日平均涨跌幅（当日实时涨幅，后续在前端显示用）
                    avg_today_change = sum(today_changes) / len(today_changes)
                    # 实时指数值 = 昨日指数值 × (1 + 今日涨跌幅)
                    realtime_index_value = last_index_value * (1 + avg_today_change / 100)

                    # 计算用于排名的涨跌幅
                    if period is not None and period_base_index_value is not None:
                        # 如果指定了period，排名涨跌幅 = (实时指数 / period天前指数 - 1) × 100
                        ranking_change_pct = ((realtime_index_value / period_base_index_value) - 1) * 100
                        logger.debug(f"[{display_name}] 实时指数={realtime_index_value:.2f}, 今日涨跌={avg_today_change:+.2f}%, {period}天涨跌={ranking_change_pct:+.2f}%")
                    else:
                        # 如果没有指定period，排名涨跌幅就是今日涨跌幅
                        ranking_change_pct = avg_today_change
                        logger.debug(f"[{display_name}] 实时指数={realtime_index_value:.2f}, 今日涨跌={avg_today_change:+.2f}%")

                    # 保存实时数据：包括用于排名的周期涨跌幅和当日实时涨幅
                    realtime_data[display_name] = {
                        'index_value': realtime_index_value,
                        'change_pct': ranking_change_pct,      # 用于排名的涨跌幅（可能是周期）
                        'today_change_pct': avg_today_change,   # 当日实时涨幅
                        'base_value': last_index_value,         # 昨日指数值
                        'base_date': last_date.strftime('%Y-%m-%d'),
                        'period_base_value': period_base_index_value if period_base_index_value is not None else None,
                        'period_base_date': period_base_date.strftime('%Y-%m-%d') if period_base_date is not None else None
                    }
        
        if not realtime_data:
            logger.warning("未能获取任何实时数据")
            return None
        
        # 获取最后一个历史交易日的排名（用于打破并列）
        last_historical_ranks = {}
        
        # 优先使用传入的历史排名
        if historical_rankings is not None and not historical_rankings.empty:
            last_date = historical_rankings.index[-1]
            for col in historical_rankings.columns:
                if not col.endswith('_value') and not col.endswith('_pct'):
                    last_historical_ranks[col] = historical_rankings.loc[last_date, col]
        else:
            # 如果没有传入，尝试从 self.comparison_result 获取
            for key, idx_info in self.indices_data.items():
                display_name = idx_info['display_name']
                if display_name in realtime_data:
                    historical_data = idx_info['data'].copy()
                    if not historical_data.empty:
                        historical_data['date'] = pd.to_datetime(historical_data['date'])
                        last_date = historical_data['date'].max()
                        
                        # 尝试从已计算的排名结果中获取
                        if self.comparison_result is not None and not self.comparison_result.empty:
                            if last_date in self.comparison_result.index and display_name in self.comparison_result.columns:
                                last_historical_ranks[display_name] = self.comparison_result.loc[last_date, display_name]
        
        # 计算实时排名，打破并列
        # 创建排序数据：(指数名, 涨跌幅, 历史排名)
        changes_list = []
        for name, data in realtime_data.items():
            change_pct = data['change_pct']
            hist_rank = last_historical_ranks.get(name, 999)  # 没有历史排名的给一个很大的值
            changes_list.append((name, change_pct, hist_rank))
        
        # 排序：首先按涨跌幅降序，然后按历史排名升序，最后按名称字母顺序
        changes_list.sort(key=lambda x: (-x[1], x[2], x[0]))
        
        realtime_rankings = {}
        for rank, (name, change, _) in enumerate(changes_list, 1):
            realtime_rankings[name] = {
                'rank': rank,
                'change_pct': change,
                'today_change_pct': realtime_data[name].get('today_change_pct'),
                'index_value': realtime_data[name]['index_value'],
                'base_value': realtime_data[name]['base_value'],
                'base_date': realtime_data[name]['base_date'],
                'period_base_value': realtime_data[name].get('period_base_value'),
                'period_base_date': realtime_data[name].get('period_base_date')
            }
        
        # 找到最早的时间戳
        earliest_timestamp = min(all_timestamps) if all_timestamps else None
        
        period_desc = f"近{period}日" if period else "当天"
        logger.info(f"实时排名计算完成，共 {len(realtime_rankings)} 个指数（基于{period_desc}涨跌幅）")
        logger.info(f"实时数据时间: {earliest_timestamp}")
        
        return {
            'rankings': realtime_rankings,
            'timestamp': earliest_timestamp
        }
