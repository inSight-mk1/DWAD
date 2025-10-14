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
from ..utils.config import config


class IndexComparator:
    """股池指数比较器"""
    
    def __init__(self, comparison_config_path: Optional[str] = None):
        """
        初始化指数比较器
        
        Args:
            comparison_config_path: 比较配置文件路径，如果为None则使用默认配置
        """
        self.storage = ParquetStorage()
        self.comparison_config = self._load_comparison_config(comparison_config_path)
        self.indices_data = {}
        self.comparison_result = None
        
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
        
        # 将各指数的归一化值和涨跌幅添加到结果DataFrame
        for display_name, data in aligned_data.items():
            result_df[f'{display_name}_normalized'] = data['normalized_value']
            result_df[f'{display_name}_change'] = data['change_pct']
        
        # 前向填充缺失值（某些指数可能在某些日期没有数据）
        result_df = result_df.ffill()
        
        if period is None:
            logger.info(f"数据归一化完成，共 {len(result_df)} 个交易日")
        return result_df
    
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
            
            # 计算每个交易日的排名（对每一行进行排名，axis=1）
            rankings = change_data.rank(axis=1, ascending=False, method='min')
            
            # 将排名结果添加到结果DataFrame
            for col in change_columns:
                display_name = col.replace('_change', '')
                ranking_df[display_name] = rankings[col]
            
            # 添加归一化值和涨跌幅
            normalized_columns = [col for col in aligned_df.columns if col.endswith('_normalized')]
            for col in normalized_columns:
                display_name = col.replace('_normalized', '')
                ranking_df[f'{display_name}_value'] = aligned_df[col]
            
            for col in change_columns:
                display_name = col.replace('_change', '')
                ranking_df[f'{display_name}_pct'] = aligned_df[col]
            
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
        
        # 对每一天的涨跌幅进行排名
        rankings = change_df[change_columns].rank(axis=1, ascending=False, method='min')
        
        # 添加排名和涨跌幅数据
        for col in change_columns:
            display_name = col.replace('_pct', '')
            ranking_df[display_name] = rankings[col]
            ranking_df[f'{display_name}_pct'] = change_df[col]
        
        # 计算归一化值（基于窗口起始日期）
        for display_name in all_data.keys():
            series = df[display_name]
            # 对于ranking_df中的每个日期，计算其相对于window天前的归一化值
            normalized_values = []
            for date in ranking_df.index:
                if date in series.index:
                    # 找到window天前的日期
                    idx = series.index.get_loc(date)
                    if idx >= window:
                        base_idx = idx - window
                        base_value = series.iloc[base_idx]
                        current_value = series.loc[date]
                        normalized = (current_value / base_value) * 100
                        normalized_values.append(normalized)
                    else:
                        normalized_values.append(None)
                else:
                    normalized_values.append(None)
            
            ranking_df[f'{display_name}_value'] = normalized_values
        
        logger.info(f"滑动窗口排名计算完成，窗口={window}天，共 {len(ranking_df)} 个交易日")
        
        # 显示最新排名
        self._print_latest_ranking(ranking_df)
        
        return ranking_df
    
    def _print_latest_ranking(self, ranking_df: pd.DataFrame):
        """打印最新排名信息"""
        if not ranking_df.empty:
            latest_date = ranking_df.index[-1]
            logger.info(f"\n最新排名 ({latest_date}):")
            
            # 获取排名列
            rank_columns = [col for col in ranking_df.columns 
                          if not col.endswith('_value') and not col.endswith('_pct')]
            latest_ranks = ranking_df.loc[latest_date, rank_columns].sort_values()
            
            for display_name, rank in latest_ranks.items():
                change_pct = ranking_df.loc[latest_date, f'{display_name}_pct']
                logger.info(f"  {int(rank)}. {display_name}: {change_pct:+.2f}%")
    
    def get_ranking_data_for_visualization(self, periods: list = None) -> Dict[str, any]:
        """
        获取用于可视化的排名数据（支持多周期）
        
        Args:
            periods: 周期列表，例如 [20, 55, 233] 表示近20、55、233个交易日
                    如果为None，则返回全部数据
        
        Returns:
            包含可视化所需数据的字典
        """
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
                           if not col.endswith('_value') and not col.endswith('_pct')]
            
            # 获取总指数数量
            total_indices = len(rank_columns)
            
            dates = self.comparison_result.index.strftime('%Y-%m-%d').tolist()
            series_data = []
            for display_name in rank_columns:
                ranks = self.comparison_result[display_name].tolist()
                changes = self.comparison_result[f'{display_name}_pct'].tolist()
                series_data.append({
                    'name': display_name,
                    'dates': dates,
                    'ranks': ranks,
                    'changes': changes
                })
            
            return {
                'dates': dates,
                'series': series_data,
                'total_indices': total_indices,
                'config': self.comparison_config.get('visualization', {})
            }
        
        # 对于多周期分析，分别计算每个周期的排名
        periods_data = []
        total_indices = len(self.indices_data)
        
        for period in periods:
            logger.info(f"  计算近{period}个交易日的排名...")
            
            # 为每个周期单独计算排名
            period_df = self.calculate_rankings(period=period)
            
            if period_df.empty:
                logger.warning(f"周期 {period} 天的数据为空")
                continue
            
            # 提取排名列（不包括_value和_pct后缀的列）
            rank_columns = [col for col in period_df.columns 
                           if not col.endswith('_value') and not col.endswith('_pct')]
            
            dates = period_df.index.strftime('%Y-%m-%d').tolist()
            series_data = []
            
            for display_name in rank_columns:
                ranks = period_df[display_name].tolist()
                changes = period_df[f'{display_name}_pct'].tolist()
                series_data.append({
                    'name': display_name,
                    'dates': dates,
                    'ranks': ranks,
                    'changes': changes
                })
            
            periods_data.append({
                'period': period,
                'title': f'近{period}个交易日排名趋势（基于该周期内涨跌幅）',
                'dates': dates,
                'series': series_data,
                'total_indices': total_indices
            })
        
        return {
            'periods': periods_data,
            'total_indices': total_indices,
            'config': self.comparison_config.get('visualization', {})
        }
    
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
