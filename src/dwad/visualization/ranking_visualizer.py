"""
股池指数排名可视化模块

该模块用于生成股池指数排名的可视化Web页面
"""

from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
from loguru import logger
import json


class RankingVisualizer:
    """指数排名可视化器"""
    
    def __init__(self):
        """初始化可视化器"""
        self.colors = [
            '#FF6B6B',  # 红色
            '#4ECDC4',  # 青色
            '#45B7D1',  # 蓝色
            '#FFA07A',  # 浅橙色
            '#98D8C8',  # 薄荷绿
            '#F7DC6F',  # 黄色
            '#BB8FCE',  # 紫色
            '#85C1E2',  # 天蓝色
            '#F8B88B',  # 桃色
            '#AAB7B8',  # 灰色
        ]
    
    def generate_html(self, ranking_data: Dict, output_path: Optional[str] = None) -> bool:
        """
        生成排名可视化HTML页面（支持单图表和多图表）
        
        Args:
            ranking_data: 排名数据字典，由IndexComparator.get_ranking_data_for_visualization()生成
            output_path: 输出文件路径
            
        Returns:
            是否生成成功
        """
        # 判断是单图表还是多图表模式
        if 'periods' in ranking_data:
            # 多周期模式
            return self._generate_multi_period_html(ranking_data, output_path)
        elif 'series' in ranking_data:
            # 单图表模式
            return self._generate_single_html(ranking_data, output_path)
        else:
            logger.error("无法生成可视化：排名数据格式错误")
            return False
    
    def _generate_single_html(self, ranking_data: Dict, output_path: Optional[str] = None) -> bool:
        """
        生成单图表HTML页面
        
        Args:
            ranking_data: 排名数据字典
            output_path: 输出文件路径
            
        Returns:
            是否生成成功
        """
        if not ranking_data or 'series' not in ranking_data:
            logger.error("无法生成可视化：排名数据为空")
            return False
        
        # 获取配置
        vis_config = ranking_data.get('config', {})
        title = vis_config.get('title', '股池指数排名趋势')
        width = vis_config.get('width', 1400)
        height = vis_config.get('height', 800)
        show_markers = vis_config.get('show_markers', True)
        line_width = vis_config.get('line_width', 2)
        show_grid = vis_config.get('show_grid', True)
        
        # 确定输出路径
        if output_path is None:
            output_dir = Path(__file__).parent.parent.parent.parent / "reports"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = vis_config.get('output_filename', 'index_ranking_comparison.html')
            output_path = output_dir / output_filename
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 准备Plotly数据
        traces = []
        series_list = ranking_data['series']
        total_indices = ranking_data['total_indices']
        
        # 获取日期列表（用于x轴标签）
        first_series = series_list[0]
        dates = first_series['dates'][1:] if len(first_series['dates']) > 1 else first_series['dates']
        
        for idx, series in enumerate(series_list):
            color = self.colors[idx % len(self.colors)]
            # 跳过第一个数据点（第一天所有指数涨跌幅都是0%，排名无意义）
            series_dates = series['dates'][1:] if len(series['dates']) > 1 else series['dates']
            ranks = series['ranks'][1:] if len(series['ranks']) > 1 else series['ranks']
            changes = series['changes'][1:] if len(series['changes']) > 1 else series['changes']
            index_values = series['index_values'][1:] if len(series['index_values']) > 1 else series['index_values']
            base_values = series['base_values'][1:] if len(series['base_values']) > 1 else series['base_values']
            base_dates = series['base_dates'][1:] if len(series['base_dates']) > 1 else series['base_dates']
            
            # 使用交易日索引作为x轴（从0开始）
            x_values = list(range(len(ranks)))
            
            # 准备customdata: [日期, 涨跌幅, 当前指数值, 基准日期, 基准指数值]
            period = len(series['dates']) - 1  # 计算周期长度
            customdata = []
            for date, change, idx_val, base_date, base_val in zip(series_dates, changes, index_values, base_dates, base_values):
                customdata.append([date, change, idx_val, base_date, base_val])
            
            trace = {
                'x': x_values,
                'y': ranks,
                'name': series['name'],
                'type': 'scatter',
                'mode': 'lines',  # 只显示线条，不显示数据点
                'line': {
                    'width': line_width,
                    'color': color
                },
                'legendgroup': series['name'],  # 与实时数据同组
                'customdata': customdata,
                'hovertemplate': f"<b>{series['name']}</b><br>" +
                                "%{customdata[0]}: %{customdata[2]:.2f}<br>" +
                                "%{customdata[3]}: %{customdata[4]:.2f}<br>" +
                                "排名: %{y}<br>" +
                                f"近{period}日涨跌幅: %{{customdata[1]:.2f}}%<br>" +
                                "<extra></extra>"
            }
            traces.append(trace)
        
        # 添加实时数据（如果存在）
        realtime_data = ranking_data.get('realtime')
        realtime_timestamp = None
        if realtime_data:
            realtime_rankings = realtime_data.get('rankings', {})
            realtime_timestamp = realtime_data.get('timestamp')
            
            if realtime_rankings:
                logger.info(f"添加实时数据到图表，时间: {realtime_timestamp}")
                
                # 为每个指数添加实时数据点（虚线）
                for idx, series in enumerate(series_list):
                    name = series['name']
                    if name in realtime_rankings:
                        color = self.colors[idx % len(self.colors)]
                        
                        # 获取最后一个历史数据点
                        last_x = len(series['ranks'][1:]) - 1 if len(series['ranks']) > 1 else 0
                        last_rank = series['ranks'][-1]
                        
                        # 计算周期长度
                        period = len(series['dates']) - 1
                        
                        # 实时排名
                        realtime_rank = realtime_rankings[name]['rank']
                        # 周期涨跌幅（用于曲线和标签中的“近N日涨跌幅”）
                        realtime_change = realtime_rankings[name]['change_pct']
                        # 当日实时涨幅（相对于昨日收盘），用于右侧标签显示
                        realtime_today_change = realtime_rankings[name].get('today_change_pct')
                        realtime_index = realtime_rankings[name]['index_value']
                        base_value = realtime_rankings[name]['base_value']
                        base_date = realtime_rankings[name]['base_date']
                        period_base_value = realtime_rankings[name].get('period_base_value')
                        period_base_date = realtime_rankings[name].get('period_base_date')
                        period_base_label = period_base_date if period_base_date else (f"T-{period}" if period else "T-20")
                        period_base_value_str = f"{period_base_value:.2f}" if period_base_value is not None else "--"
                        
                        # 创建细直线trace（从最后历史点到实时点）
                        realtime_trace = {
                            'x': [last_x, last_x + 1],
                            'y': [last_rank, realtime_rank],
                            'name': f'{name}',
                            'type': 'scatter',
                            'mode': 'lines',
                            'line': {
                                'width': line_width * 0.5,  # 更细的线条
                                'color': color
                            },
                            'showlegend': False,  # 不在图例中显示
                            'legendgroup': name,  # 与历史数据同组
                            'hoverinfo': 'skip',  # 不显示悬停信息，避免历史点被覆盖
                            'is_realtime': True,  # 标记这是实时数据
                            'realtime_change': realtime_change  # 存储实时涨跌幅
                        }
                        traces.append(realtime_trace)
                        
                        # 添加实时数据点标记
                        # 将realtime_timestamp转换为日期字符串（只显示日期，不显示时分秒）
                        if realtime_timestamp:
                            # 如果是datetime对象，转换为日期字符串
                            try:
                                if isinstance(realtime_timestamp, datetime):
                                    timestamp_str = realtime_timestamp.strftime('%Y-%m-%d')
                                else:
                                    # 如果是字符串，尝试解析并格式化
                                    dt = datetime.fromisoformat(str(realtime_timestamp).replace('+08:00', ''))
                                    timestamp_str = dt.strftime('%Y-%m-%d')
                            except:
                                timestamp_str = '实时'
                        else:
                            timestamp_str = '实时'
                        marker_trace = {
                            'x': [last_x + 1],
                            'y': [realtime_rank],
                            'name': f'{name}',
                            'type': 'scatter',
                            'mode': 'markers',
                            'marker': {
                                'size': 10,
                                'color': color,
                                'symbol': 'circle',
                                'line': {
                                    'width': 2,
                                    'color': 'white'
                                }
                            },
                            'showlegend': False,
                            'legendgroup': name,  # 与历史和实时线同组
                            'customdata': [[
                                timestamp_str,
                                realtime_change,
                                realtime_index,
                                base_date,
                                base_value,
                                period_base_label,
                                period_base_value_str
                            ]],
                            'hovertemplate': f"<b>{name} (实时)</b><br>" +
                                            "%{customdata[0]}: %{customdata[2]:.2f}<br>" +
                                            "%{customdata[3]}: %{customdata[4]:.2f}<br>" +
                                            "%{customdata[5]}: %{customdata[6]}<br>" +
                                            "排名: %{y}<br>" +
                                            f"近{period}日涨跌幅: %{{customdata[1]:.2f}}%<br>" +
                                            "<extra></extra>"
                        }
                        traces.append(marker_trace)
                
                # 如果有实时数据，扩展日期列表
                dates = dates + ['实时']
        
        # 生成HTML内容
        # 使用indent=2格式化JSON，便于调试
        html_content = self._generate_html_template(
            title=title,
            traces_json=json.dumps(traces, ensure_ascii=False, indent=2),
            dates_json=json.dumps(dates, ensure_ascii=False),  # 传递日期列表用于x轴标签
            width=width,
            height=height,
            total_indices=total_indices,
            show_grid=show_grid,
            realtime_timestamp=realtime_timestamp
        )
        
        # 写入文件
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"排名可视化页面已生成: {output_path}")
            return True
        except Exception as e:
            logger.error(f"生成HTML文件失败: {e}")
            return False
    
    def _generate_multi_period_html(self, ranking_data: Dict, output_path: Optional[str] = None) -> bool:
        """
        生成多周期图表HTML页面
        
        Args:
            ranking_data: 包含多个周期数据的字典
            output_path: 输出文件路径
            
        Returns:
            是否生成成功
        """
        if not ranking_data or 'periods' not in ranking_data:
            logger.error("无法生成可视化：多周期数据为空")
            return False
        
        # 获取配置
        vis_config = ranking_data.get('config', {})
        width = vis_config.get('width', 1400)
        # 多图表模式：每个图表高度600px，适合一屏显示图表和图例
        height = vis_config.get('multi_chart_height', 600)
        line_width = vis_config.get('line_width', 2)
        show_grid = vis_config.get('show_grid', True)
        total_indices = ranking_data['total_indices']
        
        # 确定输出路径
        if output_path is None:
            output_dir = Path(__file__).parent.parent.parent.parent / "reports"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = vis_config.get('output_filename', 'index_ranking_comparison.html')
            output_path = output_dir / output_filename
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 为每个周期准备traces数据
        all_periods_traces = []
        realtime_timestamp = None
        
        for period_data in ranking_data['periods']:
            traces = []
            series_list = period_data['series']
            
            # 获取日期列表（用于x轴标签）
            first_series = series_list[0]
            dates = first_series['dates'][1:] if len(first_series['dates']) > 1 else first_series['dates']
            
            for idx, series in enumerate(series_list):
                color = self.colors[idx % len(self.colors)]
                # 跳过第一个数据点（第一天所有指数涨跌幅都是0%，排名无意义）
                series_dates = series['dates'][1:] if len(series['dates']) > 1 else series['dates']
                ranks = series['ranks'][1:] if len(series['ranks']) > 1 else series['ranks']
                changes = series['changes'][1:] if len(series['changes']) > 1 else series['changes']
                index_values = series['index_values'][1:] if len(series['index_values']) > 1 else series['index_values']
                base_values = series['base_values'][1:] if len(series['base_values']) > 1 else series['base_values']
                base_dates = series['base_dates'][1:] if len(series['base_dates']) > 1 else series['base_dates']
                
                # 获取周期信息（用于计算基准日期的指数值）
                period = series.get('period', len(series['dates']))
                
                # 使用交易日索引作为x轴（从0开始）
                x_values = list(range(len(ranks)))
                
                # 准备customdata: [日期, 涨跌幅, 当前指数值, 基准日期, 基准指数值]
                customdata = []
                for date, change, idx_val, base_date, base_val in zip(series_dates, changes, index_values, base_dates, base_values):
                    customdata.append([date, change, idx_val, base_date, base_val])
                
                trace = {
                    'x': x_values,
                    'y': ranks,
                    'name': series['name'],
                    'type': 'scatter',
                    'mode': 'lines',
                    'line': {
                        'width': line_width,
                        'color': color
                    },
                    'legendgroup': series['name'],  # 与实时数据同组
                    'customdata': customdata,
                    'hovertemplate': f"<b>{series['name']}</b><br>" +
                                    "%{customdata[0]}: %{customdata[2]:.2f}<br>" +
                                    "%{customdata[3]}: %{customdata[4]:.2f}<br>" +
                                    "排名: %{y}<br>" +
                                    f"近{period}日涨跌幅: %{{customdata[1]:.2f}}%<br>" +
                                    "<extra></extra>"
                }
                traces.append(trace)
            
            # 添加实时数据（如果存在）
            realtime_data = period_data.get('realtime')
            if realtime_data:
                realtime_rankings = realtime_data.get('rankings', {})
                realtime_timestamp = realtime_data.get('timestamp')
                
                if realtime_rankings:
                    logger.info(f"为周期 {period_data['period']} 天添加实时数据")
                    
                    # 为每个指数添加实时数据点（虚线）
                    for idx, series in enumerate(series_list):
                        name = series['name']
                        if name in realtime_rankings:
                            color = self.colors[idx % len(self.colors)]
                            
                            # 获取最后一个历史数据点
                            last_x = len(series['ranks'][1:]) - 1 if len(series['ranks']) > 1 else 0
                            last_rank = series['ranks'][-1]
                            
                            # 实时排名
                            realtime_rank = realtime_rankings[name]['rank']
                            # 周期涨跌幅（用于曲线和标签中的“近N日涨跌幅”）
                            realtime_change = realtime_rankings[name]['change_pct']
                            # 当日实时涨幅（相对于昨日收盘），用于右侧标签显示
                            realtime_today_change = realtime_rankings[name].get('today_change_pct')
                            realtime_index = realtime_rankings[name]['index_value']
                            base_value = realtime_rankings[name]['base_value']
                            base_date = realtime_rankings[name]['base_date']
                            period_base_value = realtime_rankings[name].get('period_base_value')
                            period_base_date = realtime_rankings[name].get('period_base_date')
                            period_base_label = period_base_date if period_base_date else (f"T-{period_data['period']}" if period_data.get('period') else "T-20")
                            period_base_value_str = f"{period_base_value:.2f}" if period_base_value is not None else "--"
                            
                            # 创建细直线trace（从最后历史点到实时点）
                            realtime_trace = {
                                'x': [last_x, last_x + 1],
                                'y': [last_rank, realtime_rank],
                                'name': f'{name}',  # 不加(实时)后缀，保持一致
                                'type': 'scatter',
                                'mode': 'lines',
                                'line': {
                                    'width': line_width * 0.5,  # 更细的线条
                                    'color': color
                                },
                                'showlegend': False,  # 不在图例中显示
                                'legendgroup': name,  # 与历史数据同组
                                'hoverinfo': 'skip',  # 不显示悬停信息，避免历史点被覆盖
                                'is_realtime': True,  # 标记这是实时数据
                                'realtime_change': realtime_change,           # 存储周期涨跌幅
                                'realtime_today_change': realtime_today_change  # 存储当日实时涨幅
                            }
                            traces.append(realtime_trace)
                            
                            # 添加实时数据点标记
                            # 将realtime_timestamp转换为日期字符串（只显示日期，不显示时分秒）
                            if realtime_timestamp:
                                # 如果是datetime对象，转换为日期字符串
                                try:
                                    if isinstance(realtime_timestamp, datetime):
                                        timestamp_str = realtime_timestamp.strftime('%Y-%m-%d')
                                    else:
                                        # 如果是字符串，尝试解析并格式化
                                        dt = datetime.fromisoformat(str(realtime_timestamp).replace('+08:00', ''))
                                        timestamp_str = dt.strftime('%Y-%m-%d')
                                except:
                                    timestamp_str = '实时'
                            else:
                                timestamp_str = '实时'
                            marker_trace = {
                                'x': [last_x + 1],
                                'y': [realtime_rank],
                                'name': f'{name}',
                                'type': 'scatter',
                                'mode': 'markers',
                                'marker': {
                                    'size': 8,
                                    'color': color,
                                    'symbol': 'circle'
                                },
                                'showlegend': False,
                                'legendgroup': name,  # 与历史和实时线同组
                                'customdata': [[
                                    timestamp_str,
                                    realtime_change,
                                    realtime_index,
                                    base_date,
                                    base_value,
                                    period_base_label,
                                    period_base_value_str
                                ]],
                                'hovertemplate': f"<b>{name} (实时)</b><br>" +
                                                "%{customdata[0]}: %{customdata[2]:.2f}<br>" +
                                                "%{customdata[3]}: %{customdata[4]:.2f}<br>" +
                                                "%{customdata[5]}: %{customdata[6]}<br>" +
                                                "排名: %{y}<br>" +
                                                f"近{period}日涨跌幅: %{{customdata[1]:.2f}}%<br>" +
                                                "<extra></extra>"
                            }
                            traces.append(marker_trace)
                    
                    # 如果有实时数据，扩展日期列表
                    dates = dates + ['实时']
            
            all_periods_traces.append({
                'period': period_data['period'],
                'title': period_data['title'],
                'traces': traces,
                'dates': dates  # 添加日期列表
            })
        
        # 生成多图表HTML
        html_content = self._generate_multi_chart_template(
            all_periods_traces=all_periods_traces,
            width=width,
            height=height,
            total_indices=total_indices,
            show_grid=show_grid,
            line_width=line_width,
            realtime_timestamp=realtime_timestamp
        )
        
        # 写入文件
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"多周期排名可视化页面已生成: {output_path}")
            return True
        except Exception as e:
            logger.error(f"生成HTML文件失败: {e}")
            return False
    
    def _generate_html_template(self, title: str, traces_json: str, dates_json: str, width: int, 
                                height: int, total_indices: int, show_grid: bool, realtime_timestamp=None) -> str:
        """
        生成HTML模板
        
        Args:
            title: 图表标题
            traces_json: Plotly traces的JSON字符串
            width: 图表宽度
            height: 图表高度
            total_indices: 总指数数量
            show_grid: 是否显示网格
            
        Returns:
            HTML内容字符串
        """
        html_template = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <!-- Plotly库 - 使用多个CDN源 -->
    <script src="https://cdn.jsdelivr.net/npm/plotly.js@2.26.0/dist/plotly.min.js" 
            onerror="this.onerror=null; this.src='https://cdn.plot.ly/plotly-2.26.0.min.js'"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 8px;
        }}
        
        .container {{
            max-width: 100%;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 6px 16px;
            text-align: left;
        }}
        
        .header p {{
            font-size: 12px;
            opacity: 0.9;
            margin: 0;
        }}
        
        .chart-container {{
            padding: 30px;
            min-height: {height}px;
        }}
        
        #chart {{
            width: 100%;
            min-height: {height}px;
        }}
        
        .info-panel {{
            padding: 20px 30px;
            background: #f8f9fa;
            border-top: 1px solid #e9ecef;
        }}
        
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        
        .info-item {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }}
        
        .info-label {{
            font-size: 12px;
            color: #6c757d;
            margin-bottom: 5px;
        }}
        
        .info-value {{
            font-size: 18px;
            font-weight: 600;
            color: #212529;
        }}
        
        .footer {{
            padding: 20px 30px;
            text-align: center;
            color: #6c757d;
            font-size: 12px;
            border-top: 1px solid #e9ecef;
        }}
        
        .loading {{
            text-align: center;
            padding: 50px;
            color: #6c757d;
        }}

        .task-overlay {{
            position: fixed;
            right: 16px;
            bottom: 16px;
            z-index: 1050;
            font-size: 12px;
            color: #212529;
        }}

        .task-hidden {{
            display: none;
        }}

        .task-panel {{
            min-width: 220px;
            max-width: 320px;
            background: #ffffff;
            border-radius: 8px;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
            border: 1px solid #dee2e6;
            padding: 8px 10px;
        }}

        .task-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 4px;
        }}

        .task-title {{
            font-weight: 600;
            font-size: 12px;
            color: #343a40;
        }}

        .task-body {{
            font-size: 12px;
            color: #495057;
        }}

        .task-btn-link {{
            border: none;
            background: transparent;
            color: #0d6efd;
            cursor: pointer;
            font-size: 11px;
            padding: 0 4px;
        }}

        .task-overlay.collapsed .task-body {{
            display: none;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{title}</h1>
            <p>📊 实时追踪股池指数排名变化 · 排名越小表现越好</p>
            <p id="time-info-header" style="margin-top: 6px;">
                🚀 DWAD 股池指数分析系统 · 数据更新时间: <span id="update-time-header"></span>
                <span id="realtime-info-header" style="display: none;">
                    &nbsp;&nbsp;📡 实时数据时间: <span id="realtime-time-header"></span> (虚线部分为实时数据)
                </span>
            </p>
        </div>
        
        <div class="chart-container">
            <div id="chart" class="loading">正在加载图表...</div>
        </div>
        
        <div class="info-panel">
            <div class="info-grid">
                <div class="info-item">
                    <div class="info-label">指数数量</div>
                    <div class="info-value" id="total-indices">{total_indices}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">数据起始日期</div>
                    <div class="info-value" id="start-date">-</div>
                </div>
                <div class="info-item">
                    <div class="info-label">数据结束日期</div>
                    <div class="info-value" id="end-date">-</div>
                </div>
                <div class="info-item">
                    <div class="info-label">交易日数量</div>
                    <div class="info-value" id="trading-days">-</div>
                </div>
                <div class="info-item" id="realtime-info" style="display: none;">
                    <div class="info-label">实时数据时间</div>
                    <div class="info-value" id="realtime-time" style="font-size: 14px;">-</div>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>🚀 DWAD 股池指数分析系统 · 数据更新时间: <span id="update-time"></span></p>
        </div>
    </div>
    
    <script>
        // 数据
        const traces = {traces_json};
        const dates = {dates_json};  // 日期列表
        const realtimeTimestamp = {'null' if realtime_timestamp is None else f'"{realtime_timestamp}"'};
        
        // 更新信息面板
        if (dates.length > 0) {{
            document.getElementById('start-date').textContent = dates[0];
            document.getElementById('end-date').textContent = dates[dates.length - 1];
            document.getElementById('trading-days').textContent = dates.length;
        }}
        
        // 显示实时数据时间
        if (realtimeTimestamp && realtimeTimestamp !== 'null') {{
            document.getElementById('realtime-info').style.display = 'block';
            const realtimeDate = new Date(realtimeTimestamp);
            const realtimeText = realtimeDate.toLocaleString('zh-CN', {{
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            }});
            document.getElementById('realtime-time').textContent = realtimeText;
            const headerRealtimeInfo = document.getElementById('realtime-info-header');
            if (headerRealtimeInfo) {{
                headerRealtimeInfo.style.display = 'inline';
                document.getElementById('realtime-time-header').textContent = realtimeText;
            }}
        }}
        
        // 设置当前时间
        const now = new Date();
        const nowText = now.toLocaleString('zh-CN');
        document.getElementById('update-time').textContent = nowText;
        const updateTimeHeader = document.getElementById('update-time-header');
        if (updateTimeHeader) {{
            updateTimeHeader.textContent = nowText;
        }}
        
        // 布局配置
        const layout = {{
            title: {{
                text: '',
                font: {{
                    size: 20,
                    color: '#212529'
                }}
            }},
            xaxis: {{
                title: {{
                    text: '日期',
                    font: {{
                        size: 14,
                        color: '#495057'
                    }}
                }},
                showgrid: {str(show_grid).lower()},
                gridcolor: '#e9ecef',
                tickangle: -45,
                tickmode: 'array',
                tickvals: (() => {{
                    // 自动选择合适的刻度间隔
                    const total = dates.length;
                    const maxTicks = 15;  // 最多显示15个刻度
                    const step = Math.ceil(total / maxTicks);
                    const vals = [];
                    for (let i = 0; i < total; i += step) {{
                        vals.push(i);
                    }}
                    // 确保包含最后一个点
                    if (vals[vals.length - 1] !== total - 1) {{
                        vals.push(total - 1);
                    }}
                    return vals;
                }})(),
                ticktext: (() => {{
                    const total = dates.length;
                    const maxTicks = 15;
                    const step = Math.ceil(total / maxTicks);
                    const texts = [];
                    for (let i = 0; i < total; i += step) {{
                        texts.push(dates[i]);
                    }}
                    // 确保包含最后一个日期
                    if (texts.length === 0 || dates[texts.length - 1] !== dates[total - 1]) {{
                        texts.push(dates[total - 1]);
                    }}
                    return texts;
                }})()
            }},
            yaxis: {{
                title: {{
                    text: '排名',
                    font: {{
                        size: 14,
                        color: '#495057'
                    }}
                }},
                showgrid: {str(show_grid).lower()},
                gridcolor: '#e9ecef',
                tickmode: 'linear',
                tick0: 1,
                dtick: 1,
                range: [{total_indices} + 0.5, 0.5]  // 反转Y轴范围，使排名1在最上面
            }},
            hovermode: 'closest',
            showlegend: true,
            legend: {{
                orientation: 'v',
                x: 1.01,
                y: 1,
                xanchor: 'left',
                yanchor: 'top',
                bgcolor: 'rgba(255, 255, 255, 0.9)',
                bordercolor: '#e9ecef',
                borderwidth: 1
            }},
            margin: {{
                l: 60,
                r: 120,
                t: 40,
                b: 80
            }},
            width: {width},
            height: {height},
            plot_bgcolor: '#ffffff',
            paper_bgcolor: '#ffffff',
            font: {{
                family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
                size: 12,
                color: '#495057'
            }}
        }};
        
        // 配置选项
        const config = {{
            responsive: true,
            displayModeBar: true,
            displaylogo: false,
            modeBarButtonsToRemove: ['lasso2d', 'select2d'],
            toImageButtonOptions: {{
                format: 'png',
                filename: 'index_ranking_comparison',
                height: {height},
                width: {width},
                scale: 2
            }}
        }};
        
        // 检查Plotly是否加载成功
        function renderChart() {{
            if (typeof Plotly === 'undefined') {{
                console.error('Plotly库未加载');
                document.getElementById('chart').innerHTML = 
                    '<div style="color: #dc3545; padding: 50px; text-align: center;">' +
                    '<h3>❌ 图表库加载失败</h3>' +
                    '<p style="margin-top: 10px;">可能原因：网络问题或CDN访问受限</p>' +
                    '<p style="margin-top: 10px;">建议：检查网络连接，或尝试使用VPN访问</p>' +
                    '</div>';
                return;
            }}
            
            // 渲染图表
            Plotly.newPlot('chart', traces, layout, config)
                .then(() => {{
                    console.log('✅ 图表加载完成');
                }})
                .catch((err) => {{
                    console.error('❌ 图表渲染失败:', err);
                    document.getElementById('chart').innerHTML = 
                        '<div style="color: #dc3545; padding: 50px; text-align: center;">' +
                        '<h3>❌ 图表渲染失败</h3>' +
                        '<p style="margin-top: 10px;">错误信息: ' + err.message + '</p>' +
                        '<p style="margin-top: 10px;">请检查浏览器控制台获取更多信息</p>' +
                        '</div>';
                }});
        }}
        
        // 等待DOM和Plotly加载完成
        if (document.readyState === 'loading') {{
            document.addEventListener('DOMContentLoaded', renderChart);
        }} else {{
            // 延迟100ms确保Plotly脚本加载完成
            setTimeout(renderChart, 100);
        }}
    </script>
</body>
</html>'''
        
        return html_template
    
    def _generate_multi_chart_template(self, all_periods_traces: list, width: int, 
                                      height: int, total_indices: int, show_grid: bool, line_width: int = 2, 
                                      realtime_timestamp=None) -> str:
        """
        生成多图表HTML模板
        
        Args:
            all_periods_traces: 所有周期的traces数据列表
            width: 图表宽度
            height: 每个图表的高度
            total_indices: 总指数数量
            show_grid: 是否显示网格
            line_width: 线条宽度
            realtime_timestamp: 实时数据时间戳
            
        Returns:
            HTML内容字符串
        """
        # 生成图表div和脚本
        charts_html = ""
        charts_script = ""
        
        for idx, period_data in enumerate(all_periods_traces):
            chart_id = f"chart-{idx}"
            period = period_data['period']
            title = period_data['title']
            traces_json = json.dumps(period_data['traces'], ensure_ascii=False, indent=2)
            dates_json = json.dumps(period_data['dates'], ensure_ascii=False)  # 添加日期列表
            
            # 添加图表容器
            charts_html += f'''
        <div class="chart-section">
            <h2 class="chart-title">{title}</h2>
            <div id="{chart_id}" class="chart"></div>
            <div class="legend-actions">
                <button class="legend-btn" onclick="showAllTraces('{chart_id}')">全部显示</button>
                <button class="legend-btn" onclick="hideAllTraces('{chart_id}')">全部不显示</button>
            </div>
        </div>
'''
            
            # 添加图表渲染脚本
            # 使用JSON编码title以避免JavaScript字符串转义问题
            title_json = json.dumps(title, ensure_ascii=False)
            charts_script += f'''
        // 渲染图表 {idx + 1}: {title}
        renderSingleChart(
            '{chart_id}',
            {traces_json},
            {dates_json},
            {title_json},
            {total_indices}
        );
'''
        
        html_template = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>股池指数排名多周期分析</title>
    <!-- Plotly库 - 使用多个CDN源 -->
    <script src="https://cdn.jsdelivr.net/npm/plotly.js@2.26.0/dist/plotly.min.js" 
            onerror="this.onerror=null; this.src='https://cdn.plot.ly/plotly-2.26.0.min.js'"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 8px;
        }}
        
        .container {{
            max-width: 100%;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 6px 16px;
            text-align: left;
        }}
        
        .header p {{
            font-size: 12px;
            opacity: 0.9;
            margin: 0;
        }}
        
        .chart-section {{
            padding: 24px 16px;
            border-bottom: 2px solid #f0f0f0;
        }}
        
        .chart-section:last-child {{
            border-bottom: none;
        }}
        
        .chart-title {{
            font-size: 24px;
            font-weight: 600;
            color: #333;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }}

        /* Tab 样式 */
        .tabs {{
            display: flex;
            align-items: flex-end;
            border-bottom: 1px solid #e9ecef;
            padding: 0 24px;
            background: #ffffff;
        }}

        .tab-button {{
            padding: 8px 16px;
            font-size: 13px;
            border: none;
            border-bottom: 2px solid transparent;
            background: transparent;
            cursor: pointer;
            color: #6c757d;
        }}

        .tab-button.active {{
            color: #343a40;
            border-color: #667eea;
            font-weight: 600;
        }}

        .tab-content {{
            padding: 12px 16px 20px 16px;
        }}

        .tab-content.hidden {{
            display: none;
        }}

        .chart {{
            width: 100%;
            min-height: {height}px;
        }}

        /* 板块/个股排名表格样式 */
        .sector-table-container {{
            margin-top: 8px;
            overflow-x: auto;
        }}

        #sector-ranking-table,
        #stock-ranking-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }}

        #sector-ranking-table th,
        #sector-ranking-table td,
        #stock-ranking-table th,
        #stock-ranking-table td {{
            padding: 6px 8px;
            border-bottom: 1px solid #e9ecef;
            text-align: right;
            white-space: nowrap;
        }}

        #sector-ranking-table th:first-child,
        #sector-ranking-table td:first-child,
        #stock-ranking-table th:first-child,
        #stock-ranking-table td:first-child {{
            text-align: left;
        }}

        #sector-ranking-table th,
        #stock-ranking-table th {{
            background: #f8f9fa;
            color: #495057;
            font-weight: 600;
            cursor: pointer;
        }}

        #sector-ranking-table tr:hover,
        #stock-ranking-table tr:hover {{
            background: #f1f3f5;
        }}
        
        .legend-actions {{
            display: flex;
            justify-content: flex-end;
            gap: 8px;
            margin-top: 6px;
        }}
        
        .legend-btn {{
            padding: 4px 10px;
            font-size: 12px;
            border: 1px solid #ced4da;
            border-radius: 4px;
            background: #f8f9fa;
            color: #495057;
            cursor: pointer;
        }}
        .legend-btn:hover {{
            background: #e9ecef;
        }}

        .task-status-text {{
            min-width: 150px;
            font-size: 12px;
            color: #6c757d;
        }}

        .footer {{
            padding: 16px 24px;
            text-align: right;
            color: #6c757d;
            font-size: 12px;
            border-top: 1px solid #e9ecef;
        }}
        
        .loading {{
            text-align: center;
            padding: 50px;
            color: #6c757d;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <p id="time-info-header">
                数据更新时间: <span id="update-time-header"></span>
                <span id="realtime-info-header" style="display: none;">
                    &nbsp;&nbsp;实时: <span id="realtime-time-header"></span>
                </span>
            </p>
        </div>
        <!-- 顶部工具栏：调用 Flask 后端 API，执行数据下载 / 指数计算 / 更新排名 -->
        <div style="display:flex;flex-direction:column;gap:4px;padding:8px 24px 4px 24px;font-size:12px;background:#ffffff;">
            <div style="display:flex;align-items:center;gap:8px;">
                <span style="color:#6c757d;">数据操作：</span>
                <button style="padding:3px 10px;font-size:12px;border:1px solid #ced4da;border-radius:4px;background:#f8f9fa;color:#495057;cursor:pointer;" onclick="runTask('download')">下载数据</button>
                <button style="padding:3px 10px;font-size:12px;border:1px solid #ced4da;border-radius:4px;background:#f8f9fa;color:#495057;cursor:pointer;" onclick="runTask('calculate')">计算指数</button>
                <button style="padding:3px 10px;font-size:12px;border:1px solid #ced4da;border-radius:4px;background:#f8f9fa;color:#495057;cursor:pointer;" onclick="runTask('update')">更新排名</button>
                <span id="task-status" style="margin-left:12px;color:#6c757d;"></span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;color:#495057;">
                <label style="display:flex;align-items:center;gap:4px;cursor:pointer;">
                    <input type="checkbox" id="auto-update-toggle" style="cursor:pointer;">
                    <span>自动更新排名</span>
                </label>
                <div id="auto-update-settings" style="display:none;align-items:center;gap:6px;">
                    <span>频率(分钟):</span>
                    <input id="auto-update-interval" type="number" min="1" value="5" style="width:60px;padding:2px 4px;font-size:12px;border:1px solid #ced4da;border-radius:4px;">
                    <span>时间范围:</span>
                    <input id="auto-update-start" type="time" step="1" value="09:25:00" style="padding:2px 4px;font-size:12px;border:1px solid #ced4da;border-radius:4px;">
                    <span>至</span>
                    <input id="auto-update-end" type="time" step="1" value="15:00:00" style="padding:2px 4px;font-size:12px;border:1px solid #ced4da;border-radius:4px;">
                    <span id="auto-update-countdown" style="margin-left:8px;color:#0d6efd;"></span>
                </div>
            </div>
        </div>
        
        <!-- Tab 导航：排名趋势 / 板块排名 / 个股排名 -->
        <div class="tabs">
            <button id="tab-btn-trend" class="tab-button active">排名趋势</button>
            <button id="tab-btn-sector" class="tab-button">板块排名</button>
            <button id="tab-btn-stock" class="tab-button">个股排名</button>
        </div>
        
        <!-- Tab 1: 排名趋势（原有多周期图表） -->
        <div id="tab-trend" class="tab-content">
{charts_html}
        </div>
        
        <!-- Tab 2: 板块排名列表（表格） -->
        <div id="tab-sector" class="tab-content hidden">
            <h2 class="chart-title">板块排名列表</h2>
            <div style="margin:4px 0 8px 0;font-size:12px;color:#495057;display:flex;align-items:center;gap:4px;">
                <span>起始日期:</span>
                <input id="sector-start-date-input" type="text" placeholder="例如 20250101" style="width:100px;padding:2px 4px;font-size:12px;border:1px solid #ced4da;border-radius:4px;">
                <button id="sector-start-date-btn" style="padding:3px 8px;font-size:12px;border:1px solid #ced4da;border-radius:4px;background:#f8f9fa;color:#495057;cursor:pointer;">自起点排序</button>
            </div>
            <div class="sector-table-container">
                <table id="sector-ranking-table">
                    <thead>
                        <tr>
                            <th data-col="name">板块名称</th>
                            <th data-col="index_value">当前点位</th>
                            <th data-col="daily_pct">当日涨幅</th>
                            <th data-col="r20">近20日</th>
                            <th data-col="r55">近55日</th>
                            <th data-col="r233">近233日</th>
                            <th data-col="since_start">自起点以来</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- 由 JavaScript 动态填充 -->
                    </tbody>
                </table>
            </div>
            <p style="font-size:12px;color:#6c757d;margin-top:4px;">提示：点击表头可排序，点击板块名称可查看个股（后续实现）。</p>
        </div>
        
        <!-- Tab 3: 个股排名列表（表格） -->
        <div id="tab-stock" class="tab-content hidden">
            <h2 class="chart-title" id="stock-table-title">个股排名列表</h2>
            <div style="margin:4px 0 8px 0;font-size:12px;color:#495057;display:flex;align-items:center;gap:4px;">
                <span>起始日期:</span>
                <input id="stock-start-date-input" type="text" placeholder="例如 20250101" style="width:100px;padding:2px 4px;font-size:12px;border:1px solid #ced4da;border-radius:4px;">
                <button id="stock-start-date-btn" style="padding:3px 8px;font-size:12px;border:1px solid #ced4da;border-radius:4px;background:#f8f9fa;color:#495057;cursor:pointer;">自起点排序</button>
                <span id="stock-current-sector" style="margin-left:8px;color:#6c757d;"></span>
            </div>
            <div class="sector-table-container">
                <table id="stock-ranking-table">
                    <thead>
                        <tr>
                            <th data-col="symbol">代码</th>
                            <th data-col="name">名称</th>
                            <th data-col="index_value">当前价格</th>
                            <th data-col="daily_pct">当日涨幅</th>
                            <th data-col="r20">近20日</th>
                            <th data-col="r55">近55日</th>
                            <th data-col="r233">近233日</th>
                            <th data-col="since_start">自起点以来</th>
                        </tr>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
            </div>
            <p style="font-size:12px;color:#6c757d;margin-top:4px;">提示：点击表头可排序。</p>
        </div>
        
        <div class="footer">
            <span style="font-size:12px;color:#6c757d;">DWAD 股池指数分析系统</span>
        </div>
    </div>
    
    <script>
        function setTaskStatus(message) {{
            const el = document.getElementById('task-status');
            if (el) {{
                el.textContent = message;
            }}
        }}
        
        function formatTime() {{
            const now = new Date();
            return now.toLocaleString('zh-CN', {{
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            }});
        }}

        // 通用任务调用函数：调用 Flask 后端 API，并在按钮旁边展示执行进度
        async function runTask(task) {{
            let apiPath = '';
            let taskName = '';

            if (task === 'download') {{
                apiPath = '/api/download_data';
                taskName = '下载数据';
                setTaskStatus(taskName + '(进行中...)');
            }} else if (task === 'calculate') {{
                apiPath = '/api/calculate_index';
                taskName = '计算指数';
                setTaskStatus(taskName + '(进行中...)');
            }} else if (task === 'update') {{
                apiPath = '/api/update_ranking';
                taskName = '更新排名';
                setTaskStatus(taskName + '(进行中...)');
            }} else {{
                return;
            }}

            const isUpdate = apiPath === '/api/update_ranking';

            try {{
                const resp = await fetch(apiPath, {{ method: 'POST' }});
                const data = await resp.json().catch(() => ({{ ok: false, error: '响应解析失败' }}));
                if (data && data.ok) {{
                    const completedTime = formatTime();
                    if (task === 'download') {{
                        // 下载任务：再查询一次后台日志，获取成功/总数信息
                        try {{
                            const sResp = await fetch('/api/download_status');
                            const sData = await sResp.json().catch(() => null);
                            if (sData && sData.ok && sData.latest_update) {{
                                const latest = sData.latest_update;
                                const total = latest.total_stocks || latest.total || 0;
                                const success = latest.success_count || 0;
                                if (total > 0) {{
                                    setTaskStatus(taskName + '已完成(' + success + '/' + total + ')：' + completedTime);
                                }} else {{
                                    setTaskStatus(taskName + '已完成：' + completedTime);
                                }}
                            }} else {{
                                setTaskStatus(taskName + '已完成：' + completedTime);
                            }}
                        }} catch (e) {{
                            console.error('获取下载状态失败', e);
                            setTaskStatus(taskName + '已完成：' + completedTime);
                        }}
                    }} else if (task === 'calculate') {{
                        setTaskStatus(taskName + '已完成：' + completedTime);
                    }} else if (isUpdate) {{
                        const statusMsg = taskName + '已完成：' + completedTime;
                        setTaskStatus(statusMsg + '，正在刷新页面...');
                        // 保存状态到 localStorage，刷新后恢复
                        localStorage.setItem('lastTaskStatus', statusMsg);
                        localStorage.setItem('lastTaskTime', Date.now().toString());
                        setTimeout(() => window.location.reload(), 800);
                        return;
                    }}
                }} else {{
                    setTaskStatus(taskName + '执行失败，请查看日志');
                    console.error('任务执行失败', apiPath, data && data.error);
                }}
            }} catch (err) {{
                setTaskStatus(taskName + '调用接口出错，请稍后重试');
                console.error('调用 API 出错', apiPath, err);
            }}
        }}

        // 页面加载时恢复上次任务状态（如果是刚刚刷新的）
        const lastStatus = localStorage.getItem('lastTaskStatus');
        const lastTime = localStorage.getItem('lastTaskTime');
        if (lastStatus && lastTime) {{
            const elapsed = Date.now() - parseInt(lastTime);
            // 如果是 5 秒内刷新的，显示上次状态
            if (elapsed < 5000) {{
                setTaskStatus(lastStatus);
            }}
            // 清除保存的状态
            localStorage.removeItem('lastTaskStatus');
            localStorage.removeItem('lastTaskTime');
        }}

        // 设置当前时间（头部小字）
        const now = new Date();
        const nowText = now.toLocaleString('zh-CN');
        const updateTimeHeaderMulti = document.getElementById('update-time-header');
        if (updateTimeHeaderMulti) {{
            updateTimeHeaderMulti.textContent = nowText;
        }}
        
        // 显示实时数据时间（仅更新头部的小字）
        const realtimeTimestamp = {'null' if realtime_timestamp is None else f'"{realtime_timestamp}"'};
        if (realtimeTimestamp && realtimeTimestamp !== 'null') {{
            const realtimeDate = new Date(realtimeTimestamp);
            const realtimeText = realtimeDate.toLocaleString('zh-CN', {{
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            }});
            const headerRealtimeInfoMulti = document.getElementById('realtime-info-header');
            const headerRealtimeTimeMulti = document.getElementById('realtime-time-header');
            if (headerRealtimeInfoMulti && headerRealtimeTimeMulti) {{
                headerRealtimeInfoMulti.style.display = 'inline';
                headerRealtimeTimeMulti.textContent = realtimeText;
            }}
        }}
        
        function hideAllTraces(chartId) {{
            const gd = document.getElementById(chartId);
            if (!gd || !gd.data) return;
            const indices = gd.data.map((_, i) => i);
            const vis = gd.data.map(tr => (tr && tr.showlegend === false ? false : 'legendonly'));
            Plotly.restyle(chartId, {{ visible: vis }}, indices);
        }}
        
        function showAllTraces(chartId) {{
            const gd = document.getElementById(chartId);
            if (!gd || !gd.data) return;
            const indices = gd.data.map((_, i) => i);
            const vis = gd.data.map(() => true);
            Plotly.restyle(chartId, {{ visible: vis }}, indices);
        }}

        // ===========================
        // 板块排名 Tab & 表格逻辑
        // ===========================

        const SECTOR_START_DATE_KEY = 'dwad_sector_start_date';
        const STOCK_START_DATE_KEY = 'dwad_stock_start_date';

        let sectorData = [];
        let sectorLoaded = false;
        let sectorSortCol = 'r20';   // 默认按近20日排序
        let sectorSortAsc = false;   // 默认降序（涨幅高在前）
        let sectorSinceStartLabel = '自起点以来';

        function updateSinceStartHeader(label) {{
            const th = document.querySelector('#sector-ranking-table th[data-col="since_start"]');
            if (!th) return;
            if (label && label.trim()) {{
                th.textContent = label.trim();
            }} else {{
                th.textContent = sectorSinceStartLabel;
            }}
        }}

        // 个股排名 Tab 状态
        let stockData = [];
        let stockLoaded = false;
        let stockSortCol = 'r20';
        let stockSortAsc = false;
        let stockSinceStartLabel = '自起点以来';
        let currentStockSector = '';

        function updateStockSinceStartHeader(label) {{
            const th = document.querySelector('#stock-ranking-table th[data-col="since_start"]');
            if (!th) return;
            if (label && label.trim()) {{
                th.textContent = label.trim();
            }} else {{
                th.textContent = stockSinceStartLabel;
            }}
        }}

        function formatPct(v) {{
            if (v === null || v === undefined) return '--';
            const num = Number(v);
            if (!Number.isFinite(num)) return '--';
            const pct = (num * 100).toFixed(2);
            return (num >= 0 ? '+' : '') + pct + '%';
        }}

        function formatNumber(v, digits = 2) {{
            if (v === null || v === undefined) return '--';
            const num = Number(v);
            if (!Number.isFinite(num)) return '--';
            return num.toFixed(digits);
        }}

        function renderSectorTable() {{
            const tbody = document.querySelector('#sector-ranking-table tbody');
            if (!tbody) return;
            tbody.innerHTML = '';

            sectorData.forEach((row) => {{
                const tr = document.createElement('tr');

                // 板块名称
                const tdName = document.createElement('td');
                tdName.textContent = row.name || '';
                tdName.style.cursor = 'pointer';
                // 点击板块名称 -> 进入个股排名 Tab
                tdName.addEventListener('click', () => {{
                    openStockTabForSector(row.name);
                }});
                tr.appendChild(tdName);

                // 当前点位
                const tdIndex = document.createElement('td');
                tdIndex.textContent = formatNumber(row.index_value, 2);
                tr.appendChild(tdIndex);

                // 当日涨幅
                const tdDaily = document.createElement('td');
                tdDaily.textContent = formatPct(row.daily_pct);
                tr.appendChild(tdDaily);

                // 近20日 / 55日 / 233日 / 自起点
                const td20 = document.createElement('td');
                td20.textContent = formatPct(row.r20);
                tr.appendChild(td20);

                const td55 = document.createElement('td');
                td55.textContent = formatPct(row.r55);
                tr.appendChild(td55);

                const td233 = document.createElement('td');
                td233.textContent = formatPct(row.r233);
                tr.appendChild(td233);

                const tdSince = document.createElement('td');
                tdSince.textContent = formatPct(row.since_start);
                tr.appendChild(tdSince);

                tbody.appendChild(tr);
            }});
        }}

        function sortSectorData(col, asc) {{
            sectorData.sort((a, b) => {{
                const va = a[col];
                const vb = b[col];

                if (col === 'name') {{
                    const sa = (va || '').toString();
                    const sb = (vb || '').toString();
                    return asc ? sa.localeCompare(sb, 'zh-CN') : sb.localeCompare(sa, 'zh-CN');
                }}

                const na = Number(va);
                const nb = Number(vb);
                const fa = Number.isFinite(na);
                const fb = Number.isFinite(nb);
                if (!fa && !fb) return 0;
                if (!fa) return 1;   // 空值排在后面
                if (!fb) return -1;
                return asc ? na - nb : nb - na;
            }});
        }}

        async function loadSectorRankingIfNeeded() {{
            if (sectorLoaded) {{
                renderSectorTable();
                return;
            }}
            try {{
                const resp = await fetch('/api/sector_ranking');
                const data = await resp.json();
                if (Array.isArray(data)) {{
                    sectorData = data;
                    sectorLoaded = true;
                    sortSectorData(sectorSortCol, sectorSortAsc);
                    renderSectorTable();
                }} else {{
                    console.error('板块排名数据格式错误', data);
                }}
            }} catch (err) {{
                console.error('获取板块排名失败', err);
            }}
        }}

        function renderStockTable() {{
            const tbody = document.querySelector('#stock-ranking-table tbody');
            if (!tbody) return;
            tbody.innerHTML = '';

            stockData.forEach((row) => {{
                const tr = document.createElement('tr');

                const tdSymbol = document.createElement('td');
                tdSymbol.textContent = row.symbol || '';
                tr.appendChild(tdSymbol);

                const tdName = document.createElement('td');
                tdName.textContent = row.name || '';
                tr.appendChild(tdName);

                const tdIndex = document.createElement('td');
                tdIndex.textContent = formatNumber(row.index_value, 2);
                tr.appendChild(tdIndex);

                const tdDaily = document.createElement('td');
                tdDaily.textContent = formatPct(row.daily_pct);
                tr.appendChild(tdDaily);

                const td20 = document.createElement('td');
                td20.textContent = formatPct(row.r20);
                tr.appendChild(td20);

                const td55 = document.createElement('td');
                td55.textContent = formatPct(row.r55);
                tr.appendChild(td55);

                const td233 = document.createElement('td');
                td233.textContent = formatPct(row.r233);
                tr.appendChild(td233);

                const tdSince = document.createElement('td');
                tdSince.textContent = formatPct(row.since_start);
                tr.appendChild(tdSince);

                tbody.appendChild(tr);
            }});
        }}

        function sortStockData(col, asc) {{
            stockData.sort((a, b) => {{
                const va = a[col];
                const vb = b[col];

                if (col === 'name' || col === 'symbol') {{
                    const sa = (va || '').toString();
                    const sb = (vb || '').toString();
                    return asc ? sa.localeCompare(sb, 'zh-CN') : sb.localeCompare(sa, 'zh-CN');
                }}

                const na = Number(va);
                const nb = Number(vb);
                const fa = Number.isFinite(na);
                const fb = Number.isFinite(nb);
                if (!fa && !fb) return 0;
                if (!fa) return 1;
                if (!fb) return -1;
                return asc ? na - nb : nb - na;
            }});
        }}

        async function loadStockRanking(sectorName, startDate) {{
            if (!sectorName) return;
            const titleEl = document.getElementById('stock-table-title');
            const sectorEl = document.getElementById('stock-current-sector');
            if (titleEl) {{
                titleEl.textContent = `个股排名列表 - ${{sectorName}}`;
            }}
            if (sectorEl) {{
                sectorEl.textContent = `当前板块: ${{sectorName}}`;
            }}

            const payload = {{ sector_name: sectorName }};
            if (startDate) {{
                payload.start_date = startDate;
            }}

            try {{
                const resp = await fetch('/api/sector_stock_ranking', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify(payload)
                }});
                const data = await resp.json().catch(() => null);
                if (!resp.ok || !Array.isArray(data)) {{
                    console.error('获取个股排名失败', data);
                    alert((data && data.error) || '获取个股排名失败，请检查后台日志');
                    return;
                }}
                stockData = data;
                stockLoaded = true;
                stockSortCol = 'since_start';
                stockSortAsc = false;
                sortStockData(stockSortCol, stockSortAsc);
                renderStockTable();
            }} catch (err) {{
                console.error('调用个股排名接口失败', err);
                alert('获取个股排名失败，请稍后重试');
            }}
        }}

        function openStockTabForSector(sectorName) {{
            const btnTrend = document.getElementById('tab-btn-trend');
            const btnSector = document.getElementById('tab-btn-sector');
            const btnStock = document.getElementById('tab-btn-stock');
            const tabTrend = document.getElementById('tab-trend');
            const tabSector = document.getElementById('tab-sector');
            const tabStock = document.getElementById('tab-stock');
            if (!btnStock || !tabStock || !btnTrend || !btnSector || !tabTrend || !tabSector) return;

            currentStockSector = sectorName;

            btnTrend.classList.remove('active');
            btnSector.classList.remove('active');
            btnStock.classList.add('active');
            tabTrend.classList.add('hidden');
            tabSector.classList.add('hidden');
            tabStock.classList.remove('hidden');

            let startDate = null;
            try {{
                const savedStock = localStorage.getItem(STOCK_START_DATE_KEY);
                if (savedStock && savedStock.trim()) {{
                    let raw = savedStock.trim();
                    if (/^\d{{8}}$/.test(raw)) {{
                        startDate = raw.slice(0, 4) + '-' + raw.slice(4, 6) + '-' + raw.slice(6, 8);
                    }} else {{
                        startDate = raw;
                    }}
                    updateStockSinceStartHeader(raw);
                }}
            }} catch (e) {{
                console.error('读取个股起点日期缓存失败', e);
            }}

            loadStockRanking(sectorName, startDate);
        }}

        function initTabsAndSectorTable() {{
            const btnTrend = document.getElementById('tab-btn-trend');
            const btnSector = document.getElementById('tab-btn-sector');
            const btnStock = document.getElementById('tab-btn-stock');
            const tabTrend = document.getElementById('tab-trend');
            const tabSector = document.getElementById('tab-sector');
            const tabStock = document.getElementById('tab-stock');
            const startInput = document.getElementById('sector-start-date-input');
            const startBtn = document.getElementById('sector-start-date-btn');
            const stockStartInput = document.getElementById('stock-start-date-input');
            const stockStartBtn = document.getElementById('stock-start-date-btn');

            if (startInput) {{
                try {{
                    const saved = localStorage.getItem(SECTOR_START_DATE_KEY);
                    if (saved && saved.trim()) {{
                        startInput.value = saved;
                        updateSinceStartHeader(saved);
                    }}
                }} catch (e) {{
                    console.error('读取板块起点日期缓存失败', e);
                }}
            }}

            if (stockStartInput) {{
                try {{
                    const savedStock = localStorage.getItem(STOCK_START_DATE_KEY);
                    if (savedStock && savedStock.trim()) {{
                        stockStartInput.value = savedStock;
                        updateStockSinceStartHeader(savedStock);
                    }}
                }} catch (e) {{
                    console.error('读取个股起点日期缓存失败', e);
                }}
            }}

            if (!btnTrend || !btnSector || !tabTrend || !tabSector || !btnStock || !tabStock) return;

            btnTrend.addEventListener('click', () => {{
                btnTrend.classList.add('active');
                btnSector.classList.remove('active');
                tabTrend.classList.remove('hidden');
                tabSector.classList.add('hidden');
            }});

            btnSector.addEventListener('click', () => {{
                btnSector.classList.add('active');
                btnTrend.classList.remove('active');
                if (btnStock) btnStock.classList.remove('active');
                tabSector.classList.remove('hidden');
                tabTrend.classList.add('hidden');
                tabStock.classList.add('hidden');

                let saved = null;
                try {{
                    saved = localStorage.getItem(SECTOR_START_DATE_KEY);
                }} catch (e) {{
                    console.error('读取板块起点日期缓存失败', e);
                }}

                const raw = saved && saved.trim() ? saved.trim() : '';
                if (raw && startBtn && startInput) {{
                    startInput.value = raw;
                    updateSinceStartHeader(raw);
                    startBtn.click();
                }} else {{
                    sectorLoaded = false;
                    sectorSinceStartLabel = '自起点以来';
                    updateSinceStartHeader('');
                    loadSectorRankingIfNeeded();
                }}
            }});

            if (btnStock) {{
                btnStock.addEventListener('click', () => {{
                    btnStock.classList.add('active');
                    btnTrend.classList.remove('active');
                    btnSector.classList.remove('active');
                    tabStock.classList.remove('hidden');
                    tabTrend.classList.add('hidden');
                    tabSector.classList.add('hidden');
                    if (currentStockSector) {{
                        let startDate = null;
                        try {{
                            const savedStock = localStorage.getItem(STOCK_START_DATE_KEY);
                            if (savedStock && savedStock.trim()) {{
                                let raw = savedStock.trim();
                                if (/^\d{{8}}$/.test(raw)) {{
                                    startDate = raw.slice(0, 4) + '-' + raw.slice(4, 6) + '-' + raw.slice(6, 8);
                                }} else {{
                                    startDate = raw;
                                }}
                                updateStockSinceStartHeader(raw);
                            }}
                        }} catch (e) {{
                            console.error('读取个股起点日期缓存失败', e);
                        }}
                        loadStockRanking(currentStockSector, startDate);
                    }}
                }});
            }}

            if (startBtn && startInput) {{
                startBtn.addEventListener('click', async () => {{
                    const raw = (startInput.value || '').trim();
                    if (!raw) {{
                        alert('请输入起始日期，例如 20250101');
                        return;
                    }}
                    let startDate = raw;
                    if (/^\d{{8}}$/.test(raw)) {{
                        startDate = raw.slice(0, 4) + '-' + raw.slice(4, 6) + '-' + raw.slice(6, 8);
                    }}
                    try {{
                        localStorage.setItem(SECTOR_START_DATE_KEY, raw);
                    }} catch (e) {{
                        console.error('保存板块起点日期缓存失败', e);
                    }}
                    try {{
                        const resp = await fetch('/api/sector_ranking_from_date', {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify({{ start_date: startDate }})
                        }});
                        const data = await resp.json().catch(() => null);
                        if (!resp.ok || !Array.isArray(data)) {{
                            console.error('自起点排序失败', data);
                            alert((data && data.error) || '自起点排序失败，请检查后台日志');
                            return;
                        }}
                        sectorData = data;
                        sectorLoaded = true;
                        sectorSortCol = 'since_start';
                        sectorSortAsc = false;
                        updateSinceStartHeader(raw);
                        sortSectorData(sectorSortCol, sectorSortAsc);
                        renderSectorTable();
                    }} catch (err) {{
                        console.error('调用自起点排序接口失败', err);
                        alert('自起点排序失败，请稍后重试');
                    }}
                }});
            }}

            if (stockStartBtn && stockStartInput) {{
                stockStartBtn.addEventListener('click', async () => {{
                    if (!currentStockSector) {{
                        alert('请先在“板块排名”中选择一个板块');
                        return;
                    }}
                    const raw = (stockStartInput.value || '').trim();
                    if (!raw) {{
                        alert('请输入起始日期，例如 20250101');
                        return;
                    }}
                    let startDate = raw;
                    if (/^\d{{8}}$/.test(raw)) {{
                        startDate = raw.slice(0, 4) + '-' + raw.slice(4, 6) + '-' + raw.slice(6, 8);
                    }}
                    try {{
                        localStorage.setItem(STOCK_START_DATE_KEY, raw);
                    }} catch (e) {{
                        console.error('保存个股起点日期缓存失败', e);
                    }}
                    updateStockSinceStartHeader(raw);
                    await loadStockRanking(currentStockSector, startDate);
                }});
            }}

            // 表头点击排序
            const headers = document.querySelectorAll('#sector-ranking-table th[data-col]');
            headers.forEach((th) => {{
                th.addEventListener('click', () => {{
                    const col = th.getAttribute('data-col');
                    if (!col) return;
                    if (sectorSortCol === col) {{
                        sectorSortAsc = !sectorSortAsc;
                    }} else {{
                        sectorSortCol = col;
                        // 默认数值列降序，名称列升序
                        sectorSortAsc = (col === 'name');
                    }}
                    sortSectorData(sectorSortCol, sectorSortAsc);
                    renderSectorTable();
                }});
            }});

            const stockHeaders = document.querySelectorAll('#stock-ranking-table th[data-col]');
            stockHeaders.forEach((th) => {{
                th.addEventListener('click', () => {{
                    const col = th.getAttribute('data-col');
                    if (!col) return;
                    if (stockSortCol === col) {{
                        stockSortAsc = !stockSortAsc;
                    }} else {{
                        stockSortCol = col;
                        stockSortAsc = (col === 'name' || col === 'symbol');
                    }}
                    sortStockData(stockSortCol, stockSortAsc);
                    renderStockTable();
                }});
            }});
        }}

        // 通用图表渲染函数
        function renderSingleChart(chartId, traces, dates, title, totalIndices) {{
            // 为每条线生成标签注释
            const annotations = [];
            
            // 先收集每个指数的历史trace和实时trace
            const tracesByName = {{}};
            traces.forEach((trace) => {{
                if (trace.showlegend !== false && !trace.is_realtime) {{
                    // 这是历史数据trace
                    tracesByName[trace.name] = {{
                        historical: trace,
                        realtime: null
                    }};
                }} else if (trace.is_realtime) {{
                    // 这是实时数据trace
                    if (!tracesByName[trace.name]) {{
                        tracesByName[trace.name] = {{
                            historical: null,
                            realtime: trace
                        }};
                    }} else {{
                        tracesByName[trace.name].realtime = trace;
                    }}
                }}
            }});
            
            // 为每个指数生成标注
            Object.keys(tracesByName).forEach((name) => {{
                const data = tracesByName[name];
                const trace = data.historical;
                
                if (!trace) return;
                
                // 1. 在折线的多个位置放置标签（保留原有功能）
                for (let i = 1; i <= 4; i++) {{
                    const pointIdx = Math.floor(trace.x.length * i / 5);
                    if (pointIdx < trace.x.length) {{
                        annotations.push({{
                            x: trace.x[pointIdx],
                            y: trace.y[pointIdx],
                            xref: 'x',
                            yref: 'y',
                            text: trace.name,
                            showarrow: false,
                            font: {{
                                size: 9,
                                color: trace.line.color
                            }},
                            bgcolor: 'rgba(255, 255, 255, 0.7)',
                            borderpad: 2,
                            opacity: 0.8
                        }});
                    }}
                }}
                
                // 2. 在终点右侧添加带涨幅的标注
                //    左侧为周期涨幅（近N日），右侧为当日实时涨幅
                //    如果没有实时数据，当日涨幅显示为 "--"
                let labelX, labelY;
                let periodChangeValue = null;   // 近N日涨跌幅
                let todayChangeValue = null;    // 当日实时涨跌幅

                if (data.realtime) {{
                    // 使用实时数据点
                    labelX = data.realtime.x[data.realtime.x.length - 1];
                    labelY = data.realtime.y[data.realtime.y.length - 1];
                    periodChangeValue = data.realtime.realtime_change;
                    if (typeof data.realtime.realtime_today_change === 'number') {{
                        todayChangeValue = data.realtime.realtime_today_change;
                    }}
                }} else if (trace.customdata && trace.customdata.length > 0) {{
                    // 仅使用历史数据最后一点，只有周期涨幅
                    const lastIdx = trace.x.length - 1;
                    labelX = trace.x[lastIdx];
                    labelY = trace.y[lastIdx];
                    periodChangeValue = trace.customdata[lastIdx][1];
                }} else {{
                    return;  // 没有数据，跳过
                }}

                let periodText;
                if (periodChangeValue !== null && periodChangeValue !== undefined) {{
                    const v = periodChangeValue;
                    const sign = v >= 0 ? '+' : '';
                    periodText = sign + v.toFixed(2) + '%';
                }} else {{
                    periodText = '--';
                }}

                let todayText;
                if (todayChangeValue !== null && todayChangeValue !== undefined) {{
                    const v2 = todayChangeValue;
                    const sign2 = v2 >= 0 ? '+' : '';
                    todayText = sign2 + v2.toFixed(2) + '%';
                }} else {{
                    todayText = '--';
                }}

                let labelText;
                // 去掉文字“近N日”，但保留两段涨幅数值，并始终显示“当日”一栏（无实时数据时为"--"）
                labelText = name + ' ' + periodText + ' | 当日: ' + todayText;

                annotations.push({{
                    x: labelX,
                    y: labelY,
                    xref: 'x',
                    yref: 'y',
                    text: labelText,
                    xanchor: 'left',
                    yanchor: 'middle',
                    showarrow: false,
                    font: {{
                        size: 10,
                        color: trace.line.color,
                        weight: 'bold'
                    }},
                    xshift: 5,
                    bgcolor: 'rgba(255, 255, 255, 0.9)',
                    borderpad: 3
                }});
            }});
            
            const layout = {{
                title: {{
                    text: '',
                    font: {{
                        size: 18,
                        color: '#212529'
                    }}
                }},
                xaxis: {{
                    title: {{
                        text: '日期',
                        font: {{
                            size: 12,
                            color: '#495057'
                        }}
                    }},
                    showgrid: {str(show_grid).lower()},
                    gridcolor: '#e9ecef',
                    tickangle: -45,
                    tickmode: 'array',
                    tickvals: (() => {{
                        // 自动选择合适的刻度间隔
                        const total = dates.length;
                        const maxTicks = 15;  // 最多显示15个刻度
                        const step = Math.ceil(total / maxTicks);
                        const vals = [];
                        for (let i = 0; i < total; i += step) {{
                            vals.push(i);
                        }}
                        // 确保包含最后一个点
                        if (vals[vals.length - 1] !== total - 1) {{
                            vals.push(total - 1);
                        }}
                        return vals;
                    }})(),
                    ticktext: (() => {{
                        const total = dates.length;
                        const maxTicks = 15;
                        const step = Math.ceil(total / maxTicks);
                        const texts = [];
                        for (let i = 0; i < total; i += step) {{
                            texts.push(dates[i]);
                        }}
                        // 确保包含最后一个日期
                        if (texts.length === 0 || dates[texts.length - 1] !== dates[total - 1]) {{
                            texts.push(dates[total - 1]);
                        }}
                        return texts;
                    }})()
                }},
                yaxis: {{
                    title: {{
                        text: '排名',
                        font: {{
                            size: 12,
                            color: '#495057'
                        }}
                    }},
                    showgrid: {str(show_grid).lower()},
                    gridcolor: '#e9ecef',
                    tickmode: 'linear',
                    tick0: 1,
                    dtick: 1,
                    range: [totalIndices + 0.5, 0.5]
                }},
                annotations: annotations,
                hovermode: 'closest',
                showlegend: true,
                legend: {{
                    orientation: 'h',
                    x: 0.5,
                    y: -0.15,
                    xanchor: 'center',
                    yanchor: 'top',
                    bgcolor: 'rgba(255, 255, 255, 0.9)',
                    bordercolor: '#e9ecef',
                    borderwidth: 1
                }},
                margin: {{
                    l: 40,
                    r: 80,
                    t: 30,
                    b: 80
                }},
                autosize: true,
                plot_bgcolor: '#ffffff',
                paper_bgcolor: '#ffffff',
                font: {{
                    family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
                    size: 11,
                    color: '#495057'
                }}
            }};
            
            const config = {{
                responsive: true,
                displayModeBar: true,
                displaylogo: false,
                modeBarButtonsToRemove: ['lasso2d', 'select2d'],
                toImageButtonOptions: {{
                    format: 'png',
                    filename: chartId,
                    scale: 2
                }}
            }};
            
            // 使用Plotly.react以支持响应式调整
            Plotly.react(chartId, traces, layout, config)
                .then(() => {{
                    console.log('✅ 图表加载完成:', chartId);
                    
                    // 添加点击事件：点击线条时加粗并显示数据点
                    const chartElement = document.getElementById(chartId);
                    const clickedTraces = new Set();  // 记录哪些线被点击了
                    const realtimeTraceMap = new Map();  // 记录历史trace与实时trace的对应关系

                    traces.forEach((trace, index) => {{
                        if (trace.is_realtime) {{
                            const group = trace.legendgroup;
                            if (group) {{
                                if (!realtimeTraceMap.has(group)) {{
                                    realtimeTraceMap.set(group, []);
                                }}
                                realtimeTraceMap.get(group).push(index);
                            }}
                        }}
                    }});
                    
                    chartElement.on('plotly_click', function(data) {{
                        const pointData = data.points[0];
                        const traceIndex = pointData.curveNumber;
                        
                        // 切换该线的状态
                        const traceName = traces[traceIndex] ? traces[traceIndex].legendgroup || traces[traceIndex].name : null;
                        const relatedRealtime = traceName && realtimeTraceMap.has(traceName) ? realtimeTraceMap.get(traceName) : [];
                        const indicesToUpdate = [traceIndex, ...relatedRealtime];

                        if (clickedTraces.has(traceIndex)) {{
                            // 已被点击过，恢复原状
                            clickedTraces.delete(traceIndex);
                            Plotly.restyle(chartId, {{
                                'mode': 'lines',
                                'line.width': {line_width}
                            }}, indicesToUpdate);
                        }} else {{
                            // 未被点击，加粗并显示数据点
                            clickedTraces.add(traceIndex);
                            Plotly.restyle(chartId, {{
                                'mode': 'lines+markers',
                                'line.width': {line_width * 2},
                                'marker.size': 6
                            }}, indicesToUpdate);
                        }}
                    }});
                    
                    // 监听图例点击：同步切换实时trace的可见性
                    chartElement.on('plotly_legendclick', function(ev) {{
                        const traceIndex = ev.curveNumber;
                        const gd = document.getElementById(chartId);
                        if (!gd || !gd.data || gd.data.length <= traceIndex) {{
                            return false; // 阻止默认行为
                        }}
                        const curVis = gd.data[traceIndex].visible; // true | 'legendonly' | false | undefined
                        let newVis;
                        if (curVis === 'legendonly' || curVis === false) {{
                            newVis = true;
                        }} else {{
                            newVis = 'legendonly';
                        }}
                        const groupName = gd.data[traceIndex].legendgroup || gd.data[traceIndex].name;
                        const relatedRealtime = groupName && realtimeTraceMap.has(groupName) ? realtimeTraceMap.get(groupName) : [];
                        const rtVis = (newVis === true) ? true : false;
                        const ops = [Plotly.restyle(chartId, {{ visible: newVis }}, [traceIndex])];
                        if (relatedRealtime.length > 0) {{
                            ops.push(Plotly.restyle(chartId, {{ visible: rtVis }}, relatedRealtime));
                        }}
                        Promise.all(ops);

                        return false; // 自定义切换后，阻止默认切换，避免状态冲突
                    }});
                    
                    // 提示用户可以点击
                    console.log('💡 提示: 点击线条可以加粗并显示数据点，再次点击可恢复');
                }})
                .catch((err) => {{
                    console.error('❌ 图表渲染失败:', chartId, err);
                    document.getElementById(chartId).innerHTML = 
                        '<div style="color: #dc3545; padding: 50px; text-align: center;">' +
                        '<h3>❌ 图表渲染失败</h3>' +
                        '<p style="margin-top: 10px;">错误信息: ' + err.message + '</p>' +
                        '</div>';
                }});
        }}
        
        // 检查Plotly是否加载成功并渲染所有图表
        function renderAllCharts() {{
            if (typeof Plotly === 'undefined') {{
                console.error('Plotly库未加载');
                return;
            }}
            
{charts_script}

            // 图表渲染完成后初始化 Tab 和板块排名表格逻辑
            try {{
                initTabsAndSectorTable();
            }} catch (err) {{
                console.error('初始化 Tab/板块表格失败', err);
            }}

            try {{
                initAutoUpdateControls();
            }} catch (err) {{
                console.error('初始化自动更新控件失败', err);
            }}
        }}

        const AUTO_UPDATE_CONFIG_KEY = 'dwad_auto_update_config';
        let autoUpdateTimerId = null;
        let autoUpdateCountdownTimerId = null;
        let nextAutoUpdateTime = null;

        function saveAutoUpdateConfig(config) {{
            try {{
                localStorage.setItem(AUTO_UPDATE_CONFIG_KEY, JSON.stringify(config));
            }} catch (e) {{
                console.error('保存自动更新配置失败', e);
            }}
        }}

        function loadAutoUpdateConfig() {{
            try {{
                const raw = localStorage.getItem(AUTO_UPDATE_CONFIG_KEY);
                if (!raw) return null;
                return JSON.parse(raw);
            }} catch (e) {{
                console.error('读取自动更新配置失败', e);
                return null;
            }}
        }}

        function parseTimeToSeconds(text) {{
            if (!text) return null;
            const parts = text.split(':');
            if (parts.length < 2) return null;
            const h = parseInt(parts[0], 10) || 0;
            const m = parseInt(parts[1], 10) || 0;
            const s = parts.length >= 3 ? (parseInt(parts[2], 10) || 0) : 0;
            return h * 3600 + m * 60 + s;
        }}

        function clearAutoUpdateTimers() {{
            if (autoUpdateTimerId) {{
                clearTimeout(autoUpdateTimerId);
                autoUpdateTimerId = null;
            }}
            if (autoUpdateCountdownTimerId) {{
                clearInterval(autoUpdateCountdownTimerId);
                autoUpdateCountdownTimerId = null;
            }}
        }}

        function updateAutoUpdateCountdown() {{
            const el = document.getElementById('auto-update-countdown');
            if (!el) return;
            if (!nextAutoUpdateTime) {{
                el.textContent = '';
                return;
            }}
            const now = new Date();
            const diffMs = nextAutoUpdateTime.getTime() - now.getTime();
            if (diffMs <= 0) {{
                el.textContent = '即将自动更新...';
                return;
            }}
            const totalSeconds = Math.floor(diffMs / 1000);
            const minutes = Math.floor(totalSeconds / 60);
            const seconds = totalSeconds % 60;
            const mm = String(minutes).padStart(2, '0');
            const ss = String(seconds).padStart(2, '0');
            el.textContent = '下次自动更新倒计时: ' + mm + ':' + ss;
        }}

        function scheduleNextAutoUpdate() {{
            clearAutoUpdateTimers();
            const toggle = document.getElementById('auto-update-toggle');
            const intervalInput = document.getElementById('auto-update-interval');
            const startInput = document.getElementById('auto-update-start');
            const endInput = document.getElementById('auto-update-end');
            if (!toggle || !intervalInput || !startInput || !endInput) {{
                return;
            }}
            if (!toggle.checked) {{
                nextAutoUpdateTime = null;
                updateAutoUpdateCountdown();
                saveAutoUpdateConfig({{ enabled: false, intervalMinutes: Number(intervalInput.value) || 0, startTime: startInput.value, endTime: endInput.value }});
                return;
            }}

            const intervalMinutes = parseInt(intervalInput.value, 10);
            if (!intervalMinutes || intervalMinutes <= 0) {{
                setTaskStatus('请设置大于0的自动更新频率(分钟)');
                toggle.checked = false;
                nextAutoUpdateTime = null;
                updateAutoUpdateCountdown();
                return;
            }}

            const startSeconds = parseTimeToSeconds(startInput.value || '09:25:00');
            const endSeconds = parseTimeToSeconds(endInput.value || '15:00:00');
            if (startSeconds === null || endSeconds === null || startSeconds >= endSeconds) {{
                setTaskStatus('自动更新时间范围不合法');
                toggle.checked = false;
                nextAutoUpdateTime = null;
                updateAutoUpdateCountdown();
                return;
            }}

            const now = new Date();
            const todayStart = new Date(now);
            todayStart.setHours(0, 0, 0, 0);
            const windowStart = new Date(todayStart.getTime() + startSeconds * 1000);
            const windowEnd = new Date(todayStart.getTime() + endSeconds * 1000);

            const intervalMs = intervalMinutes * 60 * 1000;
            let firstRun;

            if (now < windowStart) {{
                firstRun = windowStart;
            }} else if (now >= windowEnd) {{
                firstRun = new Date(windowStart.getTime() + 24 * 60 * 60 * 1000);
            }} else {{
                firstRun = new Date(now.getTime() + intervalMs);
                if (firstRun > windowEnd) {{
                    firstRun = new Date(windowStart.getTime() + 24 * 60 * 60 * 1000);
                }}
            }}

            nextAutoUpdateTime = firstRun;
            updateAutoUpdateCountdown();
            autoUpdateCountdownTimerId = setInterval(updateAutoUpdateCountdown, 1000);

            const delayMs = Math.max(0, firstRun.getTime() - now.getTime());
            autoUpdateTimerId = setTimeout(() => {{
                runTask('update');
            }}, delayMs);

            saveAutoUpdateConfig({{
                enabled: true,
                intervalMinutes: intervalMinutes,
                startTime: startInput.value,
                endTime: endInput.value
            }});
        }}

        function initAutoUpdateControls() {{
            const toggle = document.getElementById('auto-update-toggle');
            const settings = document.getElementById('auto-update-settings');
            const intervalInput = document.getElementById('auto-update-interval');
            const startInput = document.getElementById('auto-update-start');
            const endInput = document.getElementById('auto-update-end');
            if (!toggle || !settings || !intervalInput || !startInput || !endInput) {{
                return;
            }}

            const cfg = loadAutoUpdateConfig();
            if (cfg) {{
                if (typeof cfg.intervalMinutes === 'number' && cfg.intervalMinutes > 0) {{
                    intervalInput.value = cfg.intervalMinutes;
                }}
                if (cfg.startTime) {{
                    startInput.value = cfg.startTime;
                }}
                if (cfg.endTime) {{
                    endInput.value = cfg.endTime;
                }}
                if (cfg.enabled) {{
                    toggle.checked = true;
                    settings.style.display = 'flex';
                    scheduleNextAutoUpdate();
                }}
            }}

            toggle.addEventListener('change', () => {{
                if (toggle.checked) {{
                    settings.style.display = 'flex';
                    scheduleNextAutoUpdate();
                }} else {{
                    settings.style.display = 'none';
                    clearAutoUpdateTimers();
                    nextAutoUpdateTime = null;
                    updateAutoUpdateCountdown();
                    saveAutoUpdateConfig({{
                        enabled: false,
                        intervalMinutes: Number(intervalInput.value) || 0,
                        startTime: startInput.value,
                        endTime: endInput.value
                    }});
                }}
            }});

            [intervalInput, startInput, endInput].forEach((el) => {{
                el.addEventListener('change', () => {{
                    if (toggle.checked) {{
                        scheduleNextAutoUpdate();
                    }}
                }});
            }});
        }}

        // 等待DOM和Plotly加载完成
        if (document.readyState === 'loading') {{
            document.addEventListener('DOMContentLoaded', renderAllCharts);
        }} else {{
            setTimeout(renderAllCharts, 100);
        }}
    </script>
</body>
</html>'''
        
        return html_template
