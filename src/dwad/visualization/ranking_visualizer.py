"""
股池指数排名可视化模块

该模块用于生成股池指数排名的可视化Web页面
"""

from pathlib import Path
from typing import Dict, Optional
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
            
            # 使用交易日索引作为x轴（从0开始）
            x_values = list(range(len(ranks)))
            
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
                'customdata': [[date, change] for date, change in zip(series_dates, changes)],  # 添加日期和涨跌幅数据
                'hovertemplate': f"<b>{series['name']}</b><br>" +
                                "日期: %{customdata[0]}<br>" +
                                "排名: %{y}<br>" +
                                "涨跌幅: %{customdata[1]:.2f}%<br>" +
                                "<extra></extra>"
            }
            traces.append(trace)
        
        # 生成HTML内容
        # 使用indent=2格式化JSON，便于调试
        html_content = self._generate_html_template(
            title=title,
            traces_json=json.dumps(traces, ensure_ascii=False, indent=2),
            dates_json=json.dumps(dates, ensure_ascii=False),  # 传递日期列表用于x轴标签
            width=width,
            height=height,
            total_indices=total_indices,
            show_grid=show_grid
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
                
                # 使用交易日索引作为x轴（从0开始）
                x_values = list(range(len(ranks)))
                
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
                    'customdata': [[date, change] for date, change in zip(series_dates, changes)],  # 添加日期和涨跌幅数据
                    'hovertemplate': f"<b>{series['name']}</b><br>" +
                                    "日期: %{customdata[0]}<br>" +
                                    "排名: %{y}<br>" +
                                    "涨跌幅: %{customdata[1]:.2f}%<br>" +
                                    "<extra></extra>"
                }
                traces.append(trace)
            
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
            line_width=line_width
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
                                height: int, total_indices: int, show_grid: bool) -> str:
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
            padding: 20px;
        }}
        
        .container {{
            max-width: 98%;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 32px;
            font-weight: 600;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 14px;
            opacity: 0.9;
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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{title}</h1>
            <p>📊 实时追踪股池指数排名变化 · 排名越小表现越好</p>
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
        
        // 更新信息面板
        if (dates.length > 0) {{
            document.getElementById('start-date').textContent = dates[0];
            document.getElementById('end-date').textContent = dates[dates.length - 1];
            document.getElementById('trading-days').textContent = dates.length;
        }}
        
        // 设置当前时间
        const now = new Date();
        document.getElementById('update-time').textContent = now.toLocaleString('zh-CN');
        
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
                x: 1.02,
                y: 1,
                xanchor: 'left',
                yanchor: 'top',
                bgcolor: 'rgba(255, 255, 255, 0.9)',
                bordercolor: '#e9ecef',
                borderwidth: 1
            }},
            margin: {{
                l: 60,
                r: 150,
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
                                      height: int, total_indices: int, show_grid: bool, line_width: int = 2) -> str:
        """
        生成多图表HTML模板
        
        Args:
            all_periods_traces: 所有周期的traces数据列表
            width: 图表宽度
            height: 每个图表的高度
            total_indices: 总指数数量
            show_grid: 是否显示网格
            line_width: 线条宽度
            
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
        </div>
'''
            
            # 添加图表渲染脚本
            charts_script += f'''
        // 渲染图表 {idx + 1}: {title}
        renderSingleChart(
            '{chart_id}',
            {traces_json},
            {dates_json},
            '{title}',
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
            padding: 20px;
        }}
        
        .container {{
            max-width: 98%;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 32px;
            font-weight: 600;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 14px;
            opacity: 0.9;
        }}
        
        .chart-section {{
            padding: 30px;
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
        
        .chart {{
            width: 100%;
            min-height: {height}px;
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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>股池指数排名多周期分析</h1>
            <p>📊 多时间维度对比分析 · 排名越小表现越好</p>
        </div>
        
{charts_html}
        
        <div class="footer">
            <p>🚀 DWAD 股池指数分析系统 · 数据更新时间: <span id="update-time"></span></p>
        </div>
    </div>
    
    <script>
        // 设置当前时间
        const now = new Date();
        document.getElementById('update-time').textContent = now.toLocaleString('zh-CN');
        
        // 通用图表渲染函数
        function renderSingleChart(chartId, traces, dates, title, totalIndices) {{
            // 为每条线生成标签注释
            const annotations = [];
            
            traces.forEach((trace, idx) => {{
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
                
                // 2. 在终点右侧添加带涨幅的标注（新功能）
                const lastIdx = trace.x.length - 1;
                const lastChange = trace.customdata[lastIdx][1];
                const changeText = lastChange >= 0 ? `+${{lastChange.toFixed(2)}}%` : `${{lastChange.toFixed(2)}}%`;
                
                annotations.push({{
                    x: trace.x[lastIdx],
                    y: trace.y[lastIdx],
                    xref: 'x',
                    yref: 'y',
                    text: `${{trace.name}} ${{changeText}}`,
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
                    l: 50,
                    r: 150,
                    t: 30,
                    b: 100
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
                    
                    chartElement.on('plotly_click', function(data) {{
                        const pointData = data.points[0];
                        const traceIndex = pointData.curveNumber;
                        
                        // 切换该线的状态
                        if (clickedTraces.has(traceIndex)) {{
                            // 已被点击过，恢复原状
                            clickedTraces.delete(traceIndex);
                            Plotly.restyle(chartId, {{
                                'mode': 'lines',
                                'line.width': {line_width}
                            }}, [traceIndex]);
                        }} else {{
                            // 未被点击，加粗并显示数据点
                            clickedTraces.add(traceIndex);
                            Plotly.restyle(chartId, {{
                                'mode': 'lines+markers',
                                'line.width': {line_width * 2},
                                'marker.size': 6
                            }}, [traceIndex]);
                        }}
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
