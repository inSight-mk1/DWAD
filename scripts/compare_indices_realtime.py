#!/usr/bin/env python3
"""
DWAD 股池指数实时比较和排名脚本

使用方法:
    python compare_indices_realtime.py

功能:
    - 根据 config/index_comparison.yaml 中的配置比较多个股池指数
    - 计算每个交易日的排名（基于涨跌幅）
    - 获取实时价格数据并计算实时排名
    - 生成可视化Web页面，历史数据用实线，实时数据用虚线显示
    - 在图表上显示实时价格获取时间（以所有股票中最早的时间为准）

配置文件:
    - config/index_comparison.yaml: 指数比较配置
    - config/stock_pools.yaml: 股池配置（需要从股票名称获取代码）
    - 可配置比较起始日期、需要比较的指数等

输出:
    - reports/index_ranking_comparison_realtime.html: 实时排名可视化页面
    - reports/index_ranking_data.csv: 排名数据CSV文件

注意:
    - 需要掘金API token才能获取实时价格
    - 实时功能仅在交易时间内有效
    - 实时价格基于昨日收盘价计算涨跌幅
"""

import sys
from pathlib import Path

# 添加项目源码路径
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.analysis.index_comparator import IndexComparator
from dwad.visualization.ranking_visualizer import RankingVisualizer
from dwad.utils.logger import setup_logger
from loguru import logger


def main():
    """主函数"""
    # 初始化日志
    setup_logger()
    
    logger.info("="*70)
    logger.info("DWAD 股池指数实时比较和排名分析")
    logger.info("="*70)
    
    try:
        # 1. 初始化比较器（启用实时价格功能）
        logger.info("\n步骤 1/5: 初始化指数比较器（启用实时价格功能）...")
        comparator = IndexComparator(enable_realtime=True)
        
        # 2. 加载并计算排名
        logger.info("\n步骤 2/5: 加载指数数据并计算排名...")
        if not comparator.load_indices_data():
            logger.error("加载指数数据失败")
            return False
        
        ranking_df = comparator.calculate_rankings()
        if ranking_df.empty:
            logger.error("排名计算失败")
            return False
        
        # 3. 获取实时排名
        logger.info("\n步骤 3/5: 获取实时价格和排名...")
        realtime_ranking = comparator.get_realtime_ranking()
        
        if realtime_ranking:
            logger.info("✅ 实时排名获取成功")
            
            # 打印实时排名
            rankings = realtime_ranking['rankings']
            timestamp = realtime_ranking['timestamp']
            logger.info(f"\n实时排名 (数据时间: {timestamp}):")
            
            # 按排名排序
            sorted_rankings = sorted(rankings.items(), key=lambda x: x[1]['rank'])
            for name, data in sorted_rankings:
                rank = data['rank']
                change = data['change_pct']
                logger.info(f"  {rank}. {name}: {change:+.2f}%")
        else:
            logger.warning("⚠️  未能获取实时排名数据，将仅显示历史数据")
        
        # 4. 生成可视化（包含实时数据）
        logger.info("\n步骤 4/5: 生成实时排名可视化页面...")
        visualizer = RankingVisualizer()
        
        # 获取可视化数据（包含实时数据）
        ranking_data = comparator.get_ranking_data_for_visualization(include_realtime=True)
        
        if not ranking_data:
            logger.error("无法获取可视化数据")
            return False
        
        # 修改输出文件名
        if 'config' not in ranking_data:
            ranking_data['config'] = {}
        ranking_data['config']['output_filename'] = 'index_ranking_comparison_realtime.html'
        
        if not visualizer.generate_html(ranking_data):
            logger.error("生成可视化页面失败")
            return False
        
        # 5. 完成
        logger.info("\n步骤 5/5: 完成")
        logger.info("="*70)
        logger.info("✅ 股池指数实时比较和排名分析完成！")
        logger.info("="*70)
        
        # 显示输出文件位置
        output_dir = Path(__file__).parent.parent / "reports"
        html_path = output_dir / "index_ranking_comparison_realtime.html"
        csv_path = output_dir / "index_ranking_data.csv"
        
        logger.info(f"\n📊 实时可视化页面: {html_path}")
        logger.info(f"📁 排名数据CSV: {csv_path}")
        logger.info(f"\n💡 提示: 用浏览器打开 {html_path} 查看实时排名趋势图")
        
        if realtime_ranking:
            logger.info(f"🕐 实时数据时间: {realtime_ranking['timestamp']}")
            logger.info("   (图表中虚线部分为实时数据)")
        
        return True
        
    except FileNotFoundError as e:
        logger.error(f"文件未找到: {e}")
        logger.info("请确保已运行以下脚本:")
        logger.info("  1. python download_stock_data.py  # 下载股票数据")
        logger.info("  2. python calculate_index.py      # 计算股池指数")
        logger.info("\n并确保配置文件存在:")
        logger.info("  - config/index_comparison.yaml")
        logger.info("  - config/stock_pools.yaml")
        return False
        
    except Exception as e:
        logger.error(f"发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
