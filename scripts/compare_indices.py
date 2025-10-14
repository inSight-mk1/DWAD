#!/usr/bin/env python3
"""
DWAD 股池指数比较和排名脚本

使用方法:
    python compare_indices.py

功能:
    - 根据 config/index_comparison.yaml 中的配置比较多个股池指数
    - 计算每个交易日的排名（基于涨跌幅）
    - 生成可视化Web页面显示排名趋势
    - 导出排名数据到CSV文件

配置文件:
    - config/index_comparison.yaml: 指数比较配置
    - 可配置比较起始日期、需要比较的指数等

输出:
    - reports/index_ranking_comparison.html: 排名可视化页面
    - reports/index_ranking_data.csv: 排名数据CSV文件
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
    logger.info("DWAD 股池指数比较和排名分析")
    logger.info("="*70)
    
    try:
        # 1. 初始化比较器
        logger.info("\n步骤 1/4: 初始化指数比较器...")
        comparator = IndexComparator()
        
        # 2. 加载并计算排名
        logger.info("\n步骤 2/4: 加载指数数据并计算排名...")
        if not comparator.run():
            logger.error("指数比较分析失败")
            return False
        
        # 3. 生成可视化
        logger.info("\n步骤 3/4: 生成排名可视化页面...")
        visualizer = RankingVisualizer()
        ranking_data = comparator.get_ranking_data_for_visualization()
        
        if not ranking_data:
            logger.error("无法获取可视化数据")
            return False
        
        if not visualizer.generate_html(ranking_data):
            logger.error("生成可视化页面失败")
            return False
        
        # 4. 完成
        logger.info("\n步骤 4/4: 完成")
        logger.info("="*70)
        logger.info("✅ 股池指数比较和排名分析完成！")
        logger.info("="*70)
        
        # 显示输出文件位置
        vis_config = ranking_data.get('config', {})
        output_dir = Path(__file__).parent.parent / "reports"
        output_filename = vis_config.get('output_filename', 'index_ranking_comparison.html')
        html_path = output_dir / output_filename
        csv_path = output_dir / "index_ranking_data.csv"
        
        logger.info(f"\n📊 可视化页面: {html_path}")
        logger.info(f"📁 排名数据CSV: {csv_path}")
        logger.info(f"\n💡 提示: 用浏览器打开 {html_path} 查看排名趋势图")
        
        return True
        
    except FileNotFoundError as e:
        logger.error(f"文件未找到: {e}")
        logger.info("请确保已运行以下脚本:")
        logger.info("  1. python download_stock_data.py  # 下载股票数据")
        logger.info("  2. python calculate_index.py      # 计算股池指数")
        return False
        
    except Exception as e:
        logger.error(f"发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
