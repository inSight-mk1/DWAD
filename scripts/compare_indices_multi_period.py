#!/usr/bin/env python3
"""
DWAD 股池指数多周期比较和排名脚本

使用方法:
    python compare_indices_multi_period.py

功能:
    - 根据 config/index_comparison.yaml 中的配置比较多个股池指数
    - 计算多个时间周期（近20、55、233个交易日）的排名
    - 在一个HTML页面显示3个图表
    - 导出排名数据到CSV文件

周期说明:
    - 20个交易日：约1个月
    - 55个交易日：约3个月（一个季度）
    - 233个交易日：约1年

输出:
    - reports/index_ranking_multi_period.html: 多周期排名可视化页面
    - reports/index_ranking_data.csv: 完整排名数据CSV文件
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
    logger.info("DWAD 股池指数多周期比较和排名分析")
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
        
        # 3. 生成多周期可视化
        logger.info("\n步骤 3/4: 生成多周期排名可视化页面...")
        visualizer = RankingVisualizer()
        
        # 定义要分析的周期
        periods = [20, 55, 233]  # 近20、55、233个交易日
        logger.info(f"  分析周期: {periods} 个交易日")
        
        # 获取多周期数据
        ranking_data = comparator.get_ranking_data_for_visualization(periods=periods)
        
        if not ranking_data or 'periods' not in ranking_data:
            logger.error("无法获取多周期可视化数据")
            return False
        
        # 修改输出文件名
        ranking_data['config']['output_filename'] = 'index_ranking_multi_period.html'
        
        # 生成HTML
        if not visualizer.generate_html(ranking_data):
            logger.error("生成可视化页面失败")
            return False
        
        # 4. 完成
        logger.info("\n步骤 4/4: 完成")
        logger.info("="*70)
        logger.info("✅ 股池指数多周期比较和排名分析完成！")
        logger.info("="*70)
        
        # 显示输出文件位置
        output_dir = Path(__file__).parent.parent / "reports"
        html_path = output_dir / "index_ranking_multi_period.html"
        csv_path = output_dir / "index_ranking_data.csv"
        
        logger.info(f"\n📊 可视化页面: {html_path}")
        logger.info(f"📁 排名数据CSV: {csv_path}")
        logger.info(f"\n💡 提示: 用浏览器打开 {html_path} 查看多周期排名趋势图")
        logger.info(f"   - 近20个交易日：约1个月的排名变化")
        logger.info(f"   - 近55个交易日：约3个月（一个季度）的排名变化")
        logger.info(f"   - 近233个交易日：约1年的排名变化")
        
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
