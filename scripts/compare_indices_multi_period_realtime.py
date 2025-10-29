#!/usr/bin/env python3
"""
DWAD 股池指数多周期实时比较和排名脚本

使用方法:
    python compare_indices_multi_period_realtime.py [--no_realtime]

参数:
    --no_realtime: 不获取实时数据，仅显示历史数据

功能:
    - 根据 config/index_comparison.yaml 中的配置比较多个股池指数
    - 计算多个时间周期（近20、55、233个交易日）的排名
    - 获取实时价格数据并计算实时排名（可选）
    - 在一个HTML页面显示3个图表，每个图表包含历史数据（实线）和实时数据（虚线）
    - 导出排名数据到CSV文件

周期说明:
    - 20个交易日：约1个月
    - 55个交易日：约3个月（一个季度）
    - 233个交易日：约1年

实时数据显示:
    - 历史数据：实线
    - 实时数据：虚线 + 圆点标记（仅在启用实时功能时显示）
    - 显示实时价格获取时间

输出:
    - reports/index_ranking_multi_period_realtime.html: 多周期实时排名可视化页面
    - reports/index_ranking_data.csv: 完整排名数据CSV文件
"""

import sys
import argparse
from pathlib import Path

# 添加项目源码路径
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.analysis.index_comparator import IndexComparator
from dwad.visualization.ranking_visualizer import RankingVisualizer
from dwad.utils.logger import setup_logger
from loguru import logger


def main(enable_realtime: bool = True):
    """主函数"""
    # 初始化日志
    setup_logger()
    
    logger.info("="*70)
    logger.info("DWAD 股池指数多周期实时比较和排名分析")
    logger.info("="*70)
    
    try:
        # 1. 初始化比较器
        if enable_realtime:
            logger.info("\n步骤 1/5: 初始化指数比较器（启用实时价格功能）...")
            comparator = IndexComparator(enable_realtime=True)
        else:
            logger.info("\n步骤 1/5: 初始化指数比较器（仅历史数据）...")
            comparator = IndexComparator(enable_realtime=False)
        
        # 2. 加载指数数据
        logger.info("\n步骤 2/5: 加载指数数据...")
        if not comparator.load_indices_data():
            logger.error("加载指数数据失败")
            return False
        
        # 3. 生成多周期可视化
        if enable_realtime:
            logger.info("\n步骤 3/5: 生成多周期实时排名可视化页面...")
        else:
            logger.info("\n步骤 3/5: 生成多周期历史排名可视化页面...")
        visualizer = RankingVisualizer()
        
        # 定义要分析的周期
        periods = [20, 55, 233]  # 近20、55、233个交易日
        logger.info(f"  分析周期: {periods} 个交易日")
        
        # 获取多周期数据
        if enable_realtime:
            logger.info("  正在获取实时价格和计算排名...")
        else:
            logger.info("  正在计算历史排名...")
        
        ranking_data = comparator.get_ranking_data_for_visualization(
            periods=periods, 
            include_realtime=enable_realtime
        )
        
        # 从ranking_data中提取实时排名信息用于日志展示
        if enable_realtime and ranking_data and 'periods' in ranking_data and len(ranking_data['periods']) > 0:
            # 从第一个周期的实时数据中获取排名（所有周期共享同一份实时数据）
            first_period = ranking_data['periods'][0]
            if 'realtime' in first_period and first_period['realtime']:
                realtime_info = first_period['realtime']
                timestamp = realtime_info.get('timestamp')
                rankings = realtime_info.get('rankings', {})
                
                if rankings:
                    logger.info(f"\n✅ 实时排名 (数据时间: {timestamp}):")
                    # 按排名排序
                    sorted_rankings = sorted(rankings.items(), key=lambda x: x[1]['rank'])
                    for name, data in sorted_rankings:
                        rank = data['rank']
                        change = data['change_pct']
                        logger.info(f"  {rank}. {name}: {change:+.2f}%")
                else:
                    logger.warning("⚠️  未能获取实时排名数据，将仅显示历史数据")
            else:
                logger.warning("⚠️  未能获取实时排名数据，将仅显示历史数据")
        
        # 4. 生成可视化HTML
        logger.info("\n步骤 4/5: 生成可视化HTML...")
        
        if not ranking_data or 'periods' not in ranking_data:
            logger.error("无法获取多周期可视化数据")
            return False
        
        # 修改输出文件名
        if 'config' not in ranking_data:
            ranking_data['config'] = {}
        ranking_data['config']['output_filename'] = 'index_ranking_multi_period_realtime.html'
        
        # 生成HTML
        if not visualizer.generate_html(ranking_data):
            logger.error("生成可视化页面失败")
            return False
        
        # 5. 完成
        logger.info("\n步骤 5/5: 完成")
        logger.info("="*70)
        if enable_realtime:
            logger.info("✅ 股池指数多周期实时比较和排名分析完成！")
        else:
            logger.info("✅ 股池指数多周期历史排名分析完成！")
        logger.info("="*70)
        
        # 显示输出文件位置
        output_dir = Path(__file__).parent.parent / "reports"
        html_path = output_dir / "index_ranking_multi_period_realtime.html"
        csv_path = output_dir / "index_ranking_data.csv"
        
        if enable_realtime:
            logger.info(f"\n📊 实时可视化页面: {html_path}")
        else:
            logger.info(f"\n📊 历史数据可视化页面: {html_path}")
        logger.info(f"📁 排名数据CSV: {csv_path}")
        
        if enable_realtime:
            logger.info(f"\n💡 提示: 用浏览器打开 {html_path} 查看多周期实时排名趋势图")
        else:
            logger.info(f"\n💡 提示: 用浏览器打开 {html_path} 查看多周期历史排名趋势图")
        logger.info(f"   - 近20个交易日：约1个月的排名变化")
        logger.info(f"   - 近55个交易日：约3个月（一个季度）的排名变化")
        logger.info(f"   - 近233个交易日：约1年的排名变化")
        
        # 显示实时数据时间
        if enable_realtime and ranking_data and 'periods' in ranking_data and len(ranking_data['periods']) > 0:
            first_period = ranking_data['periods'][0]
            if 'realtime' in first_period and first_period['realtime']:
                timestamp = first_period['realtime'].get('timestamp')
                if timestamp:
                    logger.info(f"\n🕐 实时数据时间: {timestamp}")
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
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description='DWAD 股池指数多周期比较和排名分析',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 启用实时数据（默认）
  python compare_indices_multi_period_realtime.py
  
  # 仅使用历史数据
  python compare_indices_multi_period_realtime.py --no_realtime
        """
    )
    parser.add_argument(
        '--no_realtime',
        action='store_true',
        help='不获取实时数据，仅显示历史数据（适用于非交易时间）'
    )
    
    args = parser.parse_args()
    
    # 根据参数决定是否启用实时功能
    enable_realtime = not args.no_realtime
    
    success = main(enable_realtime=enable_realtime)
    sys.exit(0 if success else 1)
