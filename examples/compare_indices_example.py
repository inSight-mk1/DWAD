#!/usr/bin/env python3
"""
股池指数比较示例

本示例展示如何使用 IndexComparator 和 RankingVisualizer 进行指数比较和可视化
"""

import sys
from pathlib import Path

# 添加项目源码路径
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.analysis.index_comparator import IndexComparator
from dwad.visualization.ranking_visualizer import RankingVisualizer
from dwad.utils.logger import setup_logger


def example_basic_comparison():
    """
    示例1：基本的指数比较和排名
    """
    print("\n" + "="*60)
    print("示例1：基本的指数比较和排名")
    print("="*60)
    
    # 创建比较器
    comparator = IndexComparator()
    
    # 加载数据
    print("\n加载指数数据...")
    comparator.load_indices_data()
    
    # 计算排名
    print("\n计算排名...")
    ranking_df = comparator.calculate_rankings()
    
    if not ranking_df.empty:
        print("\n✅ 排名计算完成")
        print(f"数据范围: {ranking_df.index[0]} ~ {ranking_df.index[-1]}")
        print(f"交易日数: {len(ranking_df)}")
        
        # 显示最新排名
        print("\n最新排名:")
        latest = ranking_df.iloc[-1]
        rank_cols = [col for col in ranking_df.columns 
                    if not col.endswith('_value') and not col.endswith('_pct')]
        
        ranks = [(col, latest[col], latest[f'{col}_pct']) for col in rank_cols]
        ranks.sort(key=lambda x: x[1])
        
        for name, rank, pct in ranks:
            print(f"  {int(rank)}. {name}: {pct:+.2f}%")


def example_with_visualization():
    """
    示例2：比较并生成可视化
    """
    print("\n" + "="*60)
    print("示例2：比较并生成可视化")
    print("="*60)
    
    # 创建比较器
    comparator = IndexComparator()
    
    # 执行完整流程
    print("\n执行指数比较...")
    comparator.run()
    
    # 生成可视化
    print("\n生成可视化页面...")
    visualizer = RankingVisualizer()
    ranking_data = comparator.get_ranking_data_for_visualization()
    
    if ranking_data:
        visualizer.generate_html(ranking_data)
        print("\n✅ 可视化页面生成完成")
    else:
        print("\n❌ 无法生成可视化")


def example_custom_config():
    """
    示例3：使用自定义配置文件
    """
    print("\n" + "="*60)
    print("示例3：使用自定义配置文件")
    print("="*60)
    
    # 指定配置文件路径
    config_path = Path(__file__).parent.parent / "config" / "index_comparison.yaml"
    
    print(f"\n使用配置文件: {config_path}")
    
    # 创建比较器
    comparator = IndexComparator(comparison_config_path=str(config_path))
    
    # 执行比较
    comparator.run()
    
    # 生成可视化
    visualizer = RankingVisualizer()
    ranking_data = comparator.get_ranking_data_for_visualization()
    visualizer.generate_html(ranking_data)


def example_export_data():
    """
    示例4：导出排名数据到CSV
    """
    print("\n" + "="*60)
    print("示例4：导出排名数据")
    print("="*60)
    
    # 创建比较器并计算排名
    comparator = IndexComparator()
    comparator.load_indices_data()
    comparator.calculate_rankings()
    
    # 导出到CSV
    print("\n导出排名数据...")
    output_path = Path(__file__).parent.parent / "reports" / "my_ranking_data.csv"
    comparator.export_ranking_to_csv(str(output_path))
    
    print(f"\n✅ 数据已导出到: {output_path}")


if __name__ == "__main__":
    # 初始化日志
    setup_logger()
    
    # 运行示例
    print("\n" + "="*60)
    print("股池指数比较示例程序")
    print("="*60)
    
    # 选择要运行的示例
    print("\n请选择要运行的示例:")
    print("1. 基本的指数比较和排名")
    print("2. 比较并生成可视化")
    print("3. 使用自定义配置文件")
    print("4. 导出排名数据到CSV")
    
    choice = input("\n请输入选项 (1-4): ").strip()
    
    if choice == "1":
        example_basic_comparison()
    elif choice == "2":
        example_with_visualization()
    elif choice == "3":
        example_custom_config()
    elif choice == "4":
        example_export_data()
    else:
        print("无效的选项，请重新运行程序")
