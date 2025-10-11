#!/usr/bin/env python3
"""
股池指数计算示例

本示例展示如何使用 IndexCalculator 计算股池组合指数
"""

import sys
from pathlib import Path

# 添加项目源码路径
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.analysis.index_calculator import IndexCalculator
from dwad.data_storage.parquet_storage import ParquetStorage
from dwad.utils.logger import setup_logger


def example_calculate_all_indices():
    """
    示例1：计算所有股池的指数
    """
    print("\n" + "="*60)
    print("示例1：计算所有股池的指数")
    print("="*60)
    
    # 初始化计算器（自动加载配置文件）
    calculator = IndexCalculator()
    
    # 执行计算
    calculator.run()


def example_load_and_display_index():
    """
    示例2：读取并显示已计算的指数数据
    """
    print("\n" + "="*60)
    print("示例2：读取并显示指数数据")
    print("="*60)
    
    storage = ParquetStorage()
    
    # 读取"机器人概念"的平均指数
    index_data = storage.load_index_data(
        pool_name="大概念股池",
        concept_name="机器人概念",
        index_type="average"
    )
    
    if not index_data.empty:
        print(f"\n机器人概念平均指数（最近10个交易日）:")
        print(index_data.tail(10))
        
        # 计算涨跌幅
        if len(index_data) > 1:
            start_value = index_data['index_value'].iloc[0]
            end_value = index_data['index_value'].iloc[-1]
            change_pct = ((end_value - start_value) / start_value) * 100
            
            print(f"\n指数统计:")
            print(f"  起始值: {start_value:.2f}")
            print(f"  最新值: {end_value:.2f}")
            print(f"  涨跌幅: {change_pct:+.2f}%")
    else:
        print("未找到指数数据，请先运行 calculate_index.py")


def example_custom_config_path():
    """
    示例3：使用自定义配置文件路径
    """
    print("\n" + "="*60)
    print("示例3：使用自定义配置文件")
    print("="*60)
    
    # 指定配置文件路径
    config_path = Path(__file__).parent.parent / "config" / "stock_pools_example.yaml"
    
    calculator = IndexCalculator(stock_pools_config_path=str(config_path))
    calculator.run()


if __name__ == "__main__":
    # 初始化日志
    setup_logger()
    
    # 运行示例
    print("\n" + "="*60)
    print("股池指数计算示例程序")
    print("="*60)
    
    # 选择要运行的示例
    print("\n请选择要运行的示例:")
    print("1. 计算所有股池的指数")
    print("2. 读取并显示已计算的指数数据")
    print("3. 使用自定义配置文件计算指数")
    
    choice = input("\n请输入选项 (1-3): ").strip()
    
    if choice == "1":
        example_calculate_all_indices()
    elif choice == "2":
        example_load_and_display_index()
    elif choice == "3":
        example_custom_config_path()
    else:
        print("无效的选项，请重新运行程序")
