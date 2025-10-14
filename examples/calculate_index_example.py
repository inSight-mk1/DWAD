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
import yaml


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
    示例2：读取并显示已计算的指数数据（单个指数）
    """
    print("\n" + "="*60)
    print("示例2：读取并显示指数数据（单个示例）")
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


def example_display_all_indices():
    """
    示例3：读取并显示所有已计算的指数数据
    """
    print("\n" + "="*70)
    print("示例3：读取并显示所有已保存的指数")
    print("="*70)
    
    storage = ParquetStorage()
    
    # 加载股池配置
    config_dir = Path(__file__).parent.parent / "config"
    config_path = config_dir / "stock_pools.yaml"
    if not config_path.exists():
        config_path = config_dir / "stock_pools_example.yaml"
    
    if not config_path.exists():
        print("❌ 未找到股池配置文件")
        return
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    stock_pools = config.get('stock_pools', {})
    
    total_indices = 0
    found_indices = 0
    
    for pool_name, concepts in stock_pools.items():
        print(f"\n{'='*70}")
        print(f"📊 股池: {pool_name}")
        print('='*70)
        
        for concept_name in concepts.keys():
            total_indices += 1
            
            # 读取指数数据
            index_data = storage.load_index_data(
                pool_name=pool_name,
                concept_name=concept_name,
                index_type="average"
            )
            
            if not index_data.empty:
                found_indices += 1
                
                # 获取统计信息
                start_date = index_data['date'].iloc[0]
                end_date = index_data['date'].iloc[-1]
                start_value = index_data['index_value'].iloc[0]
                end_value = index_data['index_value'].iloc[-1]
                change_pct = ((end_value - start_value) / start_value) * 100
                
                print(f"\n  ✅ {concept_name}")
                print(f"     数据范围: {start_date} ~ {end_date} ({len(index_data)}天)")
                print(f"     指数变化: {start_value:.2f} → {end_value:.2f} ({change_pct:+.2f}%)")
                
                # 显示最近5天数据
                print(f"     最近5个交易日:")
                recent_data = index_data.tail(5)
                for _, row in recent_data.iterrows():
                    print(f"       {row['date']}: {row['index_value']:.2f}")
            else:
                print(f"\n  ❌ {concept_name} - 未找到数据")
    
    # 总结
    print(f"\n{'='*70}")
    print(f"📋 统计总结")
    print('='*70)
    print(f"总指数数量: {total_indices}")
    print(f"已保存: {found_indices} ✅")
    print(f"缺失: {total_indices - found_indices} ❌")
    
    if found_indices == total_indices:
        print("\n🎉 所有股池指数已成功计算和保存！")
    elif found_indices > 0:
        print(f"\n⚠️  部分指数缺失，请运行 scripts/calculate_index.py 重新计算")
    else:
        print("\n❌ 未找到任何指数数据，请先运行 scripts/calculate_index.py")


def example_custom_config_path():
    """
    示例4：使用自定义配置文件路径
    """
    print("\n" + "="*60)
    print("示例4：使用自定义配置文件")
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
    print("2. 读取并显示单个指数数据（机器人概念示例）")
    print("3. 读取并显示所有已保存的指数")
    print("4. 使用自定义配置文件计算指数")
    
    choice = input("\n请输入选项 (1-4): ").strip()
    
    if choice == "1":
        example_calculate_all_indices()
    elif choice == "2":
        example_load_and_display_index()
    elif choice == "3":
        example_display_all_indices()
    elif choice == "4":
        example_custom_config_path()
    else:
        print("无效的选项，请重新运行程序")
