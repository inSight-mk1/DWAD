#!/usr/bin/env python3
"""
验证所有股池指数是否已计算和保存

本脚本用于检查所有配置的股池指数是否都已成功计算并保存
"""

import sys
from pathlib import Path

# 添加项目源码路径
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.data_storage.parquet_storage import ParquetStorage
from dwad.utils.logger import setup_logger
import yaml


def load_stock_pools_config():
    """加载股池配置文件"""
    config_dir = Path(__file__).parent.parent / "config"
    
    # 尝试加载主配置文件
    config_path = config_dir / "stock_pools.yaml"
    if not config_path.exists():
        config_path = config_dir / "stock_pools_example.yaml"
    
    if not config_path.exists():
        print("❌ 未找到股池配置文件")
        return {}
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config.get('stock_pools', {})


def verify_all_indices():
    """验证所有股池指数是否已保存"""
    print("\n" + "="*70)
    print("股池指数验证报告")
    print("="*70)
    
    storage = ParquetStorage()
    stock_pools = load_stock_pools_config()
    
    if not stock_pools:
        print("❌ 股池配置为空")
        return
    
    total_concepts = 0
    saved_concepts = 0
    missing_concepts = []
    
    for pool_name, concepts in stock_pools.items():
        print(f"\n📊 股池: {pool_name}")
        print("-" * 70)
        
        for concept_name, stocks in concepts.items():
            total_concepts += 1
            
            # 尝试加载指数数据
            index_data = storage.load_index_data(
                pool_name=pool_name,
                concept_name=concept_name,
                index_type="average"
            )
            
            if not index_data.empty:
                saved_concepts += 1
                # 获取数据统计信息
                start_date = index_data['date'].iloc[0]
                end_date = index_data['date'].iloc[-1]
                data_count = len(index_data)
                
                # 计算涨跌幅
                start_value = index_data['index_value'].iloc[0]
                end_value = index_data['index_value'].iloc[-1]
                change_pct = ((end_value - start_value) / start_value) * 100
                
                print(f"  ✅ {concept_name}")
                print(f"     数据范围: {start_date} ~ {end_date} ({data_count}天)")
                print(f"     指数变化: {start_value:.2f} → {end_value:.2f} ({change_pct:+.2f}%)")
                print(f"     股票数量: {len(stocks)}只")
            else:
                print(f"  ❌ {concept_name} - 未找到指数数据")
                missing_concepts.append(f"{pool_name}/{concept_name}")
    
    # 总结
    print("\n" + "="*70)
    print("📋 验证总结")
    print("="*70)
    print(f"总概念数: {total_concepts}")
    print(f"已保存: {saved_concepts} ✅")
    print(f"缺失: {len(missing_concepts)} ❌")
    
    if missing_concepts:
        print("\n缺失的指数:")
        for concept in missing_concepts:
            print(f"  - {concept}")
        print("\n💡 提示: 请运行 scripts/calculate_index.py 来计算缺失的指数")
    else:
        print("\n🎉 所有股池指数已成功计算和保存！")


if __name__ == "__main__":
    setup_logger()
    verify_all_indices()
