#!/usr/bin/env python3
"""
DWAD 股池组合指数计算脚本

使用方法:
    python calculate_index.py

功能:
    - 根据 config/stock_pools.yaml 或 config/stock_pools_example.yaml 中的配置计算组合指数。
    - 支持平均加权和市值加权两种方式（当前版本仅实现平均指数）。
    - 将计算结果保存在 data/indices/ 目录下。
"""

import sys
import shutil
from pathlib import Path

# 添加项目源码路径到 sys.path
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.analysis.index_calculator import main
from dwad.utils.logger import setup_logger


def find_existing_indices(indices_dir: Path) -> dict:
    """
    查找现有的指数文件
    
    Args:
        indices_dir: 指数存储目录
        
    Returns:
        字典，key为分类目录名，value为该分类下的指数文件列表
    """
    existing_indices = {}
    
    if not indices_dir.exists():
        return existing_indices
    
    # 遍历所有子目录（股池分类）
    for category_dir in indices_dir.iterdir():
        if category_dir.is_dir():
            # 查找该分类下的所有parquet文件
            parquet_files = list(category_dir.glob("*.parquet"))
            if parquet_files:
                existing_indices[category_dir.name] = [f.name for f in parquet_files]
    
    return existing_indices


def display_existing_indices(existing_indices: dict, stock_pools: dict = None) -> int:
    """
    显示现有指数信息
    
    Args:
        existing_indices: 现有指数字典
        stock_pools: 股池配置字典，包含每个概念对应的股票列表
        
    Returns:
        总指数数量
    """
    total_count = 0
    
    print("\n" + "=" * 80)
    print("计算完成！已生成以下指数：")
    print("=" * 80)
    
    for category, files in existing_indices.items():
        print(f"\n【{category}】")
        for file in sorted(files):
            # 去除文件扩展名显示
            index_name = file.replace("_average.parquet", "").replace("_market_cap_weighted.parquet", "")
            method = "平均指数" if "_average" in file else "市值加权"
            
            # 获取该概念对应的股票数量
            stock_count = 0
            if stock_pools and category in stock_pools and index_name in stock_pools[category]:
                stock_count = len(stock_pools[category][index_name])
            
            print(f"  - {index_name:20} ({method:4}) - 成份股: {stock_count:3} 只")
            total_count += 1
    
    print("\n" + "-" * 80)
    print(f"共计：{total_count} 个指数文件")
    print("=" * 80)
    
    return total_count


def delete_existing_indices(indices_dir: Path, silent: bool = False) -> int:
    """
    删除所有现有指数文件
    
    Args:
        indices_dir: 指数存储目录
        silent: 是否静默删除（不输出详细信息）
        
    Returns:
        删除的文件数量
    """
    if not indices_dir.exists():
        return 0
    
    deleted_count = 0
    for category_dir in indices_dir.iterdir():
        if category_dir.is_dir():
            # 删除该分类目录下的所有文件
            for file in category_dir.iterdir():
                if file.is_file():
                    file.unlink()
                    deleted_count += 1
            
            # 如果目录为空，删除目录
            if not any(category_dir.iterdir()):
                category_dir.rmdir()
    
    if not silent and deleted_count > 0:
        print(f"✓ 已清理 {deleted_count} 个旧指数文件")
    
    return deleted_count


if __name__ == "__main__":
    # 初始化日志
    setup_logger()
    
    print("=" * 60)
    print("DWAD 股池指数计算工具")
    print("=" * 60)
    print()
    
    # 获取项目根目录
    project_root = Path(__file__).resolve().parent.parent
    indices_dir = project_root / "data" / "indices"
    
    # 自动删除旧的指数文件
    deleted_count = delete_existing_indices(indices_dir, silent=True)
    if deleted_count > 0:
        print(f"✓ 已清理 {deleted_count} 个旧指数文件")
    
    print("开始计算指数...")
    print()
    
    # 运行指数计算
    main()
    
    # 计算完成后显示结果
    print()
    existing_indices = find_existing_indices(indices_dir)
    if existing_indices:
        # 加载股池配置以获取股票数量
        from dwad.analysis.index_calculator import IndexCalculator
        calculator = IndexCalculator()
        stock_pools = calculator.stock_pools
        
        display_existing_indices(existing_indices, stock_pools)
    else:
        print("\n⚠️  未生成任何指数文件")
