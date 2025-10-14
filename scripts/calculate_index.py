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


def display_existing_indices(existing_indices: dict) -> int:
    """
    显示现有指数信息
    
    Args:
        existing_indices: 现有指数字典
        
    Returns:
        总指数数量
    """
    total_count = 0
    
    print("\n" + "=" * 60)
    print("发现以下已存在的指数文件：")
    print("=" * 60)
    
    for category, files in existing_indices.items():
        print(f"\n【{category}】 ({len(files)} 个指数)")
        for file in sorted(files):
            # 去除文件扩展名显示
            index_name = file.replace("_average.parquet", "").replace("_market_cap_weighted.parquet", "")
            method = "平均指数" if "_average" in file else "市值加权"
            print(f"  - {index_name} ({method})")
            total_count += 1
    
    print("\n" + "-" * 60)
    print(f"共计：{total_count} 个指数文件")
    print("-" * 60)
    
    return total_count


def ask_user_confirmation() -> bool:
    """
    询问用户是否删除现有指数
    
    Returns:
        True表示删除，False表示保留
    """
    print("\n⚠️  是否要删除所有已存在的指数文件？")
    print("   - 删除后将重新计算所有指数")
    print("   - 保留则可能会覆盖同名指数")
    
    while True:
        response = input("\n请输入 (y/n): ").strip().lower()
        if response in ['y', 'yes', '是']:
            return True
        elif response in ['n', 'no', '否']:
            return False
        else:
            print("输入无效，请输入 y(是) 或 n(否)")


def delete_existing_indices(indices_dir: Path):
    """
    删除所有现有指数文件
    
    Args:
        indices_dir: 指数存储目录
    """
    print("\n正在删除现有指数文件...")
    
    deleted_count = 0
    for category_dir in indices_dir.iterdir():
        if category_dir.is_dir():
            # 删除该分类目录下的所有文件
            for file in category_dir.iterdir():
                if file.is_file():
                    file.unlink()
                    deleted_count += 1
                    print(f"  ✓ 已删除: {category_dir.name}/{file.name}")
            
            # 如果目录为空，删除目录
            if not any(category_dir.iterdir()):
                category_dir.rmdir()
                print(f"  ✓ 已删除空目录: {category_dir.name}")
    
    print(f"\n✓ 成功删除 {deleted_count} 个指数文件\n")


if __name__ == "__main__":
    # 初始化日志
    setup_logger()
    
    print("=" * 60)
    print("DWAD 股池指数计算工具")
    print("=" * 60)
    
    # 获取项目根目录
    project_root = Path(__file__).resolve().parent.parent
    indices_dir = project_root / "data" / "indices"
    
    # 查找现有指数
    existing_indices = find_existing_indices(indices_dir)
    
    if existing_indices:
        # 显示现有指数
        total_count = display_existing_indices(existing_indices)
        
        # 询问用户是否删除
        if ask_user_confirmation():
            delete_existing_indices(indices_dir)
        else:
            print("\n✓ 保留现有指数文件，继续计算...\n")
    else:
        print("\n✓ 未发现已存在的指数文件\n")
    
    print("=" * 60)
    print("开始计算指数...")
    print("=" * 60)
    print()
    
    # 运行指数计算
    main()
