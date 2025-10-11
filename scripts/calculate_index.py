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
from pathlib import Path

# 添加项目源码路径到 sys.path
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.analysis.index_calculator import main
from dwad.utils.logger import setup_logger

if __name__ == "__main__":
    # 初始化日志
    setup_logger()
    
    print("=" * 60)
    print("DWAD 股池指数计算工具")
    print("=" * 60)
    
    # 运行指数计算
    main()
