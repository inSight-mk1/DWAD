#!/usr/bin/env python3
"""
DWAD 数据下载工具启动脚本

使用方法:
    python download_data.py

配置:
    请在 config/config.yaml 中设置 goldminer.token
"""

import sys
from pathlib import Path

# 添加项目源码路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dwad.tools.data_downloader import main

if __name__ == "__main__":
    main()