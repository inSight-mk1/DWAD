#!/usr/bin/env python3
"""
股票数据读取示例

这个示例展示如何读取已下载的股票数据，包括：
1. 读取某只股票的全部历史数据
2. 读取某只股票特定时间段的数据
3. 获取股票基本信息
4. 数据的基本分析操作
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent.parent))

from src.dwad.data_storage.parquet_storage import ParquetStorage


def display_stock_info(storage):
    """显示可用的股票信息"""
    print("=== 获取股票基本信息 ===")

    # 获取股票基本信息
    stock_list = storage.load_stock_info()
    if stock_list:
        print(f"共有 {len(stock_list)} 只股票")
        print("\n前10只股票信息:")
        for i, stock in enumerate(stock_list[:10]):
            print(f"{i+1}. {stock.symbol} - {stock.name} ({stock.market})")
    else:
        print("未找到股票基本信息")

    # 获取可用的股票代码
    available_symbols = storage.list_available_stocks()
    if available_symbols:
        print(f"\n数据文件中有 {len(available_symbols)} 只股票的历史数据")
        print("前10只有数据的股票:", available_symbols[:10])
        return available_symbols
    else:
        print("未找到任何股票数据文件")
        return []


def read_single_stock_data(storage, symbol):
    """读取单只股票的完整数据"""
    print(f"\n=== 读取股票 {symbol} 的完整历史数据 ===")

    data = storage.load_stock_data(symbol)

    if data.empty:
        print(f"股票 {symbol} 没有数据")
        return None

    print(f"数据条数: {len(data)}")
    print(f"数据列: {list(data.columns)}")
    print(f"数据时间范围: {data['date'].min()} 到 {data['date'].max()}")

    print("\n前5条数据:")
    print(data.head())

    print("\n后5条数据:")
    print(data.tail())

    return data


def read_stock_data_by_date_range(storage, symbol, start_date, end_date):
    """读取某只股票特定时间段的数据"""
    print(f"\n=== 读取股票 {symbol} 从 {start_date} 到 {end_date} 的数据 ===")

    # 先读取完整数据
    data = storage.load_stock_data(symbol)

    if data.empty:
        print(f"股票 {symbol} 没有数据")
        return None

    # 确保date列是datetime类型
    data['date'] = pd.to_datetime(data['date'])

    # 过滤日期范围
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    filtered_data = data[
        (data['date'] >= start_dt) &
        (data['date'] <= end_dt)
    ].copy()

    if filtered_data.empty:
        print(f"在指定时间范围内没有找到 {symbol} 的数据")
        return None

    print(f"过滤后数据条数: {len(filtered_data)}")
    print(f"实际时间范围: {filtered_data['date'].min().strftime('%Y-%m-%d')} 到 {filtered_data['date'].max().strftime('%Y-%m-%d')}")

    # 将日期转换回字符串格式显示
    filtered_data['date'] = filtered_data['date'].dt.strftime('%Y-%m-%d')

    print("\n数据样本:")
    print(filtered_data.head(10))

    return filtered_data


def analyze_stock_data(data, symbol):
    """对股票数据进行基本分析"""
    if data is None or data.empty:
        return

    print(f"\n=== 股票 {symbol} 基本分析 ===")

    # 确保数值列是float类型
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')

    # 价格统计
    if 'close' in data.columns:
        close_prices = data['close'].dropna()
        print(f"收盘价统计:")
        print(f"  最高价: ¥{close_prices.max():.2f}")
        print(f"  最低价: ¥{close_prices.min():.2f}")
        print(f"  平均价: ¥{close_prices.mean():.2f}")
        print(f"  当前价: ¥{close_prices.iloc[-1]:.2f}")

        # 计算涨跌幅
        if len(close_prices) > 1:
            first_price = close_prices.iloc[0]
            last_price = close_prices.iloc[-1]
            total_return = (last_price - first_price) / first_price * 100
            print(f"  总涨跌幅: {total_return:+.2f}%")

    # 成交量统计
    if 'volume' in data.columns:
        volumes = data['volume'].dropna()
        print(f"\n成交量统计:")
        print(f"  平均成交量: {volumes.mean():.0f}")
        print(f"  最大成交量: {volumes.max():.0f}")
        print(f"  最小成交量: {volumes.min():.0f}")

    # 计算简单移动平均线
    if 'close' in data.columns and len(data) >= 20:
        data['ma5'] = data['close'].rolling(window=5).mean()
        data['ma20'] = data['close'].rolling(window=20).mean()

        print(f"\n最近的移动平均线:")
        recent_data = data.tail(5)[['date', 'close', 'ma5', 'ma20']]
        print(recent_data.to_string(index=False, float_format='%.2f'))


def get_stock_date_range_info(storage, symbol):
    """获取股票数据的日期范围信息"""
    print(f"\n=== 股票 {symbol} 数据范围信息 ===")

    min_date, max_date = storage.get_stock_date_range(symbol)
    if min_date and max_date:
        print(f"数据时间范围: {min_date} 到 {max_date}")

        # 计算数据天数
        start_dt = pd.to_datetime(min_date)
        end_dt = pd.to_datetime(max_date)
        days = (end_dt - start_dt).days + 1
        print(f"总天数: {days} 天")

        return min_date, max_date
    else:
        print("没有找到数据")
        return None, None


def main():
    """主函数 - 演示各种数据读取操作"""
    print("股票数据读取示例")
    print("=" * 50)

    # 初始化存储管理器
    storage = ParquetStorage()

    # 1. 显示股票信息
    available_symbols = display_stock_info(storage)

    if not available_symbols:
        print("\n错误: 没有找到任何股票数据")
        print("请先运行数据下载程序下载数据")
        return

    # 选择一只股票进行演示（使用第一只有数据的股票）
    demo_symbol = available_symbols[0]
    print(f"\n使用 {demo_symbol} 进行演示")

    # 2. 获取股票数据范围信息
    min_date, max_date = get_stock_date_range_info(storage, demo_symbol)

    # 3. 读取完整历史数据
    full_data = read_single_stock_data(storage, demo_symbol)

    # 4. 读取特定时间段数据（最近3个月）
    if min_date and max_date:
        # 使用最近3个月的数据作为示例
        end_date = max_date
        start_date = (pd.to_datetime(max_date) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')

        recent_data = read_stock_data_by_date_range(storage, demo_symbol, start_date, end_date)

        # 5. 对最近的数据进行分析
        if recent_data is not None:
            analyze_stock_data(recent_data, demo_symbol)

    # 6. 演示读取多只股票
    print(f"\n=== 批量读取多只股票示例 ===")
    sample_symbols = available_symbols[:3]  # 取前3只股票

    for symbol in sample_symbols:
        print(f"\n股票 {symbol}:")
        min_d, max_d = storage.get_stock_date_range(symbol)
        if min_d and max_d:
            data = storage.load_stock_data(symbol)
            if not data.empty and 'close' in data.columns:
                latest_price = data['close'].iloc[-1]
                print(f"  数据范围: {min_d} 到 {max_d}")
                print(f"  最新收盘价: ¥{latest_price:.2f}")
                print(f"  数据条数: {len(data)}")
        else:
            print("  无数据")

    print(f"\n=== 示例结束 ===")
    print("你可以修改 demo_symbol 变量来查看其他股票的数据")
    print("或者修改日期范围来获取不同时间段的数据")


if __name__ == "__main__":
    main()