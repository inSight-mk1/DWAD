#!/usr/bin/env python3
"""
实时价格获取示例

演示如何使用 RealtimePriceFetcher 获取股票实时价格
"""

import sys
from pathlib import Path

# 添加项目源码路径
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
sys.path.insert(0, str(SRC_DIR))

from dwad.data_fetcher.realtime_price_fetcher import RealtimePriceFetcher
from dwad.utils.logger import setup_logger
from loguru import logger


def main():
    """主函数"""
    setup_logger()
    
    logger.info("="*60)
    logger.info("实时价格获取示例")
    logger.info("="*60)
    
    try:
        # 1. 创建实时价格获取器
        logger.info("\n1. 初始化实时价格获取器...")
        fetcher = RealtimePriceFetcher()
        
        # 2. 获取单只股票的实时价格
        logger.info("\n2. 获取单只股票实时价格...")
        symbol = "SHSE.600000"  # 浦发银行
        price_info = fetcher.get_current_price(symbol)
        
        if price_info:
            logger.info(f"   股票代码: {price_info.symbol}")
            logger.info(f"   最新价: {price_info.price:.2f}")
            logger.info(f"   更新时间: {price_info.created_at}")
        else:
            logger.warning(f"   未能获取 {symbol} 的实时价格")
        
        # 3. 批量获取多只股票的实时价格
        logger.info("\n3. 批量获取多只股票实时价格...")
        symbols = [
            "SHSE.600000",  # 浦发银行
            "SZSE.000001",  # 平安银行
            "SHSE.600036",  # 招商银行
            "SZSE.000002",  # 万科A
            "SHSE.600519",  # 贵州茅台
        ]
        
        prices = fetcher.get_current_prices(symbols)
        
        if prices:
            logger.info(f"   成功获取 {len(prices)} 只股票的实时价格:")
            for symbol, price_info in prices.items():
                logger.info(f"     {symbol}: ¥{price_info.price:.2f} (时间: {price_info.created_at})")
        else:
            logger.warning("   未能获取任何实时价格")
        
        # 4. 获取股池实时价格和最早时间
        logger.info("\n4. 获取股池实时价格（含最早时间戳）...")
        pool_symbols = [
            "SHSE.600000",
            "SZSE.000001",
            "SHSE.600036",
        ]
        
        pool_prices, earliest_time = fetcher.get_pool_current_prices(pool_symbols)
        
        if pool_prices:
            logger.info(f"   股池中 {len(pool_prices)} 只股票的实时价格:")
            for symbol, price_info in pool_prices.items():
                logger.info(f"     {symbol}: ¥{price_info.price:.2f}")
            logger.info(f"   最早价格时间: {earliest_time}")
        else:
            logger.warning("   未能获取股池实时价格")
        
        # 5. 计算实时涨跌幅
        logger.info("\n5. 计算相对于基准价格的涨跌幅...")
        
        # 假设这是昨日收盘价
        base_prices = {
            "SHSE.600000": 10.50,
            "SZSE.000001": 15.20,
            "SHSE.600036": 42.30,
        }
        
        changes = fetcher.calculate_realtime_change(pool_symbols, base_prices)
        
        if changes:
            logger.info("   涨跌幅计算结果:")
            for symbol, change_pct in changes.items():
                logger.info(f"     {symbol}: {change_pct:+.2f}%")
        
        # 6. 转换为DataFrame
        logger.info("\n6. 将实时价格转换为DataFrame...")
        df = fetcher.get_realtime_dataframe(symbols)
        
        if not df.empty:
            logger.info(f"   DataFrame shape: {df.shape}")
            logger.info("\n   前几行数据:")
            print(df.head())
        
        logger.info("\n" + "="*60)
        logger.info("✅ 实时价格获取示例完成")
        logger.info("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
