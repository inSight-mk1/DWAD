import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
from tqdm import tqdm
from loguru import logger

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent.parent.parent))

from dwad.data_fetcher.goldminer_fetcher import GoldMinerFetcher
from dwad.data_storage.parquet_storage import ParquetStorage
from dwad.utils.logger import setup_logger
from dwad.utils.config import config


class DataDownloader:
    """历史数据下载器"""

    def __init__(self):
        """初始化下载器"""
        self.fetcher = GoldMinerFetcher()
        self.storage = ParquetStorage()

    def download_all_stocks_data(self) -> bool:
        """
        下载所有A股股票的历史数据
        参数从配置文件读取

        Returns:
            是否成功完成
        """
        # 从配置文件读取参数
        start_date = config.get('data_fetcher.default_start_date', '2020-01-01')
        end_date = datetime.now().strftime('%Y-%m-%d')
        batch_size = config.get('data_fetcher.batch_size', 50)
        resume = config.get('data_fetcher.resume_download', True)

        logger.info(f"开始下载所有A股历史数据")
        logger.info(f"时间范围: {start_date} 到 {end_date}")
        logger.info(f"批次大小: {batch_size}")
        logger.info(f"断点续传: {resume}")

        # 获取所有股票列表
        logger.info("获取股票列表...")
        print("正在获取股票列表，如果等待时间较长可能是遇到了API访问限制...")

        all_stocks = self.fetcher.get_all_stocks()
        if not all_stocks:
            logger.error("无法获取股票列表")
            return False

        logger.info(f"共找到{len(all_stocks)}只A股股票")

        # 保存股票基本信息
        self.storage.save_stock_info(all_stocks)

        # 如果启用断点续传，过滤已下载的股票
        if resume:
            original_count = len(all_stocks)
            all_stocks = self._filter_existing_stocks(all_stocks, start_date, end_date)
            skipped_count = original_count - len(all_stocks)
            if skipped_count > 0:
                logger.info(f"断点续传模式，跳过{skipped_count}只已有数据的股票，剩余{len(all_stocks)}只股票需要下载")
            else:
                logger.info(f"断点续传模式，{len(all_stocks)}只股票都需要下载")

        if not all_stocks:
            logger.info("所有股票数据已下载完成")
            return True

        # 分批下载
        total_batches = (len(all_stocks) + batch_size - 1) // batch_size
        success_count = 0
        failed_stocks = []

        print(f"开始下载{len(all_stocks)}只股票")
        print("如果某个请求等待时间过长，可能是遇到了API访问限制，请耐心等待...")

        # 使用整体进度条
        with tqdm(total=len(all_stocks), desc="下载进度", unit="股") as pbar:
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(all_stocks))
                batch_stocks = all_stocks[start_idx:end_idx]

                # 下载当前批次
                batch_success = self._download_batch_silent(batch_stocks, start_date, end_date, pbar)
                success_count += batch_success
                failed_count = len(batch_stocks) - batch_success

                if failed_count > 0:
                    failed_stocks.extend([stock.symbol for stock in batch_stocks])

                # 只在有失败时打印日志
                if failed_count > 0:
                    logger.warning(f"第{batch_idx + 1}批有{failed_count}只股票下载失败")

        # 记录下载结果
        update_info = {
            'action': 'download_all_stocks',
            'start_date': start_date,
            'end_date': end_date,
            'total_stocks': len(all_stocks),
            'success_count': success_count,
            'failed_count': len(all_stocks) - success_count,
            'failed_stocks': failed_stocks[:10]  # 只记录前10个失败的
        }
        self.storage.save_update_log(update_info)

        logger.info(f"数据下载完成！成功{success_count}只，失败{len(all_stocks) - success_count}只")

        if failed_stocks:
            logger.warning(f"失败的股票示例: {failed_stocks[:10]}")

        return success_count > 0

    def _filter_existing_stocks(self, stocks: List, start_date: str, end_date: str) -> List:
        """
        过滤已存在数据的股票（用于断点续传）

        Args:
            stocks: 股票列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            需要下载的股票列表
        """
        need_download = []

        for stock in stocks:
            min_date, max_date = self.storage.get_stock_date_range(stock.symbol)

            if min_date is None or max_date is None:
                # 没有数据，需要下载
                need_download.append(stock)
            elif min_date > start_date or max_date < end_date:
                # 数据不完整，需要补充
                need_download.append(stock)
            else:
                logger.debug(f"股票{stock.symbol}数据已存在，跳过")

        return need_download

    def _download_batch(self, stocks: List, start_date: str, end_date: str) -> int:
        """
        下载一批股票的数据

        Args:
            stocks: 股票列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            成功下载的股票数量
        """
        success_count = 0

        # 使用进度条
        with tqdm(stocks, desc="下载进度", unit="股") as pbar:
            for stock in pbar:
                pbar.set_description(f"下载 {stock.symbol[:10]}")

                try:
                    # 获取历史数据
                    data = self.fetcher.get_historical_data(
                        symbol=stock.symbol,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if not data.empty:
                        # 保存数据
                        if self.storage.save_stock_data(stock.symbol, data):
                            success_count += 1
                        else:
                            logger.warning(f"保存{stock.symbol}数据失败")
                    else:
                        logger.warning(f"股票{stock.symbol}没有数据")

                except Exception as e:
                    logger.error(f"下载股票{stock.symbol}数据时出错: {e}")
                    continue

        return success_count

    def _download_batch_silent(self, stocks: List, start_date: str, end_date: str, pbar) -> int:
        """
        静默下载一批股票的数据（不显示内部进度条，只更新外部进度条）

        Args:
            stocks: 股票列表
            start_date: 开始日期
            end_date: 结束日期
            pbar: 外部进度条对象

        Returns:
            成功下载的股票数量
        """
        success_count = 0

        for stock in stocks:
            pbar.set_description(f"下载 {stock.symbol}")

            try:
                # 获取历史数据
                data = self.fetcher.get_historical_data(
                    symbol=stock.symbol,
                    start_date=start_date,
                    end_date=end_date
                )

                if not data.empty:
                    # 保存数据
                    if self.storage.save_stock_data(stock.symbol, data):
                        success_count += 1
                    else:
                        logger.warning(f"保存{stock.symbol}数据失败")
                else:
                    logger.warning(f"股票{stock.symbol}没有数据")

            except Exception as e:
                logger.error(f"下载股票{stock.symbol}数据时出错: {e}")

            # 更新进度条
            pbar.update(1)

        return success_count

    def update_recent_data(self) -> bool:
        """
        更新最近的数据到最新的交易日
        自动计算需要更新的日期范围

        Returns:
            是否更新成功
        """
        end_date = datetime.now().strftime('%Y-%m-%d')

        # 获取已存储的股票列表
        existing_symbols = self.storage.list_available_stocks()
        if not existing_symbols:
            logger.warning("没有找到已存储的股票数据，请先运行初始下载")
            return False

        logger.info(f"需要更新{len(existing_symbols)}只股票的数据到{end_date}")

        # 计算需要更新的日期范围
        # 找到所有股票中最新的数据日期
        latest_date = None
        for symbol in existing_symbols[:10]:  # 只检查前10只股票来确定更新起始日期
            _, max_date = self.storage.get_stock_date_range(symbol)
            if max_date:
                if latest_date is None or max_date > latest_date:
                    latest_date = max_date

        if latest_date is None:
            logger.error("无法确定最新数据日期")
            return False

        # 从最新日期的下一天开始更新
        try:
            start_date_obj = datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)
            start_date = start_date_obj.strftime('%Y-%m-%d')
        except:
            start_date = latest_date

        # 如果开始日期已经是今天或之后，说明数据已经是最新的
        if start_date >= end_date:
            logger.info("数据已经是最新的，无需更新")
            return True

        logger.info(f"更新日期范围: {start_date} 到 {end_date}")
        print("开始更新数据，如果等待时间较长可能是遇到了API访问限制...")

        success_count = 0
        with tqdm(existing_symbols, desc="更新进度", unit="股") as pbar:
            for symbol in pbar:
                pbar.set_description(f"更新 {symbol}")

                try:
                    # 获取缺失的数据
                    data = self.fetcher.get_historical_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if not data.empty:
                        # 追加数据
                        if self.storage.append_stock_data(symbol, data):
                            success_count += 1

                except Exception as e:
                    logger.error(f"更新股票{symbol}数据时出错: {e}")
                    continue

        # 记录更新结果
        update_info = {
            'action': 'update_recent',
            'start_date': start_date,
            'end_date': end_date,
            'total_stocks': len(existing_symbols),
            'success_count': success_count,
            'failed_count': len(existing_symbols) - success_count
        }
        self.storage.save_update_log(update_info)

        logger.info(f"数据更新完成！成功更新{success_count}只股票")

        # 如果没有数据需要更新，也视为成功
        if success_count == 0:
            logger.info("所有股票数据都是最新的，无需更新")

        return True  # 更新操作本身成功，即使没有新数据

    def get_download_status(self) -> dict:
        """
        获取下载状态统计

        Returns:
            状态统计信息
        """
        stats = self.storage.get_storage_stats()
        latest_update = self.storage.get_latest_update_info()

        status = {
            'total_stocks': stats.get('total_stocks', 0),
            'storage_size_mb': stats.get('storage_size_mb', 0),
            'last_update': stats.get('last_update', None)
        }

        if latest_update:
            status['last_action'] = latest_update.get('action', 'unknown')
            status['last_success_count'] = latest_update.get('success_count', 0)

        return status


def main():
    """主函数"""
    # 初始化日志
    setup_logger()

    logger.info("DWAD数据下载工具启动")

    try:
        downloader = DataDownloader()

        # 显示当前状态
        status = downloader.get_download_status()
        print(f"当前状态: 已存储{status['total_stocks']}只股票，占用{status['storage_size_mb']}MB")

        # 根据配置决定运行模式
        mode = config.get('data_fetcher.mode', 'auto')

        if mode == 'initial':
            # 强制初始下载
            print("配置为强制初始下载模式，开始下载所有股票数据...")
            success = downloader.download_all_stocks_data()
        elif mode == 'update':
            # 强制更新模式
            print("配置为强制更新模式，开始更新最近数据...")
            success = downloader.update_recent_data()
        else:
            # 自动判断模式
            if status['total_stocks'] == 0:
                # 首次下载
                print("检测到首次运行，开始下载所有股票数据...")
                success = downloader.download_all_stocks_data()
            else:
                # 更新数据
                print("检测到已有数据，开始更新最近数据...")
                success = downloader.update_recent_data()

        if success:
            # 显示最终状态
            final_status = downloader.get_download_status()
            print(f"操作完成! 当前存储{final_status['total_stocks']}只股票，占用{final_status['storage_size_mb']}MB")
        else:
            print("操作失败! 请检查日志文件获取详细错误信息")
            # 显示最近的更新日志
            storage = ParquetStorage()
            latest_update = storage.get_latest_update_info()
            if latest_update:
                print(f"最近操作: {latest_update.get('action', '未知')}")
                print(f"成功: {latest_update.get('success_count', 0)}只")
                print(f"失败: {latest_update.get('failed_count', 0)}只")
                if latest_update.get('failed_stocks'):
                    print(f"失败示例: {latest_update['failed_stocks'][:3]}")

    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        print(f"错误: {e}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        print("请查看日志文件获取详细错误信息")


if __name__ == '__main__':
    main()