import pandas as pd
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from loguru import logger

from ..utils.config import config
from ..utils.timezone import today_beijing

# 掘金 API 全局锁，确保同一时间只有一个线程在调用掘金 API
# 掘金 SDK 使用全局状态，不是线程安全的
_gm_api_lock = threading.Lock()


@dataclass
class StockInfo:
    """股票基本信息"""
    symbol: str          # 股票代码 (如: SHSE.600000)
    name: str           # 股票名称
    market: str         # 市场代码 (SHSE/SZSE)
    listing_date: Optional[str] = None  # 上市日期


@dataclass
class MarketData:
    """行情数据"""
    symbol: str
    date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    turnover: float
    market_cap: Optional[float] = None


class GoldMinerFetcher:
    """掘金数据获取器"""

    def __init__(self, token: Optional[str] = None):
        """
        初始化掘金数据获取器

        Args:
            token: 掘金API token，如果为None则从配置文件获取
        """
        # 重新加载配置文件，确保获取最新配置
        config.reload()
        
        self.token = token or config.get_goldminer_token()
        if not self.token:
            raise ValueError("掘金API token未配置，请在config.yaml中设置goldminer.token")

        # 获取数据字段配置
        self.market_data_fields = config.get_market_data_fields()

        # 初始化掘金API连接
        self._init_gm_api()

    def _init_gm_api(self):
        """初始化掘金API"""
        try:
            # 导入掘金API
            from gm.api import set_token, set_serv_addr, get_symbols

            # 使用全局锁保护掘金 API 初始化，避免并发冲突
            with _gm_api_lock:
                # 设置终端服务地址（Linux环境需要指向Windows终端）
                serv_addr = config.get_goldminer_serv_addr()
                if serv_addr:
                    set_serv_addr(serv_addr)
                    logger.info(f"掘金终端地址设置为: {serv_addr}")

                # 设置token
                set_token(self.token)

                # 测试连接
                test_result = get_symbols(sec_type1=1010, sec_type2=101001, df=True)
                if test_result is None or len(test_result) == 0:
                    raise Exception("掘金API连接测试失败")

            logger.info("掘金API连接成功")

        except ImportError:
            raise ImportError("掘金SDK未安装，请运行: pip install gm3")
        except Exception as e:
            logger.error(f"掘金API初始化失败: {e}")
            raise

    def get_all_stocks(self, trade_date: Optional[str] = None) -> List[StockInfo]:
        """
        获取所有A股股票列表

        Args:
            trade_date: 交易日期，格式为'YYYY-MM-DD'，如果为None则使用当前日期

        Returns:
            股票信息列表
        """
        from gm.api import get_symbols

        if trade_date is None:
            trade_date = today_beijing()

        try:
            print(f"正在获取{trade_date}的股票列表...")

            # 使用全局锁保护掘金 API 调用
            with _gm_api_lock:
                # 获取股票列表
                stocks_df = get_symbols(
                    sec_type1=1010,  # 股票
                    sec_type2=101001,  # A股
                    trade_date=trade_date,
                    df=True
                )

            if stocks_df is None or len(stocks_df) == 0:
                logger.warning(f"未获取到{trade_date}的股票列表")
                return []

            # 转换为StockInfo对象
            stock_list = []
            for _, row in stocks_df.iterrows():
                symbol = row['symbol']

                # 提取市场信息
                if symbol.startswith('SHSE.'):
                    market = 'SHSE'
                    name = row.get('sec_name', symbol)
                elif symbol.startswith('SZSE.'):
                    market = 'SZSE'
                    name = row.get('sec_name', symbol)
                else:
                    continue  # 跳过其他市场

                stock_info = StockInfo(
                    symbol=symbol,
                    name=name,
                    market=market,
                    listing_date=row.get('list_date', None)
                )
                stock_list.append(stock_info)

            logger.info(f"获取到{len(stock_list)}只A股股票")
            return stock_list

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            print(f"获取股票列表失败: {e}")
            return []

    def get_stock_info_by_name(self, stock_name: str) -> Optional[StockInfo]:
        """
        根据股票中文名称获取股票信息

        Args:
            stock_name: 股票中文名称

        Returns:
            股票信息或None
        """
        all_stocks = self.get_all_stocks()

        for stock in all_stocks:
            if stock_name in stock.name or stock.name in stock_name:
                return stock

        logger.warning(f"未找到股票: {stock_name}")
        return None

    def get_historical_data(self,
                          symbol: str,
                          start_date: str,
                          end_date: str,
                          frequency: str = '1d') -> pd.DataFrame:
        """
        获取历史行情数据

        Args:
            symbol: 股票代码
            start_date: 开始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            frequency: 频率，默认'1d'(日线)

        Returns:
            历史行情数据DataFrame
        """
        from gm.api import history, ADJUST_PREV

        try:
            # 构建查询字段
            fields = ','.join(self.market_data_fields)

            # 使用全局锁保护掘金 API 调用
            with _gm_api_lock:
                # 获取历史数据
                history_data = history(
                    symbol=symbol,
                    frequency=frequency,
                    start_time=f"{start_date} 09:00:00",
                    end_time=f"{end_date} 15:30:00",
                    fields=fields,
                    adjust=ADJUST_PREV,  # 前复权
                    df=True
                )

            if history_data is None or len(history_data) == 0:
                logger.debug(f"股票{symbol}在{start_date}到{end_date}期间没有数据")
                return pd.DataFrame()

            # 数据预处理
            history_data['date'] = pd.to_datetime(history_data['eob']).dt.strftime('%Y-%m-%d')
            history_data['symbol'] = symbol

            # 重命名列以匹配我们的数据模型
            column_mapping = {
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price'
            }
            history_data = history_data.rename(columns=column_mapping)

            # 选择需要的列
            required_columns = ['symbol', 'date', 'open_price', 'high_price',
                              'low_price', 'close_price', 'volume']

            # 添加turnover列（如果存在）
            if 'turnover' in history_data.columns:
                required_columns.append('turnover')
            else:
                history_data['turnover'] = 0  # 如果没有成交额数据则设为0

            result_df = history_data[required_columns].copy()

            logger.debug(f"获取到{symbol}的{len(result_df)}条历史数据")
            return result_df

        except Exception as e:
            logger.error(f"获取{symbol}历史数据失败: {e}")
            print(f"获取{symbol}数据时遇到问题，可能是API访问限制，请稍等...")
            return pd.DataFrame()

    def batch_get_historical_data(self,
                                 stock_list: List[StockInfo],
                                 start_date: str,
                                 end_date: str) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票的历史数据

        Args:
            stock_list: 股票信息列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            字典，键为股票代码，值为历史数据DataFrame
        """
        result = {}
        total_count = len(stock_list)

        logger.info(f"开始批量获取{total_count}只股票的历史数据")
        print(f"批量下载{total_count}只股票数据，如果进度缓慢可能是遇到了API访问限制...")

        for i, stock in enumerate(stock_list, 1):
            try:
                if i % 10 == 0:  # 每10只股票打印一次进度
                    print(f"已处理 {i}/{total_count} 只股票...")

                df = self.get_historical_data(stock.symbol, start_date, end_date)
                if not df.empty:
                    result[stock.symbol] = df
                else:
                    logger.debug(f"股票{stock.symbol}未获取到数据")

            except Exception as e:
                logger.error(f"获取股票{stock.symbol}数据时出错: {e}")
                continue

        logger.info(f"批量获取完成，成功获取{len(result)}只股票的数据")
        return result