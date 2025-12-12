#!/usr/bin/env python3
"""DWAD Flask 仪表盘服务

基于现有多周期排名报告界面，提供一个轻量级 Web 服务：
- 首页直接渲染最新的多周期实时排名图表
- 预留 API：下载数据、计算指数、更新排名

使用方法：
    python dashboard_server.py
    然后在浏览器中打开 http://127.0.0.1:8818/
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import json
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, Response, jsonify, request
from loguru import logger

# 计算项目根目录和源码目录
ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# 复用现有业务逻辑
from dwad.analysis.index_comparator import IndexComparator
from dwad.visualization.ranking_visualizer import RankingVisualizer
from dwad.tools.data_downloader import main as download_data_main
from dwad.analysis.index_calculator import main as calculate_index_main
from dwad.data_storage.parquet_storage import ParquetStorage
from dwad.data_fetcher.realtime_price_fetcher import RealtimePriceFetcher
from dwad.analysis.stock_alerts import StockAlertEngine
from dwad.utils.logger import setup_logger


app = Flask(__name__)


# 个股预警相关全局对象
_alert_engine: StockAlertEngine | None = None
_scheduler: BackgroundScheduler | None = None
_ALERT_DETECTION_JOB_ID = "stock_alert_detection"


def get_alert_engine() -> StockAlertEngine:
    """惰性初始化并返回全局个股预警引擎。"""

    global _alert_engine
    if _alert_engine is None:
        _alert_engine = StockAlertEngine()
    return _alert_engine


def init_logger() -> None:
    """初始化日志系统。

    Flask 3.1 已移除 before_first_request 装饰器，这里改为在 main() 中
    显式调用该函数，以复用现有 loguru 日志配置。
    """
    setup_logger()
    logger.info("DWAD Flask 仪表盘服务启动")


def init_stock_alert_scheduler() -> None:
    """初始化个股预警定时任务。

    根据配置中的 stock_alerts.push.check_interval_minutes 设置检测周期。
    检测时间窗口：9:35 - 15:05（北京时间），其他时间不执行检测。
    """

    global _scheduler

    engine = get_alert_engine()
    cfg = engine.get_push_config()
    interval = int(cfg.get("check_interval_minutes", 5) or 5)
    if interval <= 0:
        interval = 5

    if _scheduler is None:
        _scheduler = BackgroundScheduler()
    else:
        try:
            _scheduler.remove_job(_ALERT_DETECTION_JOB_ID)
        except Exception:
            # 如果不存在旧任务，忽略错误
            pass

    def _job_wrapper() -> None:
        """执行预警检测，仅在交易时间窗口内运行。"""
        try:
            from src.dwad.utils.timezone import now_beijing
            now = now_beijing()
            current_time = now.time()
            
            # 交易时间窗口：9:35 - 15:05（北京时间）
            from datetime import time as dt_time
            start_time = dt_time(9, 35)
            end_time = dt_time(15, 5)
            
            if not (start_time <= current_time <= end_time):
                logger.debug("当前时间 {} 不在预警检测窗口 (09:35-15:05)，跳过本次检测", 
                           current_time.strftime("%H:%M:%S"))
                return
            
            logger.info("执行定时预警检测 (当前北京时间: {})", now.strftime("%H:%M:%S"))
            engine.run_detection_cycle()
        except Exception:
            logger.exception("执行个股预警检测任务失败")

    _scheduler.add_job(
        _job_wrapper,
        "interval",
        minutes=interval,
        id=_ALERT_DETECTION_JOB_ID,
        replace_existing=True,
    )

    if not _scheduler.running:
        _scheduler.start()

    logger.info("个股预警定时任务已启动/更新，检测间隔 = {} 分钟，时间窗口 = 09:35-15:05", interval)


def _sector_snapshot_cache_path() -> Path:
    """返回板块排名快照缓存文件路径。"""

    return ROOT_DIR / "reports" / "sector_ranking_snapshot.json"


def _ranking_data_cache_path() -> Path:
    """返回多周期排名数据缓存文件路径。"""

    return ROOT_DIR / "reports" / "ranking_data_cache.json"


class _DateTimeEncoder(json.JSONEncoder):
    """JSON 编码器，支持 datetime 对象序列化。"""

    def default(self, obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


def _save_ranking_data_cache(data: Dict[str, Any]) -> None:
    """将多周期排名数据保存到缓存文件（失败时仅记录日志，不抛异常）。"""

    try:
        reports_dir = ROOT_DIR / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        cache_path = _ranking_data_cache_path()
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, cls=_DateTimeEncoder)
        logger.info("多周期排名数据已写入缓存: {}", cache_path)
    except Exception:
        logger.exception("写入多周期排名数据缓存失败")


def _load_ranking_data_cache() -> Dict[str, Any]:
    """从缓存文件读取多周期排名数据，失败时返回空字典。
    
    读取缓存时静默操作，不打印日志。
    """
    cache_path = _ranking_data_cache_path()
    if not cache_path.exists():
        return {}

    try:
        with cache_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    return {}


def _save_sector_ranking_cache(data: List[Dict[str, Any]]) -> None:
    """将板块排名快照保存到缓存文件（失败时仅记录日志，不抛异常）。"""

    try:
        reports_dir = ROOT_DIR / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        cache_path = _sector_snapshot_cache_path()
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        logger.info("板块排名快照已写入缓存: {}", cache_path)
    except Exception:
        logger.exception("写入板块排名快照缓存失败")


def _load_sector_ranking_cache() -> List[Dict[str, Any]]:
    """从缓存文件读取板块排名快照，失败时返回空列表并记录日志。"""

    cache_path = _sector_snapshot_cache_path()
    if not cache_path.exists():
        logger.info("未找到板块排名缓存文件: {}", cache_path)
        return []

    try:
        with cache_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        logger.error("板块排名缓存格式错误，期望list，实际为: {}", type(data))
    except Exception:
        logger.exception("读取板块排名缓存失败")

    return []


def rebuild_multi_period_page(enable_realtime: bool = True) -> bool:
    """重新计算并生成多周期实时排名页面 HTML 文件，并刷新板块排名缓存。

    为了尽量少改动现有可视化代码，这里沿用 RankingVisualizer 的
    generate_html 逻辑：

    1. 使用 IndexComparator 计算多周期排名数据（含实时数据）
    2. 调用 RankingVisualizer.generate_html() 生成 HTML 到 reports 目录
    3. 使用同一批指数数据构造板块排名快照，并写入缓存文件

    返回值表示是否生成成功，具体错误请查看日志文件。
    """
    comparator = IndexComparator(enable_realtime=enable_realtime)

    # 1. 加载指数数据
    if not comparator.load_indices_data():
        logger.error("加载指数数据失败")
        return False

    # 2. 生成多周期排名数据
    periods = [20, 55, 233]
    ranking_data = comparator.get_ranking_data_for_visualization(
        periods=periods,
        include_realtime=enable_realtime,
        clear_realtime_cache=False,  # 保留实时价格缓存，后续板块排名复用
    )

    if not ranking_data or "periods" not in ranking_data:
        logger.error("无法生成多周期排名数据")
        return False

    # 指定输出文件名（只作为中转文件使用）
    if "config" not in ranking_data:
        ranking_data["config"] = {}
    ranking_data["config"]["output_filename"] = "index_ranking_dashboard.html"

    # 3. 生成 HTML 文件
    visualizer = RankingVisualizer()
    ok = visualizer.generate_html(ranking_data)
    if not ok:
        logger.error("生成可视化页面失败")
        return False

    reports_dir = ROOT_DIR / "reports"
    html_path = reports_dir / "index_ranking_dashboard.html"
    if not html_path.exists():
        logger.error("生成的图表文件不存在: {}", html_path)
        return False

    # 4. 使用同一批指数数据构造板块排名快照并写入缓存
    try:
        logger.info("开始基于最新指数数据构造板块排名快照（随更新排名一并刷新）")
        # 复用上面用于生成多周期图表的 comparator 实例及其实时价格缓存，
        # 避免为板块排名再次重复获取相同的实时行情数据。
        snapshot = build_sector_ranking_snapshot(
            comparator=comparator,
            use_cache_for_realtime=True,
        )
        _save_sector_ranking_cache(snapshot)
    except Exception:
        # 这里记录错误但不影响整体更新流程
        logger.exception("刷新板块排名快照缓存时发生异常")

    # 5. 保存多周期排名数据缓存（供 /api/ranking_data 直接读取，避免重复计算）
    try:
        _save_ranking_data_cache(ranking_data)
    except Exception:
        logger.exception("保存多周期排名数据缓存时发生异常")

    return True


def build_multi_period_page(enable_realtime: bool = True, force_dynamic: bool = False) -> str:
    """构造多周期实时排名页面的 HTML 字符串。

    Args:
        enable_realtime: 是否启用实时数据
        force_dynamic: 是否强制动态生成（用于开发模式，每次请求都重新生成模板）

    优先读取上一次生成的报表 HTML，如果不存在则触发一次重建。
    如果 force_dynamic=True，则每次都重新生成模板（但数据仍从缓存读取）。
    """
    reports_dir = ROOT_DIR / "reports"
    html_path = reports_dir / "index_ranking_dashboard.html"

    # 开发模式：每次请求都重新生成模板
    if force_dynamic or not html_path.exists():
        if not html_path.exists():
            logger.info("未找到现有报表，正在重新生成多周期页面")
        else:
            logger.debug("动态模式：重新生成多周期页面模板")
        success = rebuild_multi_period_page(enable_realtime=enable_realtime)
        if not success:
            return "<h3>生成图表页面失败，请检查日志文件。</h3>"

    try:
        return html_path.read_text(encoding="utf-8")
    except Exception:
        logger.exception("读取图表页面失败")
        return "<h3>读取图表页面失败，请检查日志文件。</h3>"


def build_sector_ranking_snapshot(
    comparator: IndexComparator | None = None,
    use_cache_for_realtime: bool = False,
) -> List[Dict[str, Any]]:
    """构造板块（股池）层面的排名快照数据。

    返回的数据用于前端“板块排名”表格展示，包含：
    - name: 板块名称（display_name）
    - index_value: 当前指数点位（优先使用实时指数，否则使用最新历史值）
    - daily_pct: 当日实时涨幅（相对于昨日收盘），可能为 None
    - r20 / r55 / r233: 近20/55/233个交易日涨跌幅
    - since_start: 从 comparison_start_date 起点到当前的涨跌幅
    """

    if comparator is None:
        comparator = IndexComparator(enable_realtime=True)

    # 加载指数数据（如果尚未加载）
    if not comparator.indices_data:
        if not comparator.load_indices_data():
            logger.error("构造板块排名快照失败：未能加载指数数据")
            return []

    # 比较起始日期（用于计算 since_start）
    start_date_str = comparator.comparison_config.get("comparison_start_date")
    start_ts = pd.to_datetime(start_date_str) if start_date_str else None

    # 获取当日实时涨幅（如果实时功能可用）
    realtime = None
    realtime_rankings: Dict[str, Any] = {}
    if comparator.enable_realtime and comparator.realtime_fetcher is not None:
        realtime = comparator.get_realtime_ranking(period=None, use_cache=use_cache_for_realtime)
        realtime_rankings = realtime.get("rankings", {}) if realtime else {}

    results: List[Dict[str, Any]] = []

    for _, idx_info in comparator.indices_data.items():
        display_name = idx_info["display_name"]
        data = idx_info["data"].copy()
        if data.empty:
            continue

        # 确保日期为 datetime，并按日期排序
        data["date"] = pd.to_datetime(data["date"])
        data = data.sort_values("date")

        last_row = data.iloc[-1]
        last_value_hist = float(last_row["index_value"])
        current_value = last_value_hist

        # 实时数据（若有）
        daily_pct = None
        rt_info = realtime_rankings.get(display_name)
        if rt_info:
            # 使用实时指数作为当前点位；当日涨幅按 "实时指数 / 昨日指数 - 1" 计算为小数
            current_value = float(rt_info.get("index_value", last_value_hist))
            if last_value_hist > 0:
                daily_pct = current_value / last_value_hist - 1.0

        def calc_period_return(days: int) -> Any:
            """计算近 N 个交易日涨跌幅，若数据不足则返回 None。"""
            if len(data) > days:
                base_val = float(data.iloc[-(days + 1)]["index_value"])
                if base_val > 0:
                    return current_value / base_val - 1.0
            return None

        r20 = calc_period_return(20)
        r55 = calc_period_return(55)
        r233 = calc_period_return(233)

        since_start = None
        if start_ts is not None:
            sub = data[data["date"] >= start_ts]
            if not sub.empty:
                base_val = float(sub.iloc[0]["index_value"])
                if base_val > 0:
                    since_start = current_value / base_val - 1.0

        results.append(
            {
                "name": display_name,
                "index_value": current_value,
                "daily_pct": daily_pct,
                "r20": r20,
                "r55": r55,
                "r233": r233,
                "since_start": since_start,
            }
        )

    return results


def build_stock_ranking_for_sector(sector_name: str, start_ts=None) -> List[Dict[str, Any]]:
    """构造某个板块内个股层面的排名数据。

    优先使用实时价格（如获取失败则回退到本地历史数据）。

    返回的数据结构与板块排名尽量保持一致：
    - symbol: 股票代码
    - name: 股票名称
    - index_value: 当前价格（实时价，若不可用则为最新收盘价）
    - daily_pct: 当日涨幅（实时价相对于最近一次历史收盘价）
    - r20 / r55 / r233: 近20/55/233个交易日涨跌幅
    - since_start: 自起点以来涨跌幅（默认 comparison_start_date，可传入自定义 start_ts）
    """

    comparator = IndexComparator(enable_realtime=False)

    # 1. 根据展示名称找到对应的 pool_name / concept_name
    indices_cfg = comparator.comparison_config.get("indices_to_compare", [])
    pool_name: str | None = None
    concept_name: str | None = None

    for item in indices_cfg:
        cfg_display = item.get("display_name", item.get("concept_name"))
        if cfg_display == sector_name:
            pool_name = item.get("pool_name")
            concept_name = item.get("concept_name")
            break

    if not pool_name or not concept_name:
        logger.error("未在比较配置中找到板块名称对应的股池配置: {}", sector_name)
        return []

    # 2. 通过股池配置加载该板块内的股票代码
    symbols = comparator._load_stock_pool_config(pool_name, concept_name)
    if not symbols:
        logger.warning("股池 [{} - {}] 没有可用股票", pool_name, concept_name)
        return []

    # 2.1 获取该股池内所有股票的实时价格（失败时记录日志并回退到历史价格）
    realtime_prices: Dict[str, float] = {}
    try:
        price_fetcher = RealtimePriceFetcher()
        prices_dict, _ = price_fetcher.get_pool_current_prices(symbols)
        if prices_dict:
            for sym, info in prices_dict.items():
                try:
                    realtime_prices[sym] = float(info.price)
                except (TypeError, ValueError):
                    continue
    except Exception:
        logger.exception("获取个股实时价格失败，将回退到历史收盘价")

    # 3. 加载股票基本信息用于 symbol -> name 映射
    stock_info_df = comparator.storage.load_stock_info()
    symbol_to_name: Dict[str, str] = {}
    if not stock_info_df.empty:
        for _, row in stock_info_df.iterrows():
            sym = str(row.get("symbol")) if row.get("symbol") is not None else None
            nm = str(row.get("name")) if row.get("name") is not None else None
            if sym:
                symbol_to_name[sym] = nm or sym

    # 4. 起点日期：若未显式传入，则使用 comparison_start_date
    if start_ts is None:
        start_date_str = comparator.comparison_config.get("comparison_start_date")
        if start_date_str:
            try:
                start_ts = pd.to_datetime(start_date_str)
            except Exception:
                start_ts = None

    results: List[Dict[str, Any]] = []

    for symbol in symbols:
        df = comparator.storage.load_stock_data(symbol)
        if df.empty:
            continue

        if "date" not in df.columns:
            continue

        price_col = None
        if "close_price" in df.columns:
            price_col = "close_price"
        elif "close" in df.columns:
            price_col = "close"

        if price_col is None:
            # 找不到价格列，跳过该股票
            logger.warning("股票 {} 缺少价格列 (close_price/close)", symbol)
            continue

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        if df.empty:
            continue

        last_row = df.iloc[-1]
        try:
            last_hist_price = float(last_row[price_col])
        except (TypeError, ValueError):
            continue

        # 当前价格：优先使用实时价；没有实时价则使用最新历史收盘价
        rt_price = realtime_prices.get(symbol)
        if rt_price is not None and rt_price > 0:
            current_value = rt_price
        else:
            current_value = last_hist_price

        # 当日涨幅：仅在存在实时价时计算，= 实时价 / 最近历史收盘价 - 1
        daily_pct = None
        if rt_price is not None and rt_price > 0 and last_hist_price > 0:
            try:
                daily_pct = rt_price / last_hist_price - 1.0
            except (TypeError, ValueError):
                daily_pct = None

        def calc_period_return(days: int):
            # 以当前价格（实时价/最新价）相对于N个交易日前的收盘价计算涨跌幅
            if len(df) > days:
                try:
                    base_val = float(df.iloc[-(days + 1)][price_col])
                except (TypeError, ValueError):
                    return None
                if base_val > 0:
                    return current_value / base_val - 1.0
            return None

        r20 = calc_period_return(20)
        r55 = calc_period_return(55)
        r233 = calc_period_return(233)

        # 自起点以来涨幅
        since_start = None
        if start_ts is not None:
            sub = df[df["date"] >= start_ts]
            if not sub.empty:
                try:
                    base_val = float(sub.iloc[0][price_col])
                except (TypeError, ValueError):
                    base_val = 0.0
                if base_val > 0:
                    since_start = current_value / base_val - 1.0

        results.append(
            {
                "symbol": symbol,
                "name": symbol_to_name.get(symbol, symbol),
                "index_value": current_value,
                "daily_pct": daily_pct,
                "r20": r20,
                "r55": r55,
                "r233": r233,
                "since_start": since_start,
            }
        )

    return results


@app.get("/")
def index() -> Response:
    """首页：返回当前最新的多周期实时排名页面。
    
    查询参数：
        dev=1: 开发模式，每次请求都重新生成模板（用于前端代码调试）
    """
    # 开发模式：通过 ?dev=1 参数启用，每次请求都重新生成模板
    force_dynamic = request.args.get("dev", "0") == "1"
    html = build_multi_period_page(enable_realtime=True, force_dynamic=force_dynamic)
    return Response(html, mimetype="text/html")


@app.post("/api/download_data")
def api_download_data():  # type: ignore[override]
    """触发历史数据下载 / 全量刷新。

    实际逻辑由 dwad.tools.data_downloader.main 决定，
    这里只返回是否成功，详细过程请查看日志文件。
    """
    try:
        success = download_data_main()
        return jsonify({"ok": bool(success)})
    except Exception as e:  # pragma: no cover - 防御性日志
        logger.exception("下载数据失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/download_status")
def api_download_status():  # type: ignore[override]
    """返回最近一次下载任务的摘要状态，用于前端展示类似 X/Y 的进度信息。

    数据来源：ParquetStorage.metadata_path 下的 update_log.json，
    该文件由 DataDownloader.save_update_log() 持久化最近的下载或全量刷新结果。
    """
    try:
        storage = ParquetStorage()
        stats = storage.get_storage_stats()
        latest = storage.get_latest_update_info() or {}

        # 为前端准备一个相对稳定的结构
        resp: Dict[str, Any] = {
            "ok": True,
            "stats": stats,
            "latest_update": latest,
        }
        return jsonify(resp)
    except Exception as e:  # pragma: no cover - 防御性日志
        logger.exception("获取下载状态失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/calculate_index")
def api_calculate_index():  # type: ignore[override]
    """触发指数计算。"""
    try:
        success = calculate_index_main()
        return jsonify({"ok": bool(success)})
    except Exception as e:  # pragma: no cover - 防御性日志
        logger.exception("计算指数失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/update_ranking")
def api_update_ranking():  # type: ignore[override]
    """触发排名更新。

    调用 rebuild_multi_period_page 重新拉取实时数据并生成最新报表，
    前端在成功后刷新页面即可看到最新结果。
    """
    try:
        success = rebuild_multi_period_page(enable_realtime=True)
        return jsonify({"ok": bool(success)})
    except Exception as e:  # pragma: no cover - 防御性日志
        logger.exception("更新排名失败")
        return jsonify({"ok": False, "error": str(e)}), 500


def _build_ranking_traces(ranking_data: Dict[str, Any]) -> Dict[str, Any]:
    """将排名数据转换为前端需要的 traces 格式。

    Args:
        ranking_data: 由 IndexComparator.get_ranking_data_for_visualization() 生成的数据

    Returns:
        包含 periods、total_indices、realtime_timestamp 的字典
    """
    from dwad.visualization.ranking_visualizer import RankingVisualizer
    visualizer = RankingVisualizer()

    all_periods_data = []
    realtime_timestamp = None

    for period_data in ranking_data["periods"]:
        traces = []
        series_list = period_data["series"]
        period = period_data["period"]

        first_series = series_list[0]
        dates = first_series["dates"][1:] if len(first_series["dates"]) > 1 else first_series["dates"]

        for idx, series in enumerate(series_list):
            color = visualizer.colors[idx % len(visualizer.colors)]
            series_dates = series["dates"][1:] if len(series["dates"]) > 1 else series["dates"]
            ranks = series["ranks"][1:] if len(series["ranks"]) > 1 else series["ranks"]
            changes = series["changes"][1:] if len(series["changes"]) > 1 else series["changes"]
            index_values = series["index_values"][1:] if len(series["index_values"]) > 1 else series["index_values"]
            base_values = series["base_values"][1:] if len(series["base_values"]) > 1 else series["base_values"]
            base_dates = series["base_dates"][1:] if len(series["base_dates"]) > 1 else series["base_dates"]

            x_values = list(range(len(ranks)))

            customdata = []
            for date, change, idx_val, base_date, base_val in zip(series_dates, changes, index_values, base_dates, base_values):
                customdata.append([date, change, idx_val, base_date, base_val])

            trace = {
                "x": x_values,
                "y": ranks,
                "name": series["name"],
                "type": "scatter",
                "mode": "lines",
                "line": {"width": 2, "color": color},
                "legendgroup": series["name"],
                "customdata": customdata,
                "hovertemplate": f"<b>{series['name']}</b><br>" +
                                "%{customdata[0]}: %{customdata[2]:.2f}<br>" +
                                "%{customdata[3]}: %{customdata[4]:.2f}<br>" +
                                "排名: %{y}<br>" +
                                f"近{period}日涨跌幅: %{{customdata[1]:.2f}}%<br>" +
                                "<extra></extra>"
            }
            traces.append(trace)

        # 添加实时数据
        realtime_data = period_data.get("realtime")
        period_dates = dates[:]
        if realtime_data:
            realtime_rankings = realtime_data.get("rankings", {})
            realtime_timestamp = realtime_data.get("timestamp")

            if realtime_rankings:
                for idx, series in enumerate(series_list):
                    name = series["name"]
                    if name in realtime_rankings:
                        color = visualizer.colors[idx % len(visualizer.colors)]
                        last_x = len(series["ranks"][1:]) - 1 if len(series["ranks"]) > 1 else 0
                        last_rank = series["ranks"][-1]

                        rt_info = realtime_rankings[name]
                        realtime_rank = rt_info["rank"]
                        realtime_change = rt_info["change_pct"]
                        # 当日实时涨幅（相对于昨日收盘）
                        realtime_today_change = rt_info.get("today_change_pct")
                        realtime_index = rt_info["index_value"]
                        base_value = rt_info["base_value"]
                        base_date = rt_info["base_date"]

                        # 实时连接线（添加 is_realtime 和 realtime_today_change 供前端使用）
                        realtime_trace = {
                            "x": [last_x, last_x + 1],
                            "y": [last_rank, realtime_rank],
                            "name": name,
                            "type": "scatter",
                            "mode": "lines",
                            "line": {"width": 1, "color": color},
                            "showlegend": False,
                            "legendgroup": name,
                            "hoverinfo": "skip",
                            "is_realtime": True,
                            "realtime_change": realtime_change,
                            "realtime_today_change": realtime_today_change,
                        }
                        traces.append(realtime_trace)

                        # 实时数据点
                        marker_trace = {
                            "x": [last_x + 1],
                            "y": [realtime_rank],
                            "name": name,
                            "type": "scatter",
                            "mode": "markers",
                            "marker": {"size": 8, "color": color, "symbol": "circle"},
                            "showlegend": False,
                            "legendgroup": name,
                            "customdata": [[
                                str(realtime_timestamp)[:10] if realtime_timestamp else "实时",
                                realtime_change,
                                realtime_index,
                                base_date,
                                base_value
                            ]],
                            "hovertemplate": f"<b>{name} (实时)</b><br>" +
                                            "%{customdata[0]}: %{customdata[2]:.2f}<br>" +
                                            "%{customdata[3]}: %{customdata[4]:.2f}<br>" +
                                            "排名: %{y}<br>" +
                                            f"近{period}日涨跌幅: %{{customdata[1]:.2f}}%<br>" +
                                            "<extra></extra>"
                        }
                        traces.append(marker_trace)

                period_dates = period_dates + ["实时"]

        all_periods_data.append({
            "period": period,
            "title": period_data["title"],
            "traces": traces,
            "dates": period_dates
        })

    return {
        "periods": all_periods_data,
        "total_indices": ranking_data["total_indices"],
        "realtime_timestamp": str(realtime_timestamp) if realtime_timestamp else None
    }


@app.get("/api/ranking_data")
def api_ranking_data():  # type: ignore[override]
    """返回多周期排名图表数据（JSON格式）。

    该接口用于前端动态加载图表数据，无需刷新页面即可更新图表。
    优先读取 /api/update_ranking 生成的缓存数据，避免重复计算和获取实时数据。
    """
    try:
        # 优先读取缓存（静默读取，不打印日志）
        ranking_data = _load_ranking_data_cache()
        if ranking_data and "periods" in ranking_data:
            result = _build_ranking_traces(ranking_data)
            return jsonify({"ok": True, "data": result})

        # 缓存不存在，回退到重新计算（首次访问或缓存被清除时）
        logger.warning("多周期排名数据缓存不存在，正在重新计算...")
        comparator = IndexComparator(enable_realtime=True)

        if not comparator.load_indices_data():
            logger.error("加载指数数据失败")
            return jsonify({"ok": False, "error": "加载指数数据失败"}), 500

        periods = [20, 55, 233]
        ranking_data = comparator.get_ranking_data_for_visualization(
            periods=periods,
            include_realtime=True,
            clear_realtime_cache=True,
        )

        if not ranking_data or "periods" not in ranking_data:
            logger.error("无法生成多周期排名数据")
            return jsonify({"ok": False, "error": "无法生成排名数据"}), 500

        # 保存到缓存供下次使用
        _save_ranking_data_cache(ranking_data)

        result = _build_ranking_traces(ranking_data)
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        logger.exception("获取排名数据失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/sector_ranking")
def api_sector_ranking():  # type: ignore[override]
    """返回板块排名快照数据，用于“板块排名”Tab 表格。

    默认优先读取最近一次“更新排名”时生成的缓存结果：
    - 当用户点击“更新排名”按钮时，会调用 /api/update_ranking，
      该接口内部会重新生成多周期报表并刷新板块排名缓存文件；
    - 本接口首先尝试从缓存文件读取上一轮结果；
    - 如果缓存不存在或读取失败，则回退到即时计算一次并写回缓存。

    返回值为列表，每一项代表一个板块：
    [{
        "name": str,
        "index_value": float,
        "daily_pct": float | null,
        "r20": float | null,
        "r55": float | null,
        "r233": float | null,
        "since_start": float | null,
    }, ...]
    """
    try:
        # 1. 优先尝试读取缓存（上一轮“更新排名”的结果”）
        cached = _load_sector_ranking_cache()
        if cached:
            return jsonify(cached)

        # 2. 若没有缓存，则即时构造一次并写入缓存
        data = build_sector_ranking_snapshot()
        if data:
            _save_sector_ranking_cache(data)
        return jsonify(data)
    except Exception as e:  # pragma: no cover - 防御性日志
        logger.exception("生成板块排名失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/sector_ranking_from_date")
def api_sector_ranking_from_date():
    try:
        payload = request.get_json(silent=True) or {}
        start_date_str = payload.get("start_date")
        if not start_date_str:
            return jsonify({"ok": False, "error": "missing start_date"}), 400

        start_ts = pd.to_datetime(start_date_str, errors="coerce")
        if pd.isna(start_ts):
            return jsonify({"ok": False, "error": "invalid start_date"}), 400

        cached = _load_sector_ranking_cache()
        if not cached:
            return jsonify({"ok": False, "error": "no cached sector ranking, please click '更新排名' first"}), 400

        comparator = IndexComparator(enable_realtime=False)
        if not comparator.load_indices_data():
            logger.error("加载指数数据失败（自定义起始日期）")
            return jsonify({"ok": False, "error": "failed to load index data"}), 500

        index_map: Dict[str, Any] = {}
        for _, idx_info in comparator.indices_data.items():
            display_name = idx_info["display_name"]
            data = idx_info["data"].copy()
            if data.empty:
                continue
            data["date"] = pd.to_datetime(data["date"])
            data = data.sort_values("date")
            index_map[display_name] = data

        updated: List[Dict[str, Any]] = []
        for row in cached:
            name = row.get("name")
            new_row = dict(row)
            data = index_map.get(name)
            new_since = None
            if data is not None:
                sub = data[data["date"] >= start_ts]
                if not sub.empty:
                    base_val = float(sub.iloc[0]["index_value"])
                    if base_val > 0:
                        current_val = row.get("index_value")
                        try:
                            current_value = float(current_val)
                        except (TypeError, ValueError):
                            current_value = float(sub.iloc[-1]["index_value"])
                        new_since = current_value / base_val - 1.0
            new_row["since_start"] = new_since
            updated.append(new_row)

        return jsonify(updated)
    except Exception as e:
        logger.exception("生成自定义起始日期的板块排名失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/sector_stock_ranking")
def api_sector_stock_ranking():
    try:
        payload = request.get_json(silent=True) or {}
        sector_name = payload.get("sector_name")
        if not sector_name:
            return jsonify({"ok": False, "error": "missing sector_name"}), 400

        start_ts = None
        start_date_str = payload.get("start_date")
        if start_date_str:
            start_ts = pd.to_datetime(start_date_str, errors="coerce")
            if pd.isna(start_ts):
                return jsonify({"ok": False, "error": "invalid start_date"}), 400

        data = build_stock_ranking_for_sector(sector_name, start_ts=start_ts)
        return jsonify(data)
    except Exception as e:
        logger.exception("生成板块内个股排名失败")
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# 个股预警相关 API
# ---------------------------------------------------------------------------


@app.get("/api/stock_alerts/config")
def api_stock_alerts_config():  # type: ignore[override]
    """获取个股预警配置（自选列表 + 推送参数）。"""

    try:
        engine = get_alert_engine()
        data = engine.get_watchlists_with_names()
        return jsonify({"ok": True, "data": data})
    except Exception as e:  # pragma: no cover - 防御性日志
        logger.exception("获取个股预警配置失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/add")
def api_stock_alerts_add():  # type: ignore[override]
    """向女星股/金店股列表中增加一只股票。

    请求 JSON: {"rule": "nuxing"|"jindian", "query": "中文名或代码"}
    
    添加成功后会立刻触发一次预警检测（异步执行，不阻塞响应）。
    """

    try:
        payload = request.get_json(silent=True) or {}
        rule = payload.get("rule")
        query = payload.get("query")
        if rule not in ("nuxing", "jindian") or not query:
            return jsonify({"ok": False, "error": "参数错误: 需要 rule(nuxing/jindian) 和 query"}), 400

        engine = get_alert_engine()
        ok, info = engine.add_symbol(rule, str(query))
        if not ok:
            return jsonify({"ok": False, **info}), 400

        return jsonify({"ok": True, "data": info})
    except Exception as e:  # pragma: no cover
        logger.exception("新增个股预警标的失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/remove")
def api_stock_alerts_remove():  # type: ignore[override]
    """从女星股/金店股列表中删除一只股票。

    请求 JSON: {"rule": "nuxing"|"jindian", "symbol": "SHSE.600000"}
    """

    try:
        payload = request.get_json(silent=True) or {}
        rule = payload.get("rule")
        symbol = payload.get("symbol")
        if rule not in ("nuxing", "jindian") or not symbol:
            return jsonify({"ok": False, "error": "参数错误: 需要 rule(nuxing/jindian) 和 symbol"}), 400

        engine = get_alert_engine()
        ok, msg = engine.remove_symbol(rule, str(symbol))
        if not ok:
            return jsonify({"ok": False, "error": msg}), 400
        
        # 移除股票时自动删除对应的预警记录
        engine.delete_alerts_by_symbol(str(symbol))
        
        return jsonify({"ok": True})
    except Exception as e:  # pragma: no cover
        logger.exception("删除个股预警标的失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/push_config")
def api_stock_alerts_push_config():  # type: ignore[override]
    """更新预警推送参数，并重置检测定时任务间隔。"""

    try:
        payload = request.get_json(silent=True) or {}
        engine = get_alert_engine()
        new_cfg = engine.update_push_config(payload or {})

        # 重新初始化调度任务使新间隔生效
        init_stock_alert_scheduler()

        return jsonify({"ok": True, "data": new_cfg})
    except Exception as e:  # pragma: no cover
        logger.exception("更新个股预警推送配置失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/stock_alerts/alerts")
def api_stock_alerts_alerts():  # type: ignore[override]
    """获取当前所有个股预警记录。

    可选查询参数 only_active=true 时，仅返回未确认的预警。
    """

    try:
        only_active = str(request.args.get("only_active", "false")).lower() in {"1", "true", "yes"}
        engine = get_alert_engine()
        rows = engine.list_alerts(only_active=only_active)
        return jsonify({"ok": True, "data": rows})
    except Exception as e:  # pragma: no cover
        logger.exception("获取个股预警记录失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/stock_alerts/alerts_to_push")
def api_stock_alerts_alerts_to_push():  # type: ignore[override]
    """获取本次需要推送的个股预警列表。

    调用过程中会自动更新 last_push_time / push_count。
    前端轮询调用该接口用于“新预警提醒”。
    """

    try:
        engine = get_alert_engine()
        rows = engine.get_alerts_to_push()
        return jsonify({"ok": True, "data": rows})
    except Exception as e:  # pragma: no cover
        logger.exception("获取需要推送的个股预警失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/ack")
def api_stock_alerts_ack():  # type: ignore[override]
    """确认收到单条预警，后续不再推送该条记录。"""

    try:
        payload = request.get_json(silent=True) or {}
        alert_id = payload.get("id") or payload.get("alert_id")
        if not alert_id:
            return jsonify({"ok": False, "error": "缺少预警ID(id)"}), 400

        engine = get_alert_engine()
        ok = engine.ack_alert(str(alert_id))
        if not ok:
            return jsonify({"ok": False, "error": "预警不存在或已被删除"}), 400
        return jsonify({"ok": True})
    except Exception as e:  # pragma: no cover
        logger.exception("确认个股预警失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/run_detection")
def api_stock_alerts_run_detection():  # type: ignore[override]
    """手动触发一次预警检测（不受交易时间窗口限制）。"""

    try:
        engine = get_alert_engine()
        logger.info("手动触发预警检测")
        engine.run_detection_cycle()
        return jsonify({"ok": True, "message": "检测完成"})
    except Exception as e:  # pragma: no cover
        logger.exception("手动触发预警检测失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/stock_alerts/scheduler_status")
def api_stock_alerts_scheduler_status():  # type: ignore[override]
    """获取预警定时任务状态（下次执行时间、检测间隔等）。"""

    try:
        global _scheduler
        result = {
            "running": False,
            "next_run_time": None,
            "check_interval_minutes": 5,
        }

        engine = get_alert_engine()
        cfg = engine.get_push_config()
        result["check_interval_minutes"] = int(cfg.get("check_interval_minutes", 5) or 5)

        if _scheduler is not None and _scheduler.running:
            result["running"] = True
            try:
                job = _scheduler.get_job(_ALERT_DETECTION_JOB_ID)
                if job and job.next_run_time:
                    result["next_run_time"] = job.next_run_time.isoformat()
            except Exception:
                pass

        return jsonify({"ok": True, "data": result})
    except Exception as e:  # pragma: no cover
        logger.exception("获取预警定时任务状态失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/delete")
def api_stock_alerts_delete():  # type: ignore[override]
    """删除单条预警记录。"""

    try:
        payload = request.get_json(silent=True) or {}
        alert_id = payload.get("id") or payload.get("alert_id")
        if not alert_id:
            return jsonify({"ok": False, "error": "缺少预警ID(id)"}), 400

        engine = get_alert_engine()
        ok = engine.delete_alert(str(alert_id))
        if not ok:
            return jsonify({"ok": False, "error": "预警不存在或已被删除"}), 400
        return jsonify({"ok": True})
    except Exception as e:  # pragma: no cover
        logger.exception("删除预警失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/delete_all")
def api_stock_alerts_delete_all():  # type: ignore[override]
    """删除所有预警记录。"""

    try:
        engine = get_alert_engine()
        count = engine.delete_all_alerts()
        return jsonify({"ok": True, "deleted": count})
    except Exception as e:  # pragma: no cover
        logger.exception("删除所有预警失败")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/stock_alerts/delete_old")
def api_stock_alerts_delete_old():  # type: ignore[override]
    """删除非今日的预警记录。"""

    try:
        engine = get_alert_engine()
        count = engine.delete_old_alerts()
        return jsonify({"ok": True, "deleted": count})
    except Exception as e:  # pragma: no cover
        logger.exception("删除非今日预警失败")
        return jsonify({"ok": False, "error": str(e)}), 500


def main() -> None:
    """脚本入口函数。

    默认监听 127.0.0.1:5000，关闭 debug 模式，避免在生产环境中自动重载。
    
    """
    # 先初始化日志系统
    init_logger()

    # 初始化个股预警相关调度任务
    init_stock_alert_scheduler()

    app.run(host="0.0.0.0", port=8818, debug=False)


if __name__ == "__main__":
    main()
