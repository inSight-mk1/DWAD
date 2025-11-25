#!/usr/bin/env python3
"""DWAD Flask 仪表盘服务

基于现有多周期排名报告界面，提供一个轻量级 Web 服务：
- 首页直接渲染最新的多周期实时排名图表
- 预留 API：下载数据、计算指数、更新排名

使用方法：
    python dashboard_server.py
    然后在浏览器中打开 http://127.0.0.1:5000/
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import json
import pandas as pd
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
from dwad.utils.logger import setup_logger


app = Flask(__name__)


def init_logger() -> None:
    """初始化日志系统。

    Flask 3.1 已移除 before_first_request 装饰器，这里改为在 main() 中
    显式调用该函数，以复用现有 loguru 日志配置。
    """
    setup_logger()
    logger.info("DWAD Flask 仪表盘服务启动")


def _sector_snapshot_cache_path() -> Path:
    """返回板块排名快照缓存文件路径。"""

    return ROOT_DIR / "reports" / "sector_ranking_snapshot.json"


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

    return True


def build_multi_period_page(enable_realtime: bool = True) -> str:
    """构造多周期实时排名页面的 HTML 字符串。

    优先读取上一次生成的报表 HTML，如果不存在则触发一次重建。
    """
    reports_dir = ROOT_DIR / "reports"
    html_path = reports_dir / "index_ranking_dashboard.html"

    if not html_path.exists():
        logger.info("未找到现有报表，正在重新生成多周期页面")
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
    """首页：返回当前最新的多周期实时排名页面。"""
    html = build_multi_period_page(enable_realtime=True)
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


def main() -> None:
    """脚本入口函数。

    默认监听 127.0.0.1:5000，关闭 debug 模式，避免在生产环境中自动重载。
    """
    # 先初始化日志系统
    init_logger()
    app.run(host="127.0.0.1", port=5000, debug=False)


if __name__ == "__main__":
    main()
