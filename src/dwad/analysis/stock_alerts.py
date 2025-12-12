from __future__ import annotations

"""个股预警核心引擎（v1 精简版）

- 维护女星股 / 金店股两个自选列表（从 config.yaml 读取/写入）
- 解析用户输入的股票（中文名 / 6位代码 / 完整 symbol）为标准 symbol
- 读取本地日线数据 + 需要时用掘金分钟线合成当日 bar
- 实现两个预警条件：
  * 女星股：缩量下跌（量<近N日均量、量<前一日量、收盘价<前一日）
  * 金店股：2 日 K 线上的 KDJ，J<0 触发
- 将最新检测结果写入 metadata/stock_alerts_state.json，供 API 使用

注意：本文件只实现后端逻辑，不包含 Flask 视图/接口。
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from ..data_storage.parquet_storage import ParquetStorage
from ..data_fetcher.goldminer_fetcher import GoldMinerFetcher
from ..utils.config import config
from ..utils.timezone import now_beijing, now_beijing_iso, today_beijing


WATCHLIST_KEYS = {
    "nuxing": "stock_alerts.nuxing_symbols",
    "jindian": "stock_alerts.jindian_symbols",
}

DEFAULT_PUSH_CONFIG = {
    "check_interval_minutes": 5,
    "push_interval_minutes": 10,
    "max_push_times": 100,
}


class StockAlertEngine:
    """个股预警引擎（精简逻辑版）。"""

    def __init__(self, metadata_path: Optional[str] = None) -> None:
        self.storage = ParquetStorage()
        self._goldminer: Optional[GoldMinerFetcher] = None
        self._stock_info_df: Optional[pd.DataFrame] = None
        # 分钟线数据缓存，用于批量获取后复用，避免重复网络请求
        self._intraday_cache: Dict[str, pd.DataFrame] = {}

        paths = config.get_data_paths()
        base_metadata = Path(paths.get("metadata_path", "./data/metadata"))
        if metadata_path is not None:
            base_metadata = Path(metadata_path)
        base_metadata.mkdir(parents=True, exist_ok=True)
        self.state_file = base_metadata / "stock_alerts_state.json"
        self.config_file = base_metadata / "stock_alerts_config.json"  # 独立配置文件

    # ------------------------------------------------------------------
    # 配置与自选列表
    # ------------------------------------------------------------------

    def _load_state(self) -> Dict[str, Any]:
        """加载预警状态 JSON。"""
        if not self.state_file.exists():
            return {"alerts": {}}
        try:
            with self.state_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {"alerts": {}}
            if "alerts" not in data or not isinstance(data["alerts"], dict):
                data["alerts"] = {}
            return data
        except Exception as e:
            logger.error(f"加载预警状态文件失败: {e}")
            return {"alerts": {}}

    def _load_config(self) -> Dict[str, Any]:
        """加载预警配置 JSON（独立于 config.yaml）。"""
        if not self.config_file.exists():
            # 首次使用时，尝试从 config.yaml 迁移配置
            return self._migrate_config_from_yaml()
        try:
            with self.config_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return self._get_default_config()
            return data
        except Exception as e:
            logger.error(f"加载预警配置文件失败: {e}")
            return self._get_default_config()

    def _save_config(self, cfg: Dict[str, Any]) -> None:
        """保存预警配置 JSON。"""
        try:
            tmp_path = self.config_file.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
            tmp_path.replace(self.config_file)
        except Exception as e:
            logger.error(f"保存预警配置文件失败: {e}")

    def _get_default_config(self) -> Dict[str, Any]:
        """返回默认配置。"""
        return {
            "nuxing_symbols": [],
            "jindian_symbols": [],
            "push": dict(DEFAULT_PUSH_CONFIG),
        }

    def _migrate_config_from_yaml(self) -> Dict[str, Any]:
        """从 config.yaml 迁移配置到独立文件（仅首次执行）。"""
        cfg = self._get_default_config()
        
        # 尝试从 config.yaml 读取现有配置
        nuxing = config.get("stock_alerts.nuxing_symbols", [])
        jindian = config.get("stock_alerts.jindian_symbols", [])
        push = config.get("stock_alerts.push", {})
        
        if isinstance(nuxing, list):
            cfg["nuxing_symbols"] = [str(s) for s in nuxing]
        if isinstance(jindian, list):
            cfg["jindian_symbols"] = [str(s) for s in jindian]
        if isinstance(push, dict):
            for k in cfg["push"].keys():
                v = push.get(k)
                if isinstance(v, (int, float)) and int(v) > 0:
                    cfg["push"][k] = int(v)
        
        # 保存到独立配置文件
        self._save_config(cfg)
        logger.info("已将预警配置从 config.yaml 迁移到 {}", self.config_file)
        return cfg

    def _save_state(self, state: Dict[str, Any]) -> None:
        """保存预警状态 JSON。"""
        try:
            tmp_path = self.state_file.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            tmp_path.replace(self.state_file)
        except Exception as e:
            logger.error(f"保存预警状态文件失败: {e}")

    def _ensure_stock_info_df(self) -> pd.DataFrame:
        if self._stock_info_df is None:
            self._stock_info_df = self.storage.load_stock_info(as_dataframe=True)
            if self._stock_info_df is None:
                self._stock_info_df = pd.DataFrame()
        return self._stock_info_df

    def _get_symbol_name(self, symbol: str) -> str:
        df = self._ensure_stock_info_df()
        if not df.empty and {"symbol", "name"}.issubset(df.columns):
            row = df[df["symbol"] == symbol]
            if not row.empty:
                name = row.iloc[0].get("name")
                if isinstance(name, str) and name:
                    return name
        return symbol

    def _get_raw_watchlist(self, rule: str) -> List[str]:
        cfg = self._load_config()
        key = f"{rule}_symbols"
        symbols = cfg.get(key, [])
        if not isinstance(symbols, list):
            return []
        return [str(s) for s in symbols]

    def _set_raw_watchlist(self, rule: str, symbols: List[str]) -> None:
        cfg = self._load_config()
        key = f"{rule}_symbols"
        # 去重保序
        uniques = list(dict.fromkeys(symbols))
        cfg[key] = uniques
        self._save_config(cfg)

    def get_watchlists_with_names(self) -> Dict[str, Any]:
        """返回前端友好的自选列表结构。"""
        out: Dict[str, Any] = {"nuxing": [], "jindian": []}
        for rule in ("nuxing", "jindian"):
            items = []
            for sym in self._get_raw_watchlist(rule):
                items.append({"symbol": sym, "name": self._get_symbol_name(sym)})
            out[rule] = items
        out["push"] = self.get_push_config()
        return out

    def get_push_config(self) -> Dict[str, int]:
        cfg = self._load_config()
        push_cfg = cfg.get("push", {}) or {}
        result = dict(DEFAULT_PUSH_CONFIG)
        if isinstance(push_cfg, dict):
            for k in result.keys():
                v = push_cfg.get(k)
                if isinstance(v, (int, float)) and int(v) > 0:
                    result[k] = int(v)
        return result

    def update_push_config(self, new_cfg: Dict[str, Any]) -> Dict[str, int]:
        cur = self.get_push_config()
        for k in cur.keys():
            if k in new_cfg:
                v = new_cfg[k]
                if isinstance(v, (int, float)) and int(v) > 0:
                    cur[k] = int(v)
        cfg = self._load_config()
        cfg["push"] = cur
        self._save_config(cfg)
        return cur

    # ------------------------------------------------------------------
    # 股票解析与增删
    # ------------------------------------------------------------------

    def _ensure_goldminer(self) -> Optional[GoldMinerFetcher]:
        if self._goldminer is None:
            try:
                self._goldminer = GoldMinerFetcher()
            except Exception as e:
                logger.error(f"初始化掘金数据获取器失败: {e}")
                self._goldminer = None
        return self._goldminer

    def resolve_symbol(self, query: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """将用户输入解析为标准 symbol（SHSE.600000 等）。"""
        text = (query or "").strip()
        if not text:
            return None, None, "输入为空"

        df = self._ensure_stock_info_df()
        symbol: Optional[str] = None
        name: Optional[str] = None

        # 1) 已经是完整 symbol
        if "." in text:
            cand = text.upper()
            symbol = cand
            if not df.empty and "symbol" in df.columns:
                row = df[df["symbol"] == cand]
                if not row.empty and "name" in df.columns:
                    nm = row.iloc[0].get("name")
                    if isinstance(nm, str) and nm:
                        name = nm

        # 2) 6 位数字代码，匹配结尾 6 位
        elif text.isdigit() and len(text) == 6:
            if not df.empty and "symbol" in df.columns:
                row = df[df["symbol"].str[-6:] == text]
                if not row.empty:
                    symbol = str(row.iloc[0]["symbol"])
                    if "name" in df.columns:
                        nm = row.iloc[0].get("name")
                        if isinstance(nm, str) and nm:
                            name = nm

        # 3) 中文名称模糊匹配
        else:
            if not df.empty and "name" in df.columns:
                matched = df[df["name"].astype(str).str.contains(text, na=False)]
                if not matched.empty:
                    symbol = str(matched.iloc[0]["symbol"])
                    nm = matched.iloc[0].get("name")
                    if isinstance(nm, str) and nm:
                        name = nm

        # 本地没找到，用掘金按名称再查一次
        if not symbol:
            gm = self._ensure_goldminer()
            if gm is not None:
                info = gm.get_stock_info_by_name(text)
                if info is not None:
                    symbol = info.symbol
                    name = info.name

        if not symbol:
            return None, None, f"未能解析股票: {text}"

        if not name:
            name = self._get_symbol_name(symbol)
        return symbol, name, None

    def add_symbol(self, rule: str, query: str) -> Tuple[bool, Dict[str, Any]]:
        if rule not in WATCHLIST_KEYS:
            return False, {"error": f"无效规则: {rule}"}
        symbol, name, err = self.resolve_symbol(query)
        if err:
            return False, {"error": err}
        assert symbol is not None
        symbols = self._get_raw_watchlist(rule)
        if symbol in symbols:
            return True, {"symbol": symbol, "name": name or symbol, "message": "已存在"}
        # 新添加的放在列表最前面
        symbols.insert(0, symbol)
        self._set_raw_watchlist(rule, symbols)
        return True, {"symbol": symbol, "name": name or symbol}

    def remove_symbol(self, rule: str, symbol: str) -> Tuple[bool, str]:
        if rule not in WATCHLIST_KEYS:
            return False, "无效规则"
        symbols = self._get_raw_watchlist(rule)
        new_symbols = [s for s in symbols if s != symbol]
        self._set_raw_watchlist(rule, new_symbols)
        return True, "ok"

    # ------------------------------------------------------------------
    # 预警规则计算与状态管理
    # ------------------------------------------------------------------

    def _prefetch_intraday_data(self, symbols: List[str], date_str: str) -> None:
        """批量预获取所有股票的分钟线数据，存入缓存。
        
        这样后续 _build_today_bar_from_intraday 可以直接从缓存读取，
        避免每只股票单独发起网络请求。
        """
        if not symbols:
            return

        try:
            from gm.api import history, ADJUST_NONE
        except Exception as e:
            logger.error(f"导入掘金API失败(批量分钟线): {e}")
            return

        start_time = f"{date_str} 09:00:00"
        end_time = f"{date_str} 15:30:00"

        try:
            logger.info(f"批量获取 {len(symbols)} 只股票的分钟线数据...")
            # 掘金 history API 支持 list 格式的 symbol 参数
            df = history(
                symbol=symbols,
                frequency="60s",
                start_time=start_time,
                end_time=end_time,
                fields="symbol,open,close,high,low,volume,eob",
                adjust=ADJUST_NONE,
                df=True,
            )
        except Exception as e:
            logger.error(f"批量获取分钟线异常: {e}")
            return

        # 掘金API在连接失败时可能返回dict而非DataFrame
        if isinstance(df, dict):
            status = df.get("status", "unknown")
            message = df.get("message", "未知错误")
            logger.error(f"批量获取分钟线失败: status={status}, message={message}")
            return

        if df is None or len(df) == 0:
            logger.warning("批量获取分钟线: 无数据返回")
            return

        if "symbol" not in df.columns or "eob" not in df.columns:
            logger.warning("批量获取分钟线: 返回数据缺少必要列")
            return

        # 按 symbol 分组存入缓存
        df = df.copy()
        df["date"] = pd.to_datetime(df["eob"]).dt.strftime("%Y-%m-%d")
        df = df[df["date"] == date_str]

        for symbol in symbols:
            symbol_df = df[df["symbol"] == symbol]
            if not symbol_df.empty:
                self._intraday_cache[symbol] = symbol_df

        logger.info(f"分钟线数据缓存完成，共 {len(self._intraday_cache)} 只股票有数据")

    def _clear_intraday_cache(self) -> None:
        """清空分钟线数据缓存。"""
        self._intraday_cache.clear()

    def _get_daily_bars_with_today(self, symbol: str, max_days: int = 120) -> Optional[pd.DataFrame]:
        """获取包含当日模拟bar在内的日线数据。

        - 优先使用本地前复权日线数据（ParquetStorage）
        - 如果本地最新一条不是今天，则用掘金1分钟/60秒bar合成当日bar并追加
        """

        df = self.storage.load_stock_data(symbol)
        if df is None or df.empty:
            return None

        required_cols = {"date", "open_price", "high_price", "low_price", "close_price", "volume"}
        if not required_cols.issubset(df.columns):
            logger.warning(f"股票{symbol}缺少必要列，无法进行预警计算: {required_cols - set(df.columns)}")
            return None

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("date")
        if max_days and len(df) > max_days:
            df = df.tail(max_days)

        today_str = today_beijing()
        last_date = str(df["date"].iloc[-1])
        if last_date == today_str:
            return df

        # 尝试用分钟线合成当日bar
        intraday_bar = self._build_today_bar_from_intraday(symbol, today_str)
        if intraday_bar is None:
            return df

        df2 = pd.concat([df, pd.DataFrame([intraday_bar])], ignore_index=True)
        return df2

    def _build_today_bar_from_intraday(self, symbol: str, date_str: str) -> Optional[Dict[str, Any]]:
        """使用分钟线合成当日日bar，并线性外推全天成交量。

        优先从缓存读取（由 _prefetch_intraday_data 批量获取），
        若缓存无数据则单独获取（兼容旧逻辑）。
        """
        # 优先从缓存读取（静默，不打印日志）
        df = self._intraday_cache.get(symbol)

        # 缓存无数据，回退到单独获取
        if df is None or df.empty:
            try:
                from gm.api import history, ADJUST_NONE
            except Exception as e:
                logger.error(f"导入掘金API失败(分钟线): {e}")
                return None

            start_time = f"{date_str} 09:00:00"
            end_time = f"{date_str} 15:30:00"

            try:
                df = history(
                    symbol=symbol,
                    frequency="60s",
                    start_time=start_time,
                    end_time=end_time,
                    fields="open,close,high,low,volume,eob",
                    adjust=ADJUST_NONE,
                    df=True,
                )
            except Exception as e:
                logger.error(f"获取{symbol}分钟线异常: {e}")
                return None

            # 掘金API在连接失败时可能返回dict而非DataFrame
            if isinstance(df, dict):
                status = df.get("status", "unknown")
                message = df.get("message", "未知错误")
                func = df.get("function", "unknown")
                logger.error(f"获取{symbol}分钟线失败: status={status}, message={message}, function={func}")
                return None

            if df is None or len(df) == 0:
                return None

            df = df.copy()
            df["date"] = pd.to_datetime(df["eob"]).dt.strftime("%Y-%m-%d")
            df = df[df["date"] == date_str]

        # 从这里开始，df 可能来自缓存或单独获取
        if df is None or df.empty:
            return None
        if "open" not in df.columns or "close" not in df.columns or "volume" not in df.columns:
            return None

        vol_so_far = float(df["volume"].sum())
        bars = len(df)
        total_bars = 240  # 4 小时交易时间，按 1 分钟一个bar 估算
        proj_vol = vol_so_far
        if 0 < bars < total_bars:
            proj_vol = vol_so_far * (total_bars / bars)

        first = df.iloc[0]
        last = df.iloc[-1]
        high_val = float(df["high"].max()) if "high" in df.columns else float(max(first["open"], last["close"]))
        low_val = float(df["low"].min()) if "low" in df.columns else float(min(first["open"], last["close"]))

        return {
            "symbol": symbol,
            "date": date_str,
            "open_price": float(first["open"]),
            "high_price": high_val,
            "low_price": low_val,
            "close_price": float(last["close"]),
            "volume": int(proj_vol),
        }

    def _detect_nuxing(self, symbol: str, daily_df: pd.DataFrame, vol_lookback: int = 5) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """女星股：缩量调整

        条件：
        - 当日成交量 < 近 N 日平均量
        - 当日成交量 < 前一日量
        - 当日收盘价 < 前一日收盘价
        
        返回: (是否触发, 计算结果字典)
        """

        if len(daily_df) < vol_lookback + 2:
            return False, None

        df = daily_df.sort_values("date")
        last = df.iloc[-1]
        prev = df.iloc[-2]
        recent = df.tail(vol_lookback + 1).iloc[:-1]

        try:
            last_vol = float(last["volume"])
            prev_vol = float(prev["volume"])
            avg_vol = float(recent["volume"].mean())
            last_close = float(last["close_price"])
            prev_close = float(prev["close_price"])
        except Exception:
            return False, None

        if avg_vol <= 0 or prev_vol <= 0 or prev_close <= 0:
            return False, None

        # 计算易读的百分比指标
        change_pct = (last_close / prev_close - 1.0) * 100 if prev_close > 0 else 0.0
        vol_vs_prev_pct = (last_vol / prev_vol - 1.0) * 100 if prev_vol > 0 else 0.0
        vol_vs_avg_pct = (last_vol / avg_vol - 1.0) * 100 if avg_vol > 0 else 0.0

        cond1 = last_vol < avg_vol
        cond2 = last_vol < prev_vol
        cond3 = last_close < prev_close
        triggered = cond1 and cond2 and cond3

        result = {
            "rule": "nuxing",
            "symbol": symbol,
            "date": str(last["date"]),
            "metrics": {
                "当日涨幅": f"{change_pct:.2f}%",
                "日缩量(vs昨日)": f"{vol_vs_prev_pct:.2f}%",
                "5MA缩量": f"{vol_vs_avg_pct:.2f}%",
                "收盘价": f"{last_close:.2f}",
            },
        }
        
        # 输出计算结果日志
        logger.info("  [女星股] 计算结果: 涨幅={:.2f}%, 日缩量={:.2f}%, 5MA缩量={:.2f}%, 收盘价={:.2f}, 触发={}",
                    change_pct, vol_vs_prev_pct, vol_vs_avg_pct, last_close, triggered)

        return triggered, result if triggered else None

    def _build_2day_bars(self, daily_df: pd.DataFrame) -> pd.DataFrame:
        """从日K 合成 2 日 K 线序列（用于金店股 KDJ）。
        
        采用非重叠方式：从数据起点开始，每两个交易日合成一根 2日K。
        例如：数据从 12-01 开始，则 12-01+12-02 合成一根，12-03+12-04 合成一根，以此类推。
        如果总天数为奇数，最后一天单独成为一根（只用当天数据）。
        """

        df = daily_df.sort_values("date").reset_index(drop=True)
        required = ["open_price", "high_price", "low_price", "close_price", "volume"]
        for c in required:
            if c not in df.columns:
                return pd.DataFrame()

        n = len(df)
        if n < 2:
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        # 从数据起点开始，每两天合成一根 2日K
        # 索引: 0+1 -> 一根; 2+3 -> 一根; ...
        i = 0
        while i < n - 1:
            first = df.iloc[i]      # 第一天（较旧）
            second = df.iloc[i + 1]  # 第二天（较新）
            rows.append(
                {
                    "date": second["date"],  # 以第二天的日期作为 2 日K 的日期
                    "open_price": float(first["open_price"]),
                    "high_price": float(max(first["high_price"], second["high_price"])),
                    "low_price": float(min(first["low_price"], second["low_price"])),
                    "close_price": float(second["close_price"]),
                    "volume": float(first["volume"]) + float(second["volume"]),
                }
            )
            i += 2  # 跳过两天

        # 如果总天数为奇数，最后一天单独成为一根2日K（只用当天数据）
        if n % 2 == 1:
            last = df.iloc[-1]
            rows.append(
                {
                    "date": last["date"],
                    "open_price": float(last["open_price"]),
                    "high_price": float(last["high_price"]),
                    "low_price": float(last["low_price"]),
                    "close_price": float(last["close_price"]),
                    "volume": float(last["volume"]),
                }
            )

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def _compute_kdj(self, df: pd.DataFrame, period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """在给定K线序列上计算 K、D、J 指标。"""

        high = df["high_price"].astype(float)
        low = df["low_price"].astype(float)
        close = df["close_price"].astype(float)

        low_list = low.rolling(period, min_periods=1).min()
        high_list = high.rolling(period, min_periods=1).max()
        denom = high_list - low_list
        denom = denom.where(denom != 0, 1.0)
        rsv = (close - low_list) / denom * 100.0

        k = pd.Series(index=rsv.index, dtype=float)
        d = pd.Series(index=rsv.index, dtype=float)
        j = pd.Series(index=rsv.index, dtype=float)

        prev_k = 50.0
        prev_d = 50.0
        for i in range(len(rsv)):
            r = rsv.iloc[i]
            if pd.isna(r):
                k.iloc[i] = prev_k
                d.iloc[i] = prev_d
            else:
                cur_k = (2.0 / 3.0) * prev_k + (1.0 / 3.0) * float(r)
                cur_d = (2.0 / 3.0) * prev_d + (1.0 / 3.0) * cur_k
                k.iloc[i] = cur_k
                d.iloc[i] = cur_d
                prev_k = cur_k
                prev_d = cur_d
            j.iloc[i] = 3.0 * k.iloc[i] - 2.0 * d.iloc[i]

        return k, d, j

    def _detect_jindian(self, symbol: str, daily_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """金店股：2 日 K 线 KDJ 的 J<0 预警。"""

        # 至少需要若干个 2 日K（例如 9 个周期），约 20 个交易日数据
        if len(daily_df) < 20:
            logger.debug("  [金店股] 日线数据不足20条，跳过")
            return None

        df2 = self._build_2day_bars(daily_df)
        if df2.empty or len(df2) < 9:
            logger.debug("  [金店股] 2日K线数据不足9条，跳过")
            return None

        k, d, j = self._compute_kdj(df2, period=9)
        if j.empty:
            logger.debug("  [金店股] KDJ计算失败，跳过")
            return None

        j_last = j.iloc[-1]
        k_last = k.iloc[-1]
        d_last = d.iloc[-1]
        last_row = df2.iloc[-1]
        close_price = float(last_row["close_price"])
        triggered = not pd.isna(j_last) and j_last < 0
        
        # 输出计算结果日志（包含当前股价）
        logger.info("  [金店股] 计算结果: K={:.2f}, D={:.2f}, J={:.2f}, 收盘价={:.2f}, 2日K日期={}, 触发={}",
                    float(k_last), float(d_last), float(j_last), close_price, str(last_row["date"]), triggered)
        
        if not triggered:
            return None

        return {
            "rule": "jindian",
            "symbol": symbol,
            "date": str(last_row["date"]),
            "metrics": {
                "K值": f"{float(k_last):.2f}",
                "D值": f"{float(d_last):.2f}",
                "J值": f"{float(j_last):.2f}",
                "收盘价": f"{float(last_row['close_price']):.2f}",
            },
        }

    def run_detection_cycle(self) -> None:
        """执行一轮预警检测，更新状态文件。

        - 对当前配置中的女星股 / 金店股逐一检测
        - 触发后在 state["alerts"] 中按 (rule,symbol,date) 建立记录
        - 已触发的预警在当日内不会被自动清除，由前端确认后停止推送
        """

        cfg = {
            "nuxing": self._get_raw_watchlist("nuxing"),
            "jindian": self._get_raw_watchlist("jindian"),
        }
        all_symbols = sorted(set(cfg["nuxing"]) | set(cfg["jindian"]))
        if not all_symbols:
            logger.info("预警检测: 自选列表为空，跳过检测")
            return

        # 获取今天的日期，用于判断预警是否是今天的
        today_str = today_beijing()
        
        logger.info("预警检测开始: 女星股 {} 只, 金店股 {} 只, 共 {} 只待检测, 今日={}",
                    len(cfg["nuxing"]), len(cfg["jindian"]), len(all_symbols), today_str)

        # 批量预获取所有股票的分钟线数据，避免逐只股票单独请求
        self._clear_intraday_cache()
        self._prefetch_intraday_data(all_symbols, today_str)

        state = self._load_state()
        alerts: Dict[str, Any] = state.get("alerts", {})
        now_iso = now_beijing_iso()
        new_alerts_count = 0

        for symbol in all_symbols:
            name = self._get_symbol_name(symbol)
            logger.info("检测 {} ({})...", symbol, name)
            
            daily_df = self._get_daily_bars_with_today(symbol)
            if daily_df is None or daily_df.empty:
                logger.warning("  {} 无法获取日线数据，跳过", symbol)
                continue
            
            latest_date = str(daily_df["date"].iloc[-1]) if len(daily_df) > 0 else "N/A"
            is_today_data = (latest_date == today_str)
            logger.info("  获取到 {} 条日线数据，最新日期: {} {}", 
                       len(daily_df), latest_date, 
                       "(今日数据)" if is_today_data else "(非今日数据，分钟线可能获取失败)")

            # 女星股
            if symbol in cfg["nuxing"]:
                triggered, alert = self._detect_nuxing(symbol, daily_df)
                if triggered and alert is not None:
                    alert_date = alert["date"]
                    aid = f"{alert['rule']}:{alert['symbol']}:{alert_date}"
                    is_new = aid not in alerts
                    is_today_alert = (alert_date == today_str)
                    
                    if is_new:
                        alerts[aid] = {
                            "id": aid,
                            "rule": alert["rule"],
                            "symbol": alert["symbol"],
                            "date": alert_date,
                            "metrics": alert.get("metrics", {}),
                            "first_trigger_time": now_iso,
                            "last_detect_time": now_iso,
                            "last_push_time": None,
                            "push_count": 0,
                            "acknowledged": False,
                        }
                        new_alerts_count += 1
                        logger.info("  [女星股] 触发预警! {}", alert_date)
                    else:
                        alerts[aid]["metrics"] = alert.get("metrics", {})
                        alerts[aid]["last_detect_time"] = now_iso
                        # 只有今天的预警才显示"已存在"日志，历史预警不显示
                        if is_today_alert:
                            logger.info("  [女星股] 今日预警已存在，更新指标")
                        else:
                            logger.debug("  [女星股] 历史预警({})已存在，跳过", alert_date)

            # 金店股
            if symbol in cfg["jindian"]:
                alert = self._detect_jindian(symbol, daily_df)
                if alert is not None:
                    alert_date = alert["date"]
                    aid = f"{alert['rule']}:{alert['symbol']}:{alert_date}"
                    is_new = aid not in alerts
                    is_today_alert = (alert_date == today_str)
                    
                    if is_new:
                        alerts[aid] = {
                            "id": aid,
                            "rule": alert["rule"],
                            "symbol": alert["symbol"],
                            "date": alert_date,
                            "metrics": alert.get("metrics", {}),
                            "first_trigger_time": now_iso,
                            "last_detect_time": now_iso,
                            "last_push_time": None,
                            "push_count": 0,
                            "acknowledged": False,
                        }
                        new_alerts_count += 1
                        logger.info("  [金店股] 触发预警! {} 指标: {}", alert_date, alert.get("metrics", {}))
                    else:
                        alerts[aid]["metrics"] = alert.get("metrics", {})
                        alerts[aid]["last_detect_time"] = now_iso
                        # 只有今天的预警才显示"已存在"日志，历史预警不显示
                        if is_today_alert:
                            logger.info("  [金店股] 今日预警已存在，更新指标: {}", alert.get("metrics", {}))
                        else:
                            logger.debug("  [金店股] 历史预警({})已存在，跳过", alert_date)
                else:
                    logger.info("  [金店股] 未触发预警 (J值>=0)")

        state["alerts"] = alerts
        self._save_state(state)
        logger.info("预警检测完成: 本轮新增 {} 条预警", new_alerts_count)

    def list_alerts(self, only_active: bool = False) -> List[Dict[str, Any]]:
        """列出所有（或仅未确认的）预警记录。"""

        state = self._load_state()
        alerts = state.get("alerts", {}) or {}
        rows: List[Dict[str, Any]] = []
        for aid, data in alerts.items():
            if only_active and data.get("acknowledged"):
                continue
            row = dict(data)
            sym = row.get("symbol")
            if isinstance(sym, str):
                row["name"] = self._get_symbol_name(sym)
            rows.append(row)
        # 按时间倒序，最近触发的在前
        rows.sort(key=lambda r: (r.get("date"), r.get("first_trigger_time")), reverse=True)
        return rows

    def get_alerts_to_push(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """根据推送间隔与确认状态，返回本次需要推送的预警列表。

        - acknowledged=True 的记录不会再推送
        - push_count >= max_push_times 的记录不会再推送
        - 距离上次推送时间小于 push_interval_minutes 的记录不会再推送
        调用该方法会更新 state 中的 last_push_time / push_count。
        """

        if now is None:
            now = now_beijing()

        cfg = self.get_push_config()
        interval_min = int(cfg.get("push_interval_minutes", DEFAULT_PUSH_CONFIG["push_interval_minutes"]))
        max_push = int(cfg.get("max_push_times", DEFAULT_PUSH_CONFIG["max_push_times"]))

        state = self._load_state()
        alerts = state.get("alerts", {}) or {}

        to_push: List[Dict[str, Any]] = []
        changed = False

        for aid, data in alerts.items():
            if data.get("acknowledged"):
                continue

            push_count = int(data.get("push_count", 0))
            if max_push > 0 and push_count >= max_push:
                continue

            last_push_str = data.get("last_push_time")
            last_push_dt: Optional[datetime] = None
            if isinstance(last_push_str, str) and last_push_str:
                try:
                    last_push_dt = datetime.fromisoformat(last_push_str)
                except Exception:
                    last_push_dt = None

            if last_push_dt is not None:
                delta_min = (now - last_push_dt).total_seconds() / 60.0
                if delta_min < interval_min:
                    continue

            # 本次需要推送
            push_record = dict(data)
            sym = push_record.get("symbol")
            if isinstance(sym, str):
                push_record["name"] = self._get_symbol_name(sym)
            to_push.append(push_record)

            # 更新状态
            alerts[aid]["last_push_time"] = now.isoformat(timespec="seconds")
            alerts[aid]["push_count"] = push_count + 1
            changed = True

        if changed:
            state["alerts"] = alerts
            self._save_state(state)

        return to_push

    def ack_alert(self, alert_id: str) -> bool:
        """确认收到某条预警，后续不再推送。"""

        state = self._load_state()
        alerts = state.get("alerts", {}) or {}
        if alert_id not in alerts:
            return False

        alerts[alert_id]["acknowledged"] = True
        alerts[alert_id]["ack_time"] = now_beijing_iso()
        state["alerts"] = alerts
        self._save_state(state)
        return True

    def delete_alert(self, alert_id: str) -> bool:
        """删除单条预警记录。"""

        state = self._load_state()
        alerts = state.get("alerts", {}) or {}
        if alert_id not in alerts:
            return False

        del alerts[alert_id]
        state["alerts"] = alerts
        self._save_state(state)
        logger.info("删除预警: {}", alert_id)
        return True

    def delete_all_alerts(self) -> int:
        """删除所有预警记录，返回删除的数量。"""

        state = self._load_state()
        alerts = state.get("alerts", {}) or {}
        count = len(alerts)
        state["alerts"] = {}
        self._save_state(state)
        logger.info("删除所有预警，共 {} 条", count)
        return count

    def delete_old_alerts(self) -> int:
        """删除非今日的预警记录，返回删除的数量。"""

        today_str = today_beijing()
        state = self._load_state()
        alerts = state.get("alerts", {}) or {}
        
        old_ids = [aid for aid, data in alerts.items() if data.get("date") != today_str]
        for aid in old_ids:
            del alerts[aid]
        
        state["alerts"] = alerts
        self._save_state(state)
        if old_ids:
            logger.info("删除非今日预警，共 {} 条", len(old_ids))
        return len(old_ids)

    def delete_alerts_by_symbol(self, symbol: str) -> int:
        """删除指定股票的所有预警记录，返回删除的数量。"""

        state = self._load_state()
        alerts = state.get("alerts", {}) or {}
        
        to_delete = [aid for aid, data in alerts.items() if data.get("symbol") == symbol]
        for aid in to_delete:
            del alerts[aid]
        
        state["alerts"] = alerts
        self._save_state(state)
        if to_delete:
            logger.info("删除股票 {} 的预警，共 {} 条", symbol, len(to_delete))
        return len(to_delete)


# 预留一个简单的自测入口，方便你在 notebook / 脚本里快速试用
if __name__ == "__main__":
    engine = StockAlertEngine()
    print(engine.get_watchlists_with_names())
