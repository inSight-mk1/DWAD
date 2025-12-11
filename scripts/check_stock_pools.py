#!/usr/bin/env python3
import sys
import argparse
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Make project src importable
SRC_DIR = Path(__file__).resolve().parent.parent / 'src'
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd
import yaml
from datetime import datetime
from loguru import logger

from dwad.utils.logger import setup_logger
from dwad.utils.timezone import now_beijing_str
from dwad.data_storage.parquet_storage import ParquetStorage
from dwad.utils.config import config

# RealtimePriceFetcher is optional (gm3/token may be unavailable)
try:
    from dwad.data_fetcher.realtime_price_fetcher import RealtimePriceFetcher
    GM_AVAILABLE = True
except Exception:
    GM_AVAILABLE = False


def normalize_name(name: str) -> str:
    """Normalize Chinese stock names for robust matching.
    - NFKC normalize (fullwidth -> ASCII)
    - Strip common trading-day prefixes: XD, DR, XR, R, N, ST variants
    - Remove spaces and punctuation
    - Lowercase
    """
    if not isinstance(name, str):
        return ''
    s = unicodedata.normalize('NFKC', name)
    s = s.strip()
    # Remove typical prefixes repeatedly
    prefixes = [r'^\*?\s*ST', r'^S\*?ST', r'^SST', r'^XD', r'^DR', r'^XR', r'^R', r'^N']
    changed = True
    while changed and s:
        changed = False
        for p in prefixes:
            new_s = re.sub(p, '', s, flags=re.IGNORECASE)
            if new_s != s:
                s = new_s.strip()
                changed = True
    # Remove punctuation/whitespace-like chars
    s = re.sub(r"[\s\-·\._\(\)（）【】\[\]、/\\]+", '', s)
    return s.lower()


def build_stock_info_df(storage: ParquetStorage) -> pd.DataFrame:
    df = storage.load_stock_info(as_dataframe=True)
    if df is None or df.empty:
        logger.warning('股票基本信息为空，可能未进行过初始下载。')
        return pd.DataFrame(columns=['symbol', 'name', 'market', 'listing_date', 'name_norm'])
    df = df.copy()
    df['name_norm'] = df['name'].apply(normalize_name)
    return df


def match_name_to_symbol(stock_info_df: pd.DataFrame, raw_name: str, fuzzy_threshold: float = 0.8) -> Dict:
    """Return best match info for a config name.
    match_type in {exact, contains, reverse_contains, fuzzy, none, ambiguous}
    """
    from difflib import SequenceMatcher

    target_norm = normalize_name(raw_name)
    if stock_info_df.empty or not target_norm:
        return {
            'match': False,
            'match_type': 'none',
            'symbol': None,
            'matched_name': None,
            'confidence': 0.0,
            'candidates': []
        }

    # 1) exact
    exact_df = stock_info_df[stock_info_df['name_norm'] == target_norm]
    if len(exact_df) == 1:
        row = exact_df.iloc[0]
        return {
            'match': True,
            'match_type': 'exact',
            'symbol': row['symbol'],
            'matched_name': row['name'],
            'confidence': 1.0,
            'candidates': []
        }
    elif len(exact_df) > 1:
        # Multiple exact names (rare), treat as ambiguous
        return {
            'match': False,
            'match_type': 'ambiguous',
            'symbol': None,
            'matched_name': None,
            'confidence': 0.9,
            'candidates': exact_df[['symbol', 'name']].to_dict(orient='records')
        }

    # 2) contains (df contains target) — robust: compare normalized columns
    contains_df = stock_info_df[stock_info_df['name_norm'].str.contains(re.escape(target_norm), na=False)]
    if len(contains_df) == 1:
        row = contains_df.iloc[0]
        return {
            'match': True,
            'match_type': 'contains',
            'symbol': row['symbol'],
            'matched_name': row['name'],
            'confidence': 0.95,
            'candidates': []
        }
    elif len(contains_df) > 1 and not contains_df.empty:
        # rank by similarity
        scored = []
        for _, r in contains_df.iterrows():
            ratio = SequenceMatcher(None, target_norm, r['name_norm']).ratio()
            scored.append((ratio, r))
        scored.sort(key=lambda x: x[0], reverse=True)
        top_ratio, top_row = scored[0]
        if top_ratio >= fuzzy_threshold and (len(scored) == 1 or top_ratio - scored[1][0] >= 0.05):
            return {
                'match': True,
                'match_type': 'contains',
                'symbol': top_row['symbol'],
                'matched_name': top_row['name'],
                'confidence': float(top_ratio),
                'candidates': []
            }
        return {
            'match': False,
            'match_type': 'ambiguous',
            'symbol': None,
            'matched_name': None,
            'confidence': float(scored[0][0]) if scored else 0.0,
            'candidates': [{'symbol': r['symbol'], 'name': r['name']} for _, r in contains_df.iterrows()]
        }

    # 3) reverse_contains (target contains df)
    reverse_df = stock_info_df[stock_info_df['name_norm'].apply(lambda x: x in target_norm)]
    if len(reverse_df) == 1:
        row = reverse_df.iloc[0]
        return {
            'match': True,
            'match_type': 'reverse_contains',
            'symbol': row['symbol'],
            'matched_name': row['name'],
            'confidence': 0.9,
            'candidates': []
        }
    elif len(reverse_df) > 1 and not reverse_df.empty:
        scored = []
        for _, r in reverse_df.iterrows():
            ratio = SequenceMatcher(None, target_norm, r['name_norm']).ratio()
            scored.append((ratio, r))
        scored.sort(key=lambda x: x[0], reverse=True)
        top_ratio, top_row = scored[0]
        if top_ratio >= fuzzy_threshold and (len(scored) == 1 or top_ratio - scored[1][0] >= 0.05):
            return {
                'match': True,
                'match_type': 'reverse_contains',
                'symbol': top_row['symbol'],
                'matched_name': top_row['name'],
                'confidence': float(top_ratio),
                'candidates': []
            }
        return {
            'match': False,
            'match_type': 'ambiguous',
            'symbol': None,
            'matched_name': None,
            'confidence': float(scored[0][0]) if scored else 0.0,
            'candidates': [{'symbol': r['symbol'], 'name': r['name']} for _, r in reverse_df.iterrows()]
        }

    # 4) fuzzy across all
    from difflib import SequenceMatcher
    stock_info_df = stock_info_df.copy()
    stock_info_df['__ratio'] = stock_info_df['name_norm'].apply(lambda x: SequenceMatcher(None, target_norm, x).ratio())
    top = stock_info_df.sort_values('__ratio', ascending=False).head(3)
    if not top.empty and top.iloc[0]['__ratio'] >= fuzzy_threshold:
        row = top.iloc[0]
        return {
            'match': True,
            'match_type': 'fuzzy',
            'symbol': row['symbol'],
            'matched_name': row['name'],
            'confidence': float(row['__ratio']),
            'candidates': []
        }

    return {
        'match': False,
        'match_type': 'none',
        'symbol': None,
        'matched_name': None,
        'confidence': float(top.iloc[0]['__ratio']) if not top.empty else 0.0,
        'candidates': []
    }


def chinese_only_text(name: str) -> str:
    """Keep only Chinese characters from a name after NFKC normalization."""
    if not isinstance(name, str):
        return ''
    s = unicodedata.normalize('NFKC', name)
    # Remove well-known prefixes/suffixes implicitly by keeping only Chinese chars
    chars = re.findall(r'[\u4e00-\u9fa5]+', s)
    return ''.join(chars)


def chinese_only_fuzzy_match(stock_info_df: pd.DataFrame, raw_name: str, threshold: float = 0.75) -> Optional[Dict]:
    """Fuzzy match using only Chinese characters. Returns dict with name/symbol/confidence or None."""
    from difflib import SequenceMatcher
    target = chinese_only_text(raw_name)
    if not target or stock_info_df.empty:
        return None
    cand = stock_info_df[['name', 'symbol']].copy()
    cand['__ch'] = cand['name'].apply(chinese_only_text)
    cand = cand[cand['__ch'].astype(bool)]
    if cand.empty:
        return None
    cand['__ratio'] = cand['__ch'].apply(lambda x: SequenceMatcher(None, target, x).ratio())
    top = cand.sort_values('__ratio', ascending=False).head(3)
    if not top.empty and top.iloc[0]['__ratio'] >= threshold:
        top_ratio = float(top.iloc[0]['__ratio'])
        gap_ok = True if len(top) == 1 else (top_ratio - float(top.iloc[1]['__ratio']) >= 0.05)
        if gap_ok:
            return {
                'name': top.iloc[0]['name'],
                'symbol': top.iloc[0]['symbol'],
                'confidence': top_ratio,
            }
    return None


def _lookup_official_name_by_symbol(stock_info_df: pd.DataFrame, symbol: Optional[str]) -> Optional[str]:
    if not symbol:
        return None
    df = stock_info_df
    m = df[df['symbol'] == symbol]
    if not m.empty:
        return m.iloc[0]['name']
    return None


def apply_fixes_to_stock_pools(original_pools: Dict, records: List[Dict], stock_info_df: pd.DataFrame) -> Tuple[Dict, Dict, List[Dict]]:
    """Apply user's fix rules to stock pools and return (new_pools, stats, changes)."""
    rec_map: Dict[Tuple[str, str, str], Dict] = {}
    for r in records:
        rec_map[(r['pool'], r['concept'], r['stock_name'])] = r

    changed = 0
    removed = 0
    total = 0
    new_pools: Dict = {}
    changes: List[Dict] = []

    for pool_name, concepts in original_pools.items():
        new_concepts: Dict = {}
        for concept_name, names in (concepts or {}).items():
            new_list: List[str] = []
            seen = set()
            for raw_name in (names or []):
                total += 1
                rec = rec_map.get((pool_name, concept_name, raw_name))
                target_name = raw_name
                if rec is None:
                    # No record (unexpected); keep as-is
                    pass
                else:
                    both_missing = (not rec['db_exact_match']) and (not rec['rt_contains_match'])
                    single_missing = (rec['db_exact_match'] != rec['rt_contains_match'])

                    if both_missing:
                        # Try Chinese-only fuzzy; if still None, remove the entry
                        ch = chinese_only_fuzzy_match(stock_info_df, raw_name, threshold=0.75)
                        if ch:
                            target_name = ch['name']
                            if target_name != raw_name:
                                changed += 1
                                changes.append({
                                    'pool': pool_name,
                                    'concept': concept_name,
                                    'old_name': raw_name,
                                    'new_name': target_name,
                                    'action': 'changed',
                                    'source': 'chinese_only',
                                    'symbol': ch.get('symbol'),
                                })
                        else:
                            removed += 1
                            # skip append => delete from config
                            changes.append({
                                'pool': pool_name,
                                'concept': concept_name,
                                'old_name': raw_name,
                                'new_name': None,
                                'action': 'removed',
                                'source': 'unmatched_both',
                                'symbol': None,
                            })
                            continue
                    elif single_missing:
                        # Use smart suggestion first; fallback to official name by symbol
                        target_name = (
                            rec.get('smart_matched_db_name')
                            or _lookup_official_name_by_symbol(stock_info_df, rec.get('rt_contains_symbol'))
                            or _lookup_official_name_by_symbol(stock_info_df, rec.get('db_exact_symbol'))
                            or raw_name
                        )
                        if target_name != raw_name:
                            changed += 1
                            src = 'smart' if rec.get('smart_matched_db_name') else (
                                'by_symbol_rt' if rec.get('rt_contains_symbol') else (
                                    'by_symbol_exact' if rec.get('db_exact_symbol') else 'unknown'
                                )
                            )
                            changes.append({
                                'pool': pool_name,
                                'concept': concept_name,
                                'old_name': raw_name,
                                'new_name': target_name,
                                'action': 'changed',
                                'source': src,
                                'symbol': rec.get('smart_symbol') or rec.get('rt_contains_symbol') or rec.get('db_exact_symbol'),
                            })
                    else:
                        # Both sides matched; keep as-is
                        pass

                if target_name not in seen:
                    new_list.append(target_name)
                    seen.add(target_name)

            new_concepts[concept_name] = new_list
        new_pools[pool_name] = new_concepts

    stats = {
        'changed': changed,
        'removed': removed,
        'unchanged': max(0, total - changed - removed),
        'total': total,
    }
    return new_pools, stats, changes


def _load_full_yaml(path: Path) -> Dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def _save_stock_pools_config(input_cfg_path: Path, full_cfg: Dict, new_pools: Dict, output_path: Optional[Path], backup: bool = True) -> Path:
    write_path = output_path or input_cfg_path
    if backup and (output_path is None):
        ts = now_beijing_str('%Y%m%d-%H%M%S')
        backup_path = input_cfg_path.with_suffix(input_cfg_path.suffix + f'.bak-{ts}')
        backup_path.write_text(Path(input_cfg_path).read_text(encoding='utf-8'), encoding='utf-8')
    full_cfg = dict(full_cfg)
    full_cfg['stock_pools'] = new_pools
    with open(write_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(full_cfg, f, allow_unicode=True, sort_keys=False)
    return write_path

def exact_mapping_symbol(stock_info_df: pd.DataFrame, raw_name: str) -> Optional[str]:
    """Replicate IndexCalculator mapping: exact name equality without normalization."""
    if stock_info_df.empty:
        return None
    # exact equality on original 'name'
    matched = stock_info_df[stock_info_df['name'] == raw_name]
    if not matched.empty:
        return matched.iloc[0]['symbol']
    return None


def contains_mapping_symbol(stock_info_df: pd.DataFrame, raw_name: str) -> Optional[str]:
    """Replicate IndexComparator mapping: DataFrame name contains raw_name, regex=False."""
    if stock_info_df.empty or not isinstance(raw_name, str):
        return None
    try:
        matched = stock_info_df[stock_info_df['name'].str.contains(raw_name, na=False, regex=False)]
        if not matched.empty:
            return matched.iloc[0]['symbol']
    except Exception:
        # If raw_name contains special characters that cause issues, fall back to safe escape
        try:
            matched = stock_info_df[stock_info_df['name'].str.contains(re.escape(raw_name), na=False, regex=True)]
            if not matched.empty:
                return matched.iloc[0]['symbol']
        except Exception:
            return None
    return None


def load_stock_pools(config_path: Path) -> Dict:
    import yaml
    if not config_path.exists():
        raise FileNotFoundError(f'股池配置文件不存在: {config_path}')
    with open(config_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}
    return data.get('stock_pools', {})


def batch_check_realtime(symbols: List[str], batch_size: int = 200) -> Tuple[Dict[str, bool], Optional[str], bool]:
    """Return (map symbol->bool, earliest timestamp, ran_flag).
    If gm unavailable or token invalid, returns ({}, None, False).
    """
    if not GM_AVAILABLE:
        logger.warning('未安装 gm3 或实时模块不可用，跳过实时检查。')
        return {}, None, False
    # token check
    try:
        fetcher = RealtimePriceFetcher()
    except Exception as e:
        logger.warning(f'实时检查不可用: {e}')
        return {}, None, False

    results: Dict[str, bool] = {}
    timestamps = []
    unique_symbols = list(dict.fromkeys(symbols))  # deduplicate preserving order
    for i in range(0, len(unique_symbols), batch_size):
        batch = unique_symbols[i:i+batch_size]
        try:
            price_map = fetcher.get_current_prices(batch)
            for s in batch:
                results[s] = s in price_map
            if price_map:
                # take any timestamp
                timestamps.extend([p.created_at for p in price_map.values()])
        except Exception as e:
            logger.warning(f'实时批次检查失败: {e}')
            for s in batch:
                results[s] = False
    earliest = None
    if timestamps:
        earliest = str(min(timestamps))
    return results, earliest, True


def main():
    setup_logger()

    parser = argparse.ArgumentParser(description='股池数据一致性检查工具')
    parser.add_argument('-c', '--config', default=str(Path(__file__).resolve().parents[1] / 'config' / 'stock_pools.yaml'), help='股池配置文件路径')
    parser.add_argument('-o', '--output', default=str(Path(__file__).resolve().parents[1] / 'reports' / 'stock_pools_check.csv'), help='输出文件路径（csv或json）')
    parser.add_argument('--format', choices=['csv', 'json'], default=None, help='输出格式（默认根据后缀推断）')
    parser.add_argument('-r', '--realtime', action='store_true', help='启用实时可获取性检查（需配置 goldminer.token 且安装 gm3）')
    parser.add_argument('--batch-size', type=int, default=200, help='实时检查批大小')
    parser.add_argument('--fuzzy-threshold', type=float, default=0.80, help='模糊匹配阈值')
    parser.add_argument('--show-only-issues', action='store_true', help='仅显示存在问题的条目')
    parser.add_argument('--apply-fixes', action='store_true', help='按规则自动修复并更新股池配置文件')
    parser.add_argument('--output-config', type=str, default=None, help='修复后的配置输出路径（默认覆盖原文件）')
    parser.add_argument('--no-backup', action='store_true', help='覆盖原文件时不生成备份')
    args = parser.parse_args()

    cfg_path = Path(args.config)
    pools = load_stock_pools(cfg_path)
    if not pools:
        logger.error('股池配置为空。')
        return 1

    storage = ParquetStorage()
    stock_info_df = build_stock_info_df(storage)
    available_symbols = set(storage.list_available_stocks())

    records = []
    all_matched_symbols: List[str] = []

    logger.info('开始检查股池配置中的股票名称与本地数据库及实时获取状态...')

    for pool_name, concepts in pools.items():
        for concept_name, stock_names in concepts.items():
            if not stock_names:
                continue
            for raw_name in stock_names:
                # Replicate both mappings used in the codebase
                symbol_exact = exact_mapping_symbol(stock_info_df, raw_name)
                symbol_contains = contains_mapping_symbol(stock_info_df, raw_name)

                # Smart helper mapping (normalized + fuzzy)
                match = match_name_to_symbol(stock_info_df, raw_name, args.fuzzy_threshold)
                smart_symbol = match.get('symbol')

                # Historical data availability per mapping
                hist_data_exact = (symbol_exact in available_symbols) if symbol_exact else False
                hist_data_contains = (symbol_contains in available_symbols) if symbol_contains else False
                rec = {
                    'pool': pool_name,
                    'concept': concept_name,
                    'stock_name': raw_name,
                    'name_norm': normalize_name(raw_name),
                    # Exact (IndexCalculator)
                    'db_exact_match': bool(symbol_exact),
                    'db_exact_symbol': symbol_exact,
                    'hist_data_exact': hist_data_exact,
                    # Contains (IndexComparator)
                    'rt_contains_match': bool(symbol_contains),
                    'rt_contains_symbol': symbol_contains,
                    'hist_data_rt_symbol': hist_data_contains,
                    # Smart helper
                    'smart_match': match.get('match'),
                    'smart_match_type': match.get('match_type'),
                    'smart_symbol': smart_symbol,
                    'smart_matched_db_name': match.get('matched_name'),
                    'smart_confidence': match.get('confidence'),
                    # Realtime
                    'realtime_fetchable': None,
                    'notes': ''
                }
                # Notes and hints
                if not rec['db_exact_match'] and rec['rt_contains_match']:
                    rec['notes'] = '下载路径(Exact)无法匹配，但实时路径(Contains)可匹配。考虑修正配置名称为正式简称。'
                elif rec['db_exact_match'] and not rec['rt_contains_match']:
                    rec['notes'] = '下载路径(Exact)能匹配，但实时路径(Contains)无法匹配，可能为带前缀名称导致。'
                elif not rec['db_exact_match'] and not rec['rt_contains_match']:
                    if rec['smart_match']:
                        rec['notes'] = f"建议替换为: {rec['smart_matched_db_name']} ({rec['smart_symbol']})"
                    else:
                        rec['notes'] = '两种路径均无法匹配，请检查名称是否含有 XD/*ST 等前缀或是否为旧简称。'

                records.append(rec)
                # Collect rt symbols for realtime probe
                if symbol_contains:
                    all_matched_symbols.append(symbol_contains)

    # Realtime check (optional)
    earliest_ts = None
    if args.realtime and all_matched_symbols:
        rt_map, earliest_ts, ran_flag = batch_check_realtime(all_matched_symbols, args.batch_size)
        for rec in records:
            sym = rec['rt_contains_symbol']
            if sym:
                rec['realtime_fetchable'] = (rt_map.get(sym) if ran_flag else None)
            else:
                rec['realtime_fetchable'] = None if not ran_flag else False

    df = pd.DataFrame.from_records(records)

    # Apply fixes to config if requested
    if args.apply_fixes:
        logger.info('开始按规则修复股池配置文件...')
        full_cfg = _load_full_yaml(cfg_path)
        new_pools, stats, change_list = apply_fixes_to_stock_pools(pools, records, stock_info_df)
        out_cfg_path = Path(args.output_config) if args.output_config else None
        saved_path = _save_stock_pools_config(cfg_path, full_cfg, new_pools, out_cfg_path, backup=(not args.no_backup))
        logger.info(f"已写入修复后的配置: {saved_path}")
        logger.info(f"修复统计: 变更 {stats['changed']} 条, 删除 {stats['removed']} 条, 保留 {stats['unchanged']} 条, 总计 {stats['total']} 条")

        # Save changes report
        changes_df = pd.DataFrame(change_list)
        changes_out = Path(__file__).resolve().parents[1] / 'reports' / 'stock_pools_fixes.csv'
        changes_out.parent.mkdir(parents=True, exist_ok=True)
        changes_df.to_csv(changes_out, index=False, encoding='utf-8-sig')
        logger.info(f'修复明细已保存: {changes_out}')

    # Decide output format
    out_path = Path(args.output)
    if args.format is None:
        out_fmt = 'json' if out_path.suffix.lower() == '.json' else 'csv'
    else:
        out_fmt = args.format

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Optionally filter issues for console display
    # Define issues: any mapping failure or data/fetch problem
    issues_df = df[(~df['db_exact_match']) | (~df['rt_contains_match']) | (~df['hist_data_rt_symbol']) | (df['realtime_fetchable'] == False)]

    # Save
    if out_fmt == 'csv':
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
    else:
        out_path.write_text(df.to_json(orient='records', force_ascii=False, indent=2), encoding='utf-8')

    # Console summary
    total = len(df)
    unmatched_exact = int((~df['db_exact_match']).sum())
    unmatched_contains = int((~df['rt_contains_match']).sum())
    no_hist_rt = int((~df['hist_data_rt_symbol']).sum())
    rt_unknown = int(df['realtime_fetchable'].isna().sum())
    rt_fail = int((df['realtime_fetchable'] == False).sum()) if 'realtime_fetchable' in df.columns else 0

    logger.info('检查完成')
    logger.info(f'总条目: {total}, Exact未匹配: {unmatched_exact}, Contains未匹配: {unmatched_contains}, 无历史数据(Contains符号): {no_hist_rt}, 实时未检查: {rt_unknown}, 实时不可获取: {rt_fail}')
    if earliest_ts:
        logger.info(f'实时数据最早时间戳: {earliest_ts}')

    # Print top issues (up to 50 lines)
    display_df = issues_df if args.show_only_issues else df
    if not display_df.empty:
        cols = [
            'pool', 'concept', 'stock_name',
            'db_exact_symbol', 'rt_contains_symbol', 'smart_symbol',
            'hist_data_exact', 'hist_data_rt_symbol', 'realtime_fetchable',
            'notes'
        ]
        cols = [c for c in cols if c in display_df.columns]
        try:
            print(display_df[cols].head(50).to_string(index=False))
            if len(display_df) > 50:
                print(f"... 以及更多 {len(display_df)-50} 条问题，请查看输出文件 {out_path}")
        except Exception:
            # Fallback simple print
            print(f"问题条目示例(前50条): {out_path}")

    print(f"已保存检查结果到: {out_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
