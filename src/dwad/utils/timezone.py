"""时区工具模块

统一使用北京时间 (UTC+8) 作为系统时区。
"""

from datetime import datetime, timezone, timedelta

# 北京时区 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))


def now_beijing() -> datetime:
    """获取当前北京时间（带时区信息）。"""
    return datetime.now(BEIJING_TZ)


def now_beijing_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """获取当前北京时间的字符串表示。"""
    return now_beijing().strftime(fmt)


def now_beijing_iso() -> str:
    """获取当前北京时间的 ISO 格式字符串（精确到秒）。"""
    return now_beijing().strftime("%Y-%m-%dT%H:%M:%S")


def today_beijing() -> str:
    """获取当前北京时间的日期字符串 (YYYY-MM-DD)。"""
    return now_beijing().strftime("%Y-%m-%d")


def beijing_time(dt: datetime) -> datetime:
    """将 datetime 转换为北京时间。
    
    如果 dt 没有时区信息，假定它是 UTC 时间。
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BEIJING_TZ)
