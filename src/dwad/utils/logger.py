import sys
import os
from datetime import timezone, timedelta
from pathlib import Path
from loguru import logger

# 北京时区 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))

# 默认日志配置
DEFAULT_LOG_LEVEL = 'INFO'
DEFAULT_LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} | {message}"
DEFAULT_LOG_FILE = './logs/dwad.log'
DEFAULT_LOG_ROTATION = '10 MB'
DEFAULT_LOG_RETENTION = '30 days'


def beijing_time_formatter(record):
    """将日志时间转换为北京时间"""
    record["time"] = record["time"].astimezone(BEIJING_TZ)


# 立即配置 loguru 使用北京时间（在任何其他模块导入 logger 之前）
# 这样即使其他模块在 setup_logger() 之前使用 logger，时间也是北京时间
logger.configure(patcher=beijing_time_formatter)


def setup_logger() -> None:
    """设置日志配置
    
    注意：为避免循环依赖，此函数不依赖 config 模块。
    日志配置使用默认值，如需自定义请修改此文件中的默认常量。
    """
    # 移除默认处理器
    logger.remove()

    # 首先配置 loguru 使用北京时间（必须在添加 handler 之前）
    logger.configure(patcher=beijing_time_formatter)

    # 使用默认配置（避免循环依赖 config 模块）
    log_level = os.environ.get('DWAD_LOG_LEVEL', DEFAULT_LOG_LEVEL)
    log_format = DEFAULT_LOG_FORMAT
    log_file = os.environ.get('DWAD_LOG_FILE', DEFAULT_LOG_FILE)
    rotation = DEFAULT_LOG_ROTATION
    retention = DEFAULT_LOG_RETENTION

    # 控制台输出（使用北京时间）
    logger.add(
        sys.stderr,
        format=log_format,
        level=log_level,
        colorize=True
    )

    # 文件输出（使用北京时间）
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger.add(
        log_file,
        format=log_format,
        level=log_level,
        rotation=rotation,
        retention=retention,
        encoding='utf-8'
    )

    logger.info("日志系统初始化完成")


# 初始化日志
setup_logger()