import sys
from pathlib import Path
from loguru import logger
from .config import config


def setup_logger() -> None:
    """设置日志配置"""
    # 移除默认处理器
    logger.remove()

    # 获取日志配置
    log_level = config.get('logging.level', 'INFO')
    log_format = config.get('logging.format',
                           "{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} | {message}")
    log_file = config.get('logging.file_path', './logs/dwad.log')
    rotation = config.get('logging.rotation', '10 MB')
    retention = config.get('logging.retention', '30 days')

    # 控制台输出
    logger.add(
        sys.stderr,
        format=log_format,
        level=log_level,
        colorize=True
    )

    # 文件输出
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