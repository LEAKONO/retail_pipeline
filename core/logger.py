import sys
from pathlib import Path
from loguru import logger


def setup_logger(log_level: str = "INFO") -> None:
    logger.remove()
    logger.add(
        sink=sys.stdout,
        level=log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger.add(
        sink=log_dir / "pipeline_{time:YYYY-MM-DD}.log",
        level=log_level,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{level: <8} | "
            "{name}:{function} | "
            "{message}"
        ),
        rotation="00:00",
        retention="30 days",
        compression="zip",
        encoding="utf-8",
    )


__all__ = ["logger", "setup_logger"]
