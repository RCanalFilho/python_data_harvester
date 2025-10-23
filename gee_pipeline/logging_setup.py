
import logging, os, datetime
from logging.handlers import RotatingFileHandler

def now_str():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def setup_logger(log_dir: str, name: str = "gee_pipeline", verbose_console: bool = True) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = RotatingFileHandler(os.path.join(log_dir, f"{name}.log"), maxBytes=5_000_000, backupCount=3)
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if verbose_console:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    logger.info(f"Logger initialized at {now_str()} in {log_dir}")
    return logger
