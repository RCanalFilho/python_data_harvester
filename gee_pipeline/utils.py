
import time, functools, re, os, datetime
from typing import Callable

def timeit(fn: Callable):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        t0 = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            dt = time.time() - t0
            print(f"[ok] {fn.__name__} in {dt:.1f}s")
    return wrapper

def now_str():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def make_name(area: str, year: int, stem: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9_\-]", "_", stem)
    return f"{area}_{year}_{stem}"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)
