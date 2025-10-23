
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import os, time, math, logging
import pandas as pd
import geopandas as gpd
import requests
from datetime import datetime
from shapely.geometry import Point
from .utils import ensure_dir

SILO_BASE = "https://www.longpaddock.qld.gov.au/cgi-bin/silo"

VALID_CODES = set(list("RXNVDESCLJHGFTAPWM"))  

def _snap05(x: float) -> float:
    return round(x / 0.05) * 0.05  

@dataclass
class SILOPointsConfig:
    area_name: str
    mode: str = "datadrill"               
    points_path: Optional[str] = None
    lat_field: Optional[str] = None
    lon_field: Optional[str] = None
    station_field: Optional[str] = None
    variables: List[str] = field(default_factory=lambda: ["R","X","N"])
    date_start: str = "2019-01-01"
    date_end: str = "2019-12-31"
    username: str = ""                     
    password: str = "apirequest"           
    fmt: str = "csv"                       
    export_root: str = "Outputs"
    make_parquet: bool = True
    make_csv: bool = False
    log_to_file: bool = True
    batch_size: int = 50
    retry_max: int = 3
    retry_wait: float = 1.5               

    @property
    def export_dir(self) -> str:
        d = os.path.join(self.export_root, self.area_name, "SILO")
        ensure_dir(d); ensure_dir(os.path.join(d, "logs"))
        return d

def _make_logger(out_dir: str) -> logging.Logger:
    lg = logging.getLogger("gee_pipeline.silo")
    lg.setLevel(logging.INFO)
    if not lg.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        ch = logging.StreamHandler(); ch.setFormatter(fmt); lg.addHandler(ch)
        fh = logging.FileHandler(os.path.join(out_dir, "logs", "silo_points.log"), encoding="utf-8")
        fh.setFormatter(fmt); lg.addHandler(fh)
    return lg

def _load_points(cfg: SILOPointsConfig) -> pd.DataFrame:
    if cfg.mode == "station":
        if cfg.points_path:
            df = gpd.read_file(cfg.points_path)
            if cfg.station_field is None:
                raise ValueError("station_field is required for mode='station'.")
            return pd.DataFrame({ "station": df[cfg.station_field].astype(int) })
        raise ValueError("Provide points_path with a station_field for mode='station'.")
    if cfg.points_path:
        gdf = gpd.read_file(cfg.points_path)
        if gdf.crs is None:
            raise ValueError("Points file has no CRS.")
        gdf = gdf.to_crs(4326)
        gdf["lon"] = gdf.geometry.x
        gdf["lat"] = gdf.geometry.y
        return pd.DataFrame({"lon": gdf["lon"], "lat": gdf["lat"]})
    raise ValueError("For mode='datadrill', provide a points_path with point geometry (CRS set).")

def _validate_vars(vars: List[str]) -> str:
    up = [str(v).strip().upper() for v in vars]
    bad = [v for v in up if v not in VALID_CODES]
    if bad:
        raise ValueError(f"Unknown SILO variable codes: {bad}. See Climate Variables page.")
    return "".join(up)

def _call_api(url: str, params: Dict, retry_max=3, retry_wait=1.5) -> requests.Response:
    for k in range(retry_max+1):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r
        if k == retry_max:
            r.raise_for_status()
        time.sleep(retry_wait * (2 ** k))
    return r 

def _datadrill_row(lon, lat, start, finish, comment, username, password, fmt):
    url = f"{SILO_BASE}/DataDrillDataset.php"
    params = dict(lon=f"{_snap05(lon):.2f}", lat=f"{_snap05(lat):.2f}",
                  start=start, finish=finish, format=fmt,
                  comment=comment, username=username, password=password)
    return url, params

def _patchedpoint_row(station, start, finish, comment, username, fmt):
    url = f"{SILO_BASE}/PatchedPointDataset.php"
    params = dict(station=int(station), start=start, finish=finish,
                  format=fmt, comment=comment, username=username)
    return url, params

def silo_points_quick(cfg: SILOPointsConfig) -> dict:
    logger = _make_logger(cfg.export_dir) if cfg.log_to_file else logging.getLogger("gee_pipeline.silo")
    logger.info(f"SILO points | mode={cfg.mode} | vars={cfg.variables} | {cfg.date_start}→{cfg.date_end}")

    if not cfg.username:
        raise ValueError("username (your email) is required by the SILO API.")

    comment = _validate_vars(cfg.variables)
    start = cfg.date_start.replace("-", "")
    finish = cfg.date_end.replace("-", "")

    base_df = _load_points(cfg)
    out_tables = []
    for idx, row in base_df.reset_index(drop=True).iterrows():
        if cfg.mode == "datadrill":
            url, params = _datadrill_row(row["lon"], row["lat"], start, finish, comment, cfg.username, cfg.password, cfg.fmt)
            tag = f"DD_{row['lat']:.5f}_{row['lon']:.5f}"
        else:
            url, params = _patchedpoint_row(row["station"], start, finish, comment, cfg.username, cfg.fmt)
            tag = f"ST_{int(row['station'])}"

        try:
            r = _call_api(url, params, cfg.retry_max, cfg.retry_wait)
            if cfg.fmt == "json":
                js = r.json()
                recs = js.get("data", js) 
                df = pd.DataFrame(recs)
            else:
                from io import StringIO
                df = pd.read_csv(StringIO(r.text))
            if cfg.mode == "datadrill":
                df["lat_snapped"] = float(params["lat"]); df["lon_snapped"] = float(params["lon"])
            else:
                df["station"] = int(row["station"])
            df["source_tag"] = tag
            out_tables.append(df)
            logger.info(f"[ok] {tag} rows={len(df)}")
        except Exception as e:
            logger.error(f"[fail] {tag}: {e}")

    if not out_tables:
        raise RuntimeError("No tables were downloaded; check API credentials/params.")

    big = pd.concat(out_tables, ignore_index=True)
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{cfg.area_name}_SILO_POINTS_{cfg.mode}_{stamp}"
    pqt = os.path.join(cfg.export_dir, base + ".parquet")
    csv = os.path.join(cfg.export_dir, base + ".csv")
    if cfg.make_parquet: big.to_parquet(pqt, index=False)
    if cfg.make_csv:     big.to_csv(csv, index=False)
    logger.info(f"Saved: {pqt if cfg.make_parquet else ''} / {csv if cfg.make_csv else ''} [{len(big)} rows]")
    return {"table_parquet": pqt if cfg.make_parquet else "", "table_csv": csv if cfg.make_csv else "", "n_rows": len(big)}
