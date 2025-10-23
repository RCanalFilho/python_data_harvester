
from __future__ import annotations
import os, re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Iterable, Tuple

import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from shapely.geometry import Point
from pyproj import Transformer

from .utils import ensure_dir
import logging

SOF_BASE = "https://data.tern.org.au/model-derived/slga/NationalMaps/SoilAndLandscapeGrid/SOF/v1"  

FAMILIES = {"Fractions_Density": "SOF_Fractions_Density",
            "Proportions":       "SOF_Proportions",
            "Stocks":            "SOF_Stocks"}

FRACTIONS = ["MAOC", "POC", "PyOC"]
DEPTHS    = ["000_005","005_015","015_030"]  
STATS     = ["EV","05","95"]            

def _sof_is_available(family: str, depth: str, stat: str, fraction: str) -> bool:
    if family == "Fractions_Density":
        return (depth in ("000_005","005_015","015_030")) and (stat in ("EV","05","95"))
    if family == "Proportions":
        if depth == "000_005":
            return stat in ("EV","05","95")
        if depth in ("005_015","015_030"):
            return stat in ("05","95")  
        return False
    if family == "Stocks":
        return (depth == "000_030") and (stat == "EV")
    return False

def _sof_url(family: str, depth: str, stat: str, fraction: str) -> str:
    fam_dir = FAMILIES[family]
    if family == "Stocks":
        if depth != "000_030" or stat != "EV":
            raise ValueError("Stocks: only depth 000_030 and stat EV are available.")
        fname = f"SOF_000_030_EV_N_P_AU_TRN_N_20221006_Fractions_Stock_{fraction}.tif"
    else:
        if depth not in DEPTHS:
            raise ValueError(f"Depth {depth} invalid for {family}. Use one of {DEPTHS}.")
        if stat not in STATS:
            raise ValueError(f"stat must be one of {STATS}")
        suffix = "Fraction_Density" if family == "Fractions_Density" else "Proportion"
        fname = f"SOF_{depth}_{stat}_N_P_AU_TRN_N_20221006_{suffix}_{fraction}.tif"
    return f"{SOF_BASE}/{fam_dir}/{fname}"

@dataclass
class SOFPointsConfig:
    area_name: str
    points_path: str
    families: List[str] = field(default_factory=lambda: ["Fractions_Density"])
    fractions: List[str] = field(default_factory=lambda: ["MAOC","POC","PyOC"])
    depths: List[str] = field(default_factory=lambda: ["000_005","005_015","015_030"])
    stat: str = "EV"   
    export_root: str = "Outputs"
    make_parquet: bool = True
    make_csv: bool = False
    log_to_file: bool = True
    cookie_file: str | None = None

    @property
    def export_dir(self) -> str:
        d = os.path.join(self.export_root, self.area_name, "SOF")
        ensure_dir(d); ensure_dir(os.path.join(d, "logs"))
        return d

def _make_logger(out_dir: str, name="gee_pipeline.sof") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
        fh = logging.FileHandler(os.path.join(out_dir, "logs", "sof_points.log"), encoding="utf-8")
        fh.setFormatter(fmt); logger.addHandler(fh)
    return logger

def _load_points(points_path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(points_path)
    if gdf.crs is None:
        raise ValueError("Points file has no CRS; define a valid CRS.")
    return gdf

def _sample_cog_at_points(url: str, xs, ys, dst_crs_epsg: int, cookie_file: str | None):
    gdal_cfg = {
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
        "CPL_VSIL_CURL_USE_HEAD": "NO",
        "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.ovr,.tif.ovr",
    }
    if cookie_file:
        gdal_cfg["GDAL_HTTP_COOKIEFILE"] = cookie_file
        gdal_cfg["GDAL_HTTP_COOKIEJAR"] = cookie_file
        gdal_cfg["GDAL_HTTP_USERAGENT"] = "gee-pipeline-sof/1.0"

    vals = []
    with rasterio.Env(**gdal_cfg):
        with rasterio.open(url) as src:
            from pyproj import Transformer
            transformer = Transformer.from_crs(f"EPSG:{dst_crs_epsg}", src.crs, always_xy=True)
            x_src, y_src = transformer.transform(xs, ys)
            samples = list(src.sample(zip(x_src, y_src), indexes=1, masked=True))
            for v in samples:
                if hasattr(v, "mask") and v.mask.any():
                    vals.append(None)
                else:
                    vals.append(float(v[0]))
    return vals

def sof_points_quick(cfg: SOFPointsConfig) -> dict:
    for fam in cfg.families:
        if fam not in FAMILIES:
            raise ValueError(f"Unknown family '{fam}'. Choose from {list(FAMILIES.keys())}.")
    for frac in cfg.fractions:
        if frac not in FRACTIONS:
            raise ValueError(f"Unknown fraction '{frac}'. Choose from {FRACTIONS}.")
    for d in cfg.depths:
        if d not in DEPTHS and "Stocks" not in cfg.families:
            raise ValueError(f"Invalid depth '{d}'. Allowed: {DEPTHS}.")

    logger = _make_logger(cfg.export_dir) if cfg.log_to_file else logging.getLogger("gee_pipeline.sof")
    logger.info(
        f"SOF points run | area='{cfg.area_name}' | fam={cfg.families} | "
        f"frac={cfg.fractions} | depths={cfg.depths} | stat={cfg.stat}"
    )

    gdf = _load_points(cfg.points_path)
    if gdf.crs.to_epsg() is None:
        gdf = gdf.to_crs(4326)
    epsg = gdf.crs.to_epsg() or 4326

    geom_name = gdf.geometry.name
    gdf_points = gdf.copy()
    gdf_points[geom_name] = gdf_points[geom_name].apply(lambda g: g if isinstance(g, Point) else g.centroid)
    xs = gdf_points.geometry.x.values
    ys = gdf_points.geometry.y.values

    gdf_wgs84 = gdf_points.to_crs(4326)
    out_df = gdf_points.drop(columns=[geom_name]).copy()
    out_df["lon"] = gdf_wgs84.geometry.x.values
    out_df["lat"] = gdf_wgs84.geometry.y.values

    requests: List[Tuple[str, str, str, str]] = []
    for fam in cfg.families:
        if fam == "Stocks":
            for frac in cfg.fractions:
                if _sof_is_available(fam, "000_030", "EV", frac):
                    requests.append((fam, "000_030", "EV", frac))
                else:
                    logger.warning(f"[skip] {fam} EV 000_030 {frac}: not published")
        else:
            for d in cfg.depths:
                for frac in cfg.fractions:
                    if _sof_is_available(fam, d, cfg.stat, frac):
                        requests.append((fam, d, cfg.stat, frac))
                    else:
                        logger.warning(f"[skip] {fam} {cfg.stat} {d} {frac}: not published")

    for fam, d, st, frac in requests:
        url = _sof_url(fam, d, st, frac)
        kind = "STOCK" if fam == "Stocks" else ("DENS" if fam == "Fractions_Density" else "PROP")
        depth_tag = "000_030" if fam == "Stocks" else d
        col = f"{frac}_{depth_tag}_{st}_{kind}"
        try:
            vals = _sample_cog_at_points(url, xs, ys, dst_crs_epsg=epsg, cookie_file=cfg.cookie_file)
            out_df[col] = vals
            logger.info(f"Sampled: {col}")
        except Exception as e:
            logger.error(f"Failed sampling {url}: {e}")
            out_df[col] = None

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{cfg.area_name}_SOF_POINTS_{stamp}"
    pqt = os.path.join(cfg.export_dir, base + ".parquet")
    csv = os.path.join(cfg.export_dir, base + ".csv")
    if cfg.make_parquet:
        out_df.to_parquet(pqt, index=False)
    if cfg.make_csv:
        out_df.to_csv(csv, index=False)

    logger.info(f"Saved: {pqt if cfg.make_parquet else ''} / {csv if cfg.make_csv else ''} [{len(out_df)} rows]")
    return {
        "table_parquet": pqt if cfg.make_parquet else "",
        "table_csv": csv if cfg.make_csv else "",
        "n_rows": len(out_df),
    }
