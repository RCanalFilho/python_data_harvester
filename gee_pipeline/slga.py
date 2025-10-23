
from __future__ import annotations
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict

import ee
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from .utils import ensure_dir
import logging


SLGA_EE_ID = "CSIRO/SLGA"            
SLGA_NATIVE_SCALE = 90             

SLGA_ATTRIBUTES: Dict[str, str] = {
    "SOC": "Soil Organic Carbon",
    "CLY": "Clay",
    "SLT": "Silt",
    "SND": "Sand",
    "pHc": "pH (CaCl2)",
    "AWC": "Available Water Capacity",
    "ECE": "Effective Cation Exchange Capacity",
    "NTO": "Total Nitrogen",
    "PTO": "Total Phosphorus",
    "DES": "Soil Depth",
    "DER": "Regolith Depth",
}
SLGA_DEPTHS = ["000_005", "005_015", "015_030", "030_060", "060_100", "100_200"]
SLGA_STATS  = ["EV", "05", "95"]


@dataclass
class SLGAPointsConfig:
    area_name: str
    points_path: str                    
    attributes: List[str] = field(default_factory=lambda: ["SOC"])  
    stat: str = "EV"                        
    depths: List[str] = field(default_factory=lambda: ["000_005","005_015","015_030"])
    scale: int = SLGA_NATIVE_SCALE
    export_root: str = "Outputs"
    make_parquet: bool = True
    make_csv: bool = False
    log_to_file: bool = True

    @property
    def export_dir(self) -> str:
        d = os.path.join(self.export_root, self.area_name, "SLGA")
        ensure_dir(d); ensure_dir(os.path.join(d, "logs"))
        return d

def _make_logger(out_dir: str, name="gee_pipeline.slga"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
        fh = logging.FileHandler(os.path.join(out_dir, "logs", "slga_points.log"), encoding="utf-8")
        fh.setFormatter(fmt); logger.addHandler(fh)
    return logger

def _validate_cfg(cfg: SLGAPointsConfig):
    bad_attr = [a for a in cfg.attributes if a not in SLGA_ATTRIBUTES]
    if bad_attr:
        raise ValueError(f"Unknown attributes: {bad_attr}. Allowed: {list(SLGA_ATTRIBUTES.keys())}")
    if cfg.stat not in SLGA_STATS:
        raise ValueError(f"stat must be one of {SLGA_STATS}")
    for d in cfg.depths:
        if d not in SLGA_DEPTHS:
            raise ValueError(f"Invalid depth '{d}'. Allowed: {SLGA_DEPTHS}")

def _load_points_fc(points_path: str) -> ee.FeatureCollection:
    gdf = gpd.read_file(points_path)
    if gdf.crs is None:
        raise ValueError("Points file has no CRS. Define a valid CRS before using.")
    gdf = gdf.to_crs(4326)
    feats = []
    geom_col = gdf.geometry.name
    base_props_cols = [c for c in gdf.columns if c != geom_col]
    for _, row in gdf.iterrows():
        geom = row.geometry
        if not isinstance(geom, Point):
            geom = geom.centroid
        coords = [geom.x, geom.y]
        props = {k: (row[k] if pd.notna(row[k]) else None) for k in base_props_cols}
        feats.append(ee.Feature(ee.Geometry.Point(coords), props))
    return ee.FeatureCollection(feats)

def _build_slga_image(attributes: List[str], stat: str, depths: List[str]) -> ee.Image:
    bands_total = []
    for attr in attributes:
        col = ee.ImageCollection(SLGA_EE_ID).filter(ee.Filter.eq("attribute_code", attr))
        img = ee.Image(col.first())
        if img is None:
            raise ValueError(f"SLGA attribute '{attr}' not found")
        bands = [f"{attr}_{d}_{stat}" for d in depths]
        bands_total.append(img.select(bands))
    return ee.Image.cat(bands_total)

def _fc_to_dataframe(fc: ee.FeatureCollection, limit: int = 100000) -> pd.DataFrame:
    feats = fc.limit(limit).getInfo().get("features", [])
    rows = []
    for f in feats:
        props = (f.get("properties") or {}).copy()
        geom = f.get("geometry", {})
        if geom and geom.get("type") == "Point":
            coords = geom.get("coordinates", [None, None])
            props["lon"] = coords[0]; props["lat"] = coords[1]
        rows.append(props)
    return pd.DataFrame(rows)

def slga_points_quick(cfg: SLGAPointsConfig) -> dict:
    _validate_cfg(cfg)
    logger = _make_logger(cfg.export_dir) if cfg.log_to_file else logging.getLogger("gee_pipeline.slga")
    logger.info(f"SLGA points run | area='{cfg.area_name}' | attrs={cfg.attributes} | stat={cfg.stat} | depths={cfg.depths}")

    img = _build_slga_image(cfg.attributes, cfg.stat, cfg.depths)

    fc_pts = _load_points_fc(cfg.points_path)
    first_props = ee.Feature(fc_pts.first()).toDictionary().keys().getInfo()

    out_fc = img.sampleRegions(collection=fc_pts, properties=first_props, scale=cfg.scale)

    df = _fc_to_dataframe(out_fc, limit=200000)  

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{cfg.area_name}_SLGA_POINTS_{cfg.stat}_{stamp}"
    pqt_path = os.path.join(cfg.export_dir, base + ".parquet")
    csv_path = os.path.join(cfg.export_dir, base + ".csv")
    if cfg.make_parquet:
        df.to_parquet(pqt_path, index=False)
    if cfg.make_csv:
        df.to_csv(csv_path, index=False)

    logger.info(f"Saved: {pqt_path if cfg.make_parquet else ''} / {csv_path if cfg.make_csv else ''} [{len(df)} rows]")
    return {
        "table_parquet": pqt_path if cfg.make_parquet else "",
        "table_csv": csv_path if cfg.make_csv else "",
        "n_rows": len(df),
    }
