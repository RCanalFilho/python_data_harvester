from __future__ import annotations
import os, pandas as pd
import ee
from .utils import make_name, ensure_dir

def _ee_dict_to_df(ee_dict: ee.Dictionary) -> pd.DataFrame:
    data = ee_dict.getInfo() or {}
    return pd.DataFrame([data])

def _fc_points_to_df(fc: ee.FeatureCollection, limit: int = 10000) -> pd.DataFrame:
    feats = fc.limit(limit).getInfo().get("features", [])
    rows = []
    for f in feats:
        props = (f.get("properties") or {}).copy()
        geom = f.get("geometry")
        if geom and geom.get("type") == "Point":
            coords = geom.get("coordinates", [None, None])
            props["lon"] = coords[0]
            props["lat"] = coords[1]
        rows.append(props)
    return pd.DataFrame(rows)

def export_cube_table(cfg, image: ee.Image, roi_geom: ee.Geometry, logger):
    name = make_name(cfg.area_name, cfg.yield_year, "cube_stats")
    out_dir = cfg.export_dir
    ensure_dir(out_dir)
    parquet_path = os.path.join(out_dir, f"{name}.parquet")
    csv_path = os.path.join(out_dir, f"{name}.csv")
    try:
        reduced = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=roi_geom,
            scale=cfg.pixel_scale,
            maxPixels=cfg.max_pixels,
        )
        df = _ee_dict_to_df(reduced)
        if cfg.make_parquet:
            df.to_parquet(parquet_path, index=False)
        if cfg.make_csv:
            df.to_csv(csv_path, index=False)
        logger.info(f"Saved table: {parquet_path}")
        return parquet_path
    except Exception as e:
        logger.error(f"export_cube_table failed: {e}")
        return None

def export_pixel_samples(cfg, image: ee.Image, roi_geom: ee.Geometry, logger):
    name = make_name(cfg.area_name, cfg.yield_year, "samples")
    out_dir = cfg.export_dir
    ensure_dir(out_dir)
    parquet_path = os.path.join(out_dir, f"{name}.parquet")
    csv_path = os.path.join(out_dir, f"{name}.csv")
    try:
        n = cfg.sample_size or 5000
        fc = image.sample(
            region=roi_geom,
            scale=cfg.pixel_scale,
            numPixels=n,
            geometries=True,
        )
        df = _fc_points_to_df(fc, limit=n)
        if cfg.make_parquet:
            df.to_parquet(parquet_path, index=False)
        if cfg.make_csv:
            df.to_csv(csv_path, index=False)
        logger.info(f"Saved samples: {parquet_path} [{len(df)} rows]")
        return parquet_path
    except Exception as e:
        logger.error(f"export_pixel_samples failed: {e}")
        return None
