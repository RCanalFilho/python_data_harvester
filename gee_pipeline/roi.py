
from __future__ import annotations
import ee, geopandas as gpd
from shapely.geometry import mapping
from .utils import timeit

@timeit
def load_roi(roi_path: str | None, roi_ee_asset: str | None):
    if roi_ee_asset:
        fc = ee.FeatureCollection(roi_ee_asset)
        geom = fc.geometry()
        return fc, geom
    if not roi_path:
        raise ValueError("Provide roi_path or roi_ee_asset")
    gdf = gpd.read_file(roi_path).to_crs(4326)
    dissolved = gdf.dissolve()
    geom = dissolved.geometry.iloc[0]
    ee_geom = ee.Geometry(mapping(geom))
    fc = ee.FeatureCollection([ee.Feature(ee_geom)])
    return fc, ee_geom
