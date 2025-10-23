
from __future__ import annotations
import ee

def random_points_in_roi(roi_geom: ee.Geometry, n=1000, seed=42) -> ee.FeatureCollection:
    return ee.FeatureCollection.randomPoints(region=roi_geom, points=n, seed=seed)
