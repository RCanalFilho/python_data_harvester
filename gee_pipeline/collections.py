
from __future__ import annotations
import ee

def mask_s2_clouds(img: ee.Image) -> ee.Image:
    qa = img.select('QA60')
    cloud_bit = 10
    cirrus_bit = 11
    mask = qa.bitwiseAnd(1 << cloud_bit).eq(0).And(qa.bitwiseAnd(1 << cirrus_bit).eq(0))
    return img.updateMask(mask).copyProperties(img, img.propertyNames())

def select_s2_bands(img: ee.Image) -> ee.Image:
    bands_src = ['B2','B3','B4','B8','B5','B6','B7','B8A','B11','B12']
    bands_dst = ['BLUE','GREEN','RED','NIR','RE1','RE2','RE3','RE4','SWIR1','SWIR2']
    return (img
            .select(bands_src, bands_dst)
            .resample('bilinear'))

def build_s2_collection(s2_id: str, start: str, end: str, roi_geom: ee.Geometry) -> ee.ImageCollection:
    return (ee.ImageCollection(s2_id)
            .filterDate(start, end)
            .filterBounds(roi_geom)
            .map(mask_s2_clouds)
            .map(select_s2_bands))
