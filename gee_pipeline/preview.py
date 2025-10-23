
from __future__ import annotations
import geemap, ee

def make_preview_layer(image: ee.Image):
    vis = {'min':0, 'max':1, 'bands':['NIR','RED','GREEN']}
    return image, vis

def show_on_map(image: ee.Image, roi_fc: ee.FeatureCollection, center_zoom=12):
    m = geemap.Map()
    img, vis = make_preview_layer(image)
    m.addLayer(img, vis, 'Preview')
    m.addLayer(roi_fc, {}, 'ROI')
    m.centerObject(roi_fc, center_zoom)
    return m
