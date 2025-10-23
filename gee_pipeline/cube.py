
from __future__ import annotations
import ee

def rename_with_date(img: ee.Image) -> ee.Image:
    d = ee.String(img.get('date'))
    old = img.bandNames()
    new = old.map(lambda b: ee.String(b).cat('_').cat(d))
    return img.rename(new)

def assemble_cube(ts: ee.ImageCollection) -> ee.Image:
    renamed = ts.map(rename_with_date)
    first = ee.Image(renamed.first())
    def _acc(img, prev):
        prev = ee.Image(prev)
        return prev.addBands(img)
    return ee.Image(ee.ImageCollection(renamed).iterate(_acc, first))
