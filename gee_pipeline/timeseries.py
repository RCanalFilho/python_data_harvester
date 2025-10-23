
from __future__ import annotations
import ee
from .indices import apply_indices

def compose_time_series(col: ee.ImageCollection, indices) -> ee.ImageCollection:
    def _per_image(img):
        img2 = apply_indices(img, indices)
        return img2.set({'date': img.date().format('YYYY-MM-dd')})
    return col.map(_per_image)

def safe_monthly_mosaics(ts: ee.ImageCollection) -> ee.ImageCollection:
    first = ee.Image(ts.first())
    last = ee.Image(ts.sort('system:time_start', False).first())
    start = ee.Date(first.date())
    end = ee.Date(last.date())
    nmonths = end.difference(start, 'month').int()
    months = ee.List.sequence(0, nmonths).map(lambda i: start.advance(i, 'month'))

    def by_month(d):
        d = ee.Date(d)
        col = ts.filterDate(d, d.advance(1, 'month'))
        return col.mosaic().set({'date': d.format('YYYY-MM')})

    return ee.ImageCollection(months.map(by_month))
