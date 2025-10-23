
from __future__ import annotations
from ipywidgets import VBox, HBox, Text, IntText, Checkbox, Button, Layout, HTML
from .config import Config

def build_user_panel(cfg: Config):
    area = Text(value=cfg.area_name, description='Area')
    year = IntText(value=cfg.yield_year, description='Year')
    roi_path = Text(value=cfg.roi_path or '', description='ROI Path')
    roi_asset = Text(value=cfg.roi_ee_asset or '', description='ROI Asset')
    dstart = Text(value=cfg.date_start, description='Start')
    dend = Text(value=cfg.date_end, description='End')
    indices = Text(value=','.join(cfg.indices), description='Indices')
    scale = IntText(value=cfg.pixel_scale, description='Scale')
    preview = Checkbox(value=cfg.preview, description='Preview')
    make_csv = Checkbox(value=cfg.make_csv, description='CSV')
    make_parquet = Checkbox(value=cfg.make_parquet, description='Parquet')
    out = HTML(value="Ready.")
    box = VBox([HBox([area, year]), HBox([roi_path, roi_asset]), HBox([dstart, dend]), HBox([indices, scale]), HBox([preview, make_csv, make_parquet]), out])
    def get_config():
        return Config(
            area_name=area.value,
            yield_year=year.value,
            roi_path=roi_path.value or None,
            roi_ee_asset=roi_asset.value or None,
            date_start=dstart.value,
            date_end=dend.value,
            indices=[s.strip() for s in indices.value.split(',') if s.strip()],
            pixel_scale=scale.value,
            preview=preview.value,
            make_csv=make_csv.value,
            make_parquet=make_parquet.value,
        )
    box.get_config = get_config  
    return box
