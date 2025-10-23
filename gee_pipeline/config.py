
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import os

@dataclass
class Config:
    area_name: str
    yield_year: int
    roi_path: Optional[str] = None
    roi_ee_asset: Optional[str] = None
    crs_local: str = "EPSG:32754"
    date_start: str = "2018-01-01"
    date_end: str = "2018-12-31"
    collections: Dict[str, str] = field(default_factory=lambda: {
        "s2": "COPERNICUS/S2_SR_HARMONIZED",
        "hls": "NASA/HLS/HLSL30/v002"
    })
    indices: List[str] = field(default_factory=lambda: ["NDVI"])
    export_root: str = "Outputs"
    pixel_scale: int = 10
    max_pixels: float = 1e13
    preview: bool = True
    make_parquet: bool = True
    make_csv: bool = False
    sample_points_fc: Optional[str] = None
    sample_size: Optional[int] = None

    def validate(self) -> None:
        assert self.area_name, "area_name must be provided"
        assert isinstance(self.yield_year, int), "yield_year must be int"
        assert self.date_start <= self.date_end, "date_start must be <= date_end"
        if self.roi_path is None and self.roi_ee_asset is None:
            raise ValueError("Provide either roi_path or roi_ee_asset")
        os.makedirs(self.export_dir, exist_ok=True)

    @property
    def export_dir(self) -> str:
        return os.path.join(self.export_root, self.area_name, str(self.yield_year))
