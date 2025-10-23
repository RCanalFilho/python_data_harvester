
from __future__ import annotations
import ee, os
from .config import Config
from .logging_setup import setup_logger
from .report import RunReport
from .roi import load_roi
from .collections import build_s2_collection
from .timeseries import compose_time_series, safe_monthly_mosaics
from .cube import assemble_cube
from .export import export_cube_table, export_pixel_samples
from .preview import show_on_map

def run_pipeline_quick(cfg: Config) -> RunReport:
    cfg.validate()
    log_dir = os.path.join(cfg.export_dir, "logs")
    logger = setup_logger(log_dir, verbose_console=True)
    report = RunReport()

    try:
        ee.Initialize()
        logger.info("EE initialized.")
    except Exception as e:
        logger.error(f"Earth Engine init failed: {e}")
        report.add_error("ee.Initialize", e)
        return report

    try:
        roi_fc, roi_geom = load_roi(cfg.roi_path, cfg.roi_ee_asset)
        report.add_step("ROI loaded", {"source": cfg.roi_path or cfg.roi_ee_asset})
    except Exception as e:
        logger.error(f"ROI loading failed: {e}")
        report.add_error("load_roi", e)
        return report

    try:
        col = build_s2_collection(cfg.collections["s2"], cfg.date_start, cfg.date_end, roi_geom)
        ts = compose_time_series(col, cfg.indices)
        report.add_step("Time series composed")
    except Exception as e:
        logger.error(f"Time series failed: {e}")
        report.add_error("compose_time_series", e)
        return report

    try:
        monthly = safe_monthly_mosaics(ts)
        report.add_step("Monthly mosaics created")
    except Exception as e:
        logger.error(f"Monthly mosaics failed: {e}")
        report.add_error("safe_monthly_mosaics", e)
        return report

    try:
        cube = assemble_cube(monthly)
        report.add_step("Cube assembled", {"bands": cube.bandNames().size().getInfo()})
    except Exception as e:
        logger.error(f"Assemble cube failed: {e}")
        report.add_error("assemble_cube", e)
        return report

    try:
        p = export_cube_table(cfg, cube, roi_geom, logger)
        if p: report.add_artifact(p, "table")
    except Exception as e:
        logger.error(f"Export table failed: {e}")
        report.add_error("export_cube_table", e)

    try:
        p2 = export_pixel_samples(cfg, cube, roi_geom, logger)
        if p2: report.add_artifact(p2, "samples")
    except Exception as e:
        logger.error(f"Export samples failed: {e}")
        report.add_error("export_pixel_samples", e)

    if cfg.preview:
        try:
            _ = show_on_map(cube, roi_fc)
            report.add_step("Preview map ready")
        except Exception as e:
            logger.warning(f"Preview failed (skipping): {e}")
            report.add_error("preview", e)

    return report
