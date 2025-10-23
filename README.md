gee-pipeline


gee_pipeline/

init.py			export Config, build_user_panel, run_pipeline_quick, RunReport

collections.py 		Sentinel-2 SR harmonized, simple cloud mask + bands

config.py		Config object (validate and standardize paths)

cube.py			rename bands with dates and build a multi-band cube

export.py		export summary table (Parquet/CSV) and pixel samples (Parquet)

indices.py		index library and applier

logging_setup.py	cyclic logger + console

panels.py		simple ipywidgets panel that returns a Config via box.get_config()

preview.py		preview layer via geemap

report.py		RunReport accumulates steps, artifacts and errors; summary_text()

roi.py			load ROI of file (dissolve→EPSG:4326) or GEE asset

runner.py		run_pipeline_quick(cfg) for minimalist end-to-end, logs and report

sampling.py		sample random points in ROI

silo.py			access climate data repository via silo API

slga.py			access soil data on gee from slga repository

sof.py			access soil organic carbon fractions from CSIRO TERN geotiffs

timeseries.py		composes TS with indexes and generate monthly mosaics

utils.py		timeit, consistent naming and helpers


pyproject.toml + README.md + CheatSheet.pdf

Step-by-step_gee_pipeline.ipynb		guided Jupyter notebook to use above functions
