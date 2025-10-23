
from __future__ import annotations
import ee


def _safe_ratio(num: ee.Image, den: ee.Image) -> ee.Image:
    eps = ee.Image.constant(1e-6)
    den_safe = den.where(den.eq(0), eps)
    return num.divide(den_safe)


INDEX_FUNCS = {
    "NDVI":  lambda i: i.normalizedDifference(["NIR", "RED"]).rename("NDVI"),
    "EVI":   lambda i: i.expression(
                    "2.5*((NIR-RED)/(NIR+6*RED-7.5*BLUE+1))",
                    {"NIR": i.select("NIR"), "RED": i.select("RED"), "BLUE": i.select("BLUE")}
                ).rename("EVI"),
    "NDWI":  lambda i: i.normalizedDifference(["GREEN", "NIR"]).rename("NDWI"),  # McFeeters

    "EVI2":  lambda i: i.expression(
                    "2.5*((NIR-RED)/(NIR+2.4*RED+1))",
                    {"NIR": i.select("NIR"), "RED": i.select("RED")}
                ).rename("EVI2"),
    "GNDVI": lambda i: i.normalizedDifference(["NIR", "GREEN"]).rename("GNDVI"),
    "GCI":   lambda i: _safe_ratio(i.select("NIR"), i.select("GREEN")).subtract(1).rename("GCI"),
    "SAVI":  lambda i: i.expression(
                    "((NIR-RED)/(NIR+RED+L))*(1+L)",
                    {"NIR": i.select("NIR"), "RED": i.select("RED"), "L": ee.Image.constant(0.5)}
                ).rename("SAVI"),
    "MSAVI2": lambda i: i.expression(
                    "(2*NIR + 1 - sqrt((2*NIR + 1)^2 - 8*(NIR - RED)))/2",
                    {"NIR": i.select("NIR"), "RED": i.select("RED")}
                ).rename("MSAVI2"),
    "WDRVI": lambda i: i.expression(
                    "((a*NIR - RED)/(a*NIR + RED))",
                    {"NIR": i.select("NIR"), "RED": i.select("RED"), "a": ee.Image.constant(0.1)}  
                ).rename("WDRVI"),


    "NDRE":  lambda i: i.normalizedDifference(["NIR", "RE4"]).rename("NDRE"),
    "CIre":  lambda i: _safe_ratio(i.select("NIR"), i.select("RE4")).subtract(1).rename("CIre"),
    "NDMI":  lambda i: i.normalizedDifference(["NIR", "SWIR1"]).rename("NDMI"),
    "NBR":   lambda i: i.normalizedDifference(["NIR", "SWIR2"]).rename("NBR"),
    "MNDWI": lambda i: i.normalizedDifference(["GREEN", "SWIR1"]).rename("MNDWI"),
}


INDICES_REQUIRE_20M = {"NDRE", "CIRE", "NDMI", "NBR", "MNDWI"}


def apply_indices(img: ee.Image, names):
    names_norm = []
    if names:
        for s in names:
            if s is None:
                continue
            names_norm.append(str(s).strip().upper())
    canon = {k.upper(): k for k in INDEX_FUNCS.keys()}
    out = img
    for n in names_norm:
        key = canon.get(n)
        if not key:
            continue
        out = out.addBands(INDEX_FUNCS[key](img))
    has_20m = any(n in INDICES_REQUIRE_20M for n in names_norm)
    return out.set({"has_20m_indices": has_20m})
