#!/usr/bin/env python3
"""Generate scientific geographic heatmaps for Pernambuco municipalities.

Outputs:
1) Base map of Pernambuco municipal boundaries.
2) Heatmap map with georeferenced markers (points or municipalities).

Input options:
- Coordinates file (CSV or JSON) with lat/lon and optional intensity/label.
- Municipality list with optional intensities.
"""

from __future__ import annotations

import argparse
import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import geobr
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

try:
    from shapely import contains_xy
except ImportError:  # pragma: no cover
    contains_xy = None


TARGET_CRS_METRIC = "EPSG:31985"  # SIRGAS 2000 / UTM zone 25S (meters), suitable for Pernambuco
INPUT_CRS = "EPSG:4326"  # WGS84 lat/lon for coordinate input


@dataclass
class MarkerInput:
    lon: float
    lat: float
    intensity: float
    label: str


def normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.split())


def load_pernambuco_municipalities() -> gpd.GeoDataFrame:
    gdf = geobr.read_municipality(code_muni="PE", year=2020, simplified=False)
    return gdf


def load_markers_from_csv(path: Path) -> list[MarkerInput]:
    df = pd.read_csv(path)
    required = {"lat", "lon"}
    if not required.issubset(df.columns):
        raise ValueError("CSV must contain columns: lat, lon")

    markers: list[MarkerInput] = []
    for idx, row in df.iterrows():
        lat = float(row["lat"])
        lon = float(row["lon"])
        intensity = float(row.get("intensity", 1.0))
        label = str(row.get("label", f"ponto_{idx+1}"))
        markers.append(MarkerInput(lon=lon, lat=lat, intensity=intensity, label=label))
    return markers


def load_markers_from_json(path: Path) -> list[MarkerInput]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("JSON input must be a list of objects")

    markers: list[MarkerInput] = []
    for idx, row in enumerate(payload):
        if not isinstance(row, dict):
            raise ValueError("Each JSON item must be an object")
        if "lat" not in row or "lon" not in row:
            raise ValueError("Each JSON item must include lat and lon")
        lat = float(row["lat"])
        lon = float(row["lon"])
        intensity = float(row.get("intensity", 1.0))
        label = str(row.get("label", f"ponto_{idx+1}"))
        markers.append(MarkerInput(lon=lon, lat=lat, intensity=intensity, label=label))
    return markers


def load_markers_from_file(path: Path) -> list[MarkerInput]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return load_markers_from_csv(path)
    if suffix == ".json":
        return load_markers_from_json(path)
    raise ValueError("Unsupported input file format. Use .csv or .json")


def load_markers_from_municipalities(
    municipalities_csv: str,
    intensities_csv: str | None,
    pe_municipalities: gpd.GeoDataFrame,
) -> list[MarkerInput]:
    names = [n.strip() for n in municipalities_csv.split(",") if n.strip()]
    if not names:
        return []

    if intensities_csv:
        raw_values = [v.strip() for v in intensities_csv.split(",") if v.strip()]
        intensities = [float(v) for v in raw_values]
        if len(intensities) != len(names):
            raise ValueError("Number of intensities must match number of municipalities")
    else:
        intensities = [1.0] * len(names)

    pe_municipalities = pe_municipalities.copy()
    pe_municipalities["_norm_name"] = pe_municipalities["name_muni"].map(normalize_text)

    markers: list[MarkerInput] = []
    for name, intensity in zip(names, intensities):
        target = normalize_text(name)
        match = pe_municipalities[pe_municipalities["_norm_name"] == target]
        if match.empty:
            raise ValueError(f"Municipality not found in Pernambuco map: {name}")

        point = match.iloc[0].geometry.representative_point()
        markers.append(
            MarkerInput(
                lon=float(point.x),
                lat=float(point.y),
                intensity=float(intensity),
                label=str(match.iloc[0]["name_muni"]),
            )
        )

    return markers


def markers_to_geodataframe(markers: list[MarkerInput]) -> gpd.GeoDataFrame:
    if not markers:
        raise ValueError("No markers provided. Use --points-file or --municipios")

    df = pd.DataFrame(
        {
            "label": [m.label for m in markers],
            "intensity": [m.intensity for m in markers],
            "lon": [m.lon for m in markers],
            "lat": [m.lat for m in markers],
        }
    )
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=INPUT_CRS)
    return gdf


def build_heat_surface(
    points_metric: gpd.GeoDataFrame,
    pe_metric: gpd.GeoDataFrame,
    grid_size: int,
    sigma_meters: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    minx, miny, maxx, maxy = pe_metric.total_bounds
    x = np.linspace(minx, maxx, grid_size)
    y = np.linspace(miny, maxy, grid_size)
    xx, yy = np.meshgrid(x, y)

    px = points_metric.geometry.x.to_numpy()
    py = points_metric.geometry.y.to_numpy()
    pv = points_metric["intensity"].to_numpy(dtype=float)

    # Gaussian kernel heat surface with physically meaningful distance decay.
    heat = np.zeros_like(xx, dtype=float)
    for x0, y0, v0 in zip(px, py, pv):
        dist2 = (xx - x0) ** 2 + (yy - y0) ** 2
        heat += v0 * np.exp(-dist2 / (2.0 * sigma_meters ** 2))

    # Clip raster to Pernambuco geometry for geographic consistency.
    union_geom = pe_metric.union_all()
    if contains_xy is not None:
        mask_flat = contains_xy(union_geom, xx.ravel(), yy.ravel())
        mask = mask_flat.reshape(xx.shape)
    else:  # pragma: no cover
        mask = np.ones_like(xx, dtype=bool)

    heat = np.where(mask, heat, np.nan)
    return xx, yy, heat


def save_base_map(pe_metric: gpd.GeoDataFrame, output_path: Path, dpi: int) -> None:
    fig, ax = plt.subplots(figsize=(10, 12), facecolor="white")
    pe_metric.plot(ax=ax, facecolor="#f8f8f8", edgecolor="#6f6f6f", linewidth=0.35)
    ax.set_title("Pernambuco - Limites Municipais", fontsize=15, fontweight="bold", pad=12)
    ax.set_axis_off()

    fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def save_heat_map(
    pe_metric: gpd.GeoDataFrame,
    markers_metric: gpd.GeoDataFrame,
    heat: np.ndarray,
    output_path: Path,
    dpi: int,
) -> None:
    cmap = LinearSegmentedColormap.from_list(
        "climate_heat",
        ["#2166ac", "#1a9850", "#fee08b", "#d73027"],
        N=256,
    )

    minx, miny, maxx, maxy = pe_metric.total_bounds

    fig, ax = plt.subplots(figsize=(10, 12), facecolor="white")
    im = ax.imshow(
        heat,
        extent=[minx, maxx, miny, maxy],
        origin="lower",
        cmap=cmap,
        alpha=0.82,
        interpolation="bilinear",
    )

    pe_metric.boundary.plot(ax=ax, color="#3b3b3b", linewidth=0.38)

    markers_metric.plot(
        ax=ax,
        color="#0a0a0a",
        markersize=np.clip(markers_metric["intensity"].to_numpy() * 18.0, 30, 260),
        edgecolor="#ffffff",
        linewidth=0.45,
        zorder=5,
    )

    for _, row in markers_metric.iterrows():
        ax.annotate(
            str(row["label"]),
            (row.geometry.x, row.geometry.y),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=7,
            color="#111111",
        )

    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02)
    cbar.set_label("Intensidade (unidade da entrada)", fontsize=9)

    ax.set_title("Pernambuco - Heatmap Climatico com Marcacoes Georreferenciadas", fontsize=14, fontweight="bold", pad=12)
    ax.set_axis_off()

    fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Pernambuco scientific base map + heatmap")
    parser.add_argument("--points-file", type=Path, default=None, help="CSV or JSON with lat/lon/intensity/label")
    parser.add_argument("--municipios", type=str, default="", help="Comma-separated Pernambuco municipality names")
    parser.add_argument("--intensidades", type=str, default="", help="Comma-separated intensities for --municipios")
    parser.add_argument("--output-dir", type=Path, default=Path("static/maps"), help="Output directory")
    parser.add_argument("--base-output", type=str, default="pernambuco_mapa_base.png", help="Base map output filename")
    parser.add_argument(
        "--heat-output",
        type=str,
        default="pernambuco_heatmap_marcacoes.png",
        help="Heatmap output filename",
    )
    parser.add_argument("--dpi", type=int, default=300, help="PNG export resolution")
    parser.add_argument("--grid-size", type=int, default=500, help="Heat surface grid size")
    parser.add_argument("--sigma-km", type=float, default=35.0, help="Kernel spread in km")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    pe = load_pernambuco_municipalities()

    markers: list[MarkerInput] = []
    if args.points_file is not None:
        markers.extend(load_markers_from_file(args.points_file))

    if args.municipios.strip():
        markers.extend(
            load_markers_from_municipalities(
                municipalities_csv=args.municipios,
                intensities_csv=args.intensidades.strip() or None,
                pe_municipalities=pe,
            )
        )

    markers_geo = markers_to_geodataframe(markers)

    pe_metric = pe.to_crs(TARGET_CRS_METRIC)
    markers_metric = markers_geo.to_crs(TARGET_CRS_METRIC)

    _, _, heat = build_heat_surface(
        points_metric=markers_metric,
        pe_metric=pe_metric,
        grid_size=args.grid_size,
        sigma_meters=args.sigma_km * 1000.0,
    )

    base_path = args.output_dir / args.base_output
    heat_path = args.output_dir / args.heat_output

    save_base_map(pe_metric=pe_metric, output_path=base_path, dpi=args.dpi)
    save_heat_map(
        pe_metric=pe_metric,
        markers_metric=markers_metric,
        heat=heat,
        output_path=heat_path,
        dpi=args.dpi,
    )

    print(f"Base map exported: {base_path}")
    print(f"Heatmap exported: {heat_path}")


if __name__ == "__main__":
    main()
