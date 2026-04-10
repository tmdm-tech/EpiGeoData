#!/usr/bin/env python3
"""Gera mapas cientificos de Pernambuco com base IBGE e marcacoes georreferenciadas.

Saidas:
1) mapa base municipal
2) mapa com marcacoes + intensidade por municipio (escala azul->verde->amarelo->vermelho)
3) painel vertical (base em cima, marcado embaixo)
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import geobr
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import FancyArrowPatch
from matplotlib_scalebar.scalebar import ScaleBar
from shapely.geometry import Point

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = BASE_DIR / "static" / "maps"
TARGET_CRS = "EPSG:5880"  # SIRGAS 2000 / Brazil Polyconic (metros)
INPUT_CRS = "EPSG:4674"  # SIRGAS 2000 geographico (lat/lon)


@dataclass
class HeatmapOutputs:
    base_map: Path
    marked_map: Path
    combined_map: Path


def normalize_text(value: str) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"\s+", " ", text)
    return text.upper()


def read_input_table(input_path: Path) -> pd.DataFrame:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(input_path)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(input_path)
    else:
        # Permite arquivos preparados sem extensao (ex.: "municpios_pe").
        try:
            df = pd.read_csv(input_path)
        except Exception as exc:
            raise ValueError("Formato de entrada invalido. Use CSV ou XLSX.") from exc

    if df.empty:
        raise ValueError("Arquivo de entrada vazio.")

    rename_map = {c.lower().strip(): c for c in df.columns}
    has_municipio = "municipio" in rename_map
    has_lat = "latitude" in rename_map
    has_lon = "longitude" in rename_map

    if not has_municipio and not (has_lat and has_lon):
        raise ValueError(
            "Entrada deve conter 'municipio' ou o par 'latitude' e 'longitude'."
        )

    if "intensidade" not in rename_map:
        df["intensidade"] = 1.0

    if has_municipio:
        df["municipio"] = df[rename_map["municipio"]].astype(str).str.strip()
    else:
        df["municipio"] = ""

    if has_lat:
        df["latitude"] = pd.to_numeric(df[rename_map["latitude"]], errors="coerce")
    else:
        df["latitude"] = pd.NA

    if has_lon:
        df["longitude"] = pd.to_numeric(df[rename_map["longitude"]], errors="coerce")
    else:
        df["longitude"] = pd.NA

    df["intensidade"] = pd.to_numeric(df["intensidade"], errors="coerce").fillna(1.0)
    return df[["municipio", "latitude", "longitude", "intensidade"]].copy()


def load_municipal_boundaries_pe() -> gpd.GeoDataFrame:
    munis = geobr.read_municipality(code_muni="PE", year=2020, simplified=True)
    munis = munis.to_crs(INPUT_CRS)
    munis["municipio_norm"] = munis["name_muni"].map(normalize_text)
    return munis


def rows_to_points(df: pd.DataFrame, munis_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rows = df.copy()
    rows["municipio_norm"] = rows["municipio"].map(normalize_text)

    centroids = (
        munis_gdf[["municipio_norm", "geometry"]]
        .copy()
        .to_crs(TARGET_CRS)
    )
    centroids["geometry"] = centroids.geometry.centroid
    centroids = centroids.to_crs(INPUT_CRS)

    rows = rows.merge(
        centroids.rename(columns={"geometry": "centroid_geom"}),
        on="municipio_norm",
        how="left",
    )

    geometries = []
    for _, row in rows.iterrows():
        if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
            geometries.append(Point(float(row["longitude"]), float(row["latitude"])))
        else:
            geometries.append(row["centroid_geom"])

    rows["geometry"] = geometries
    points = gpd.GeoDataFrame(rows, geometry="geometry", crs=INPUT_CRS)
    points = points.dropna(subset=["geometry"]).copy()

    if points.empty:
        raise ValueError("Nenhum ponto valido foi gerado a partir da entrada.")

    joined = gpd.sjoin(
        points,
        munis_gdf[["name_muni", "municipio_norm", "geometry"]],
        how="left",
        predicate="within",
    )

    outside = joined["name_muni"].isna().sum()
    if outside > 0:
        print(f"Aviso: {outside} marcacao(oes) fora dos limites de PE foram ignoradas no agregado.")

    return joined


def aggregate_intensity_by_municipality(
    points_joined: gpd.GeoDataFrame,
    munis_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    agg = (
        points_joined.dropna(subset=["municipio_norm_right"])  # type: ignore[call-arg]
        .groupby("municipio_norm_right", as_index=False)["intensidade"]
        .sum()
        .rename(columns={"municipio_norm_right": "municipio_norm", "intensidade": "intensidade_total"})
    )

    result = munis_gdf.merge(agg, on="municipio_norm", how="left")
    result["intensidade_total"] = result["intensidade_total"].fillna(0.0)
    return result


def add_cartographic_elements(ax: plt.Axes) -> None:
    ax.set_axis_off()

    scalebar = ScaleBar(
        dx=1,
        units="m",
        location="lower right",
        box_alpha=0.85,
        scale_loc="top",
        color="#2d2d2d",
        length_fraction=0.20,
    )
    ax.add_artist(scalebar)

    north_arrow = FancyArrowPatch(
        (0.94, 0.80),
        (0.94, 0.90),
        transform=ax.transAxes,
        arrowstyle="-|>",
        mutation_scale=16,
        linewidth=1.2,
        color="#2d2d2d",
    )
    ax.add_patch(north_arrow)
    ax.text(0.94, 0.92, "N", transform=ax.transAxes, ha="center", va="bottom", fontsize=11, fontweight="bold")


def plot_base_map(
    municipalities: gpd.GeoDataFrame,
    title: str,
    output_path: Path,
    dpi: int,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 10), facecolor="white")
    municipalities.plot(
        ax=ax,
        color="#f8f8f8",
        edgecolor="#8e8e8e",
        linewidth=0.35,
    )
    ax.set_title(title, fontsize=15, fontweight="bold", pad=10)
    add_cartographic_elements(ax)
    fig.text(0.01, 0.01, "Fonte: IBGE (malha municipal 2020, SIRGAS 2000)", fontsize=9, color="#555555")
    fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_marked_heatmap(
    municipalities: gpd.GeoDataFrame,
    points: gpd.GeoDataFrame,
    title: str,
    output_path: Path,
    dpi: int,
) -> None:
    cmap = LinearSegmentedColormap.from_list(
        "climate_intensity",
        ["#2c7bb6", "#5ab4ac", "#fee08b", "#fdae61", "#d7191c"],
    )

    fig, ax = plt.subplots(figsize=(10, 10), facecolor="white")
    municipalities.plot(
        ax=ax,
        column="intensidade_total",
        cmap=cmap,
        linewidth=0.30,
        edgecolor="#6e6e6e",
        legend=True,
        legend_kwds={
            "label": "Intensidade agregada por municipio",
            "orientation": "vertical",
            "shrink": 0.70,
        },
    )

    points.plot(
        ax=ax,
        color="#111111",
        markersize=28,
        marker="o",
        alpha=0.85,
        edgecolor="white",
        linewidth=0.45,
        zorder=5,
    )

    ax.set_title(title, fontsize=15, fontweight="bold", pad=10)
    add_cartographic_elements(ax)
    fig.text(
        0.01,
        0.01,
        "Fonte: IBGE (malha municipal 2020) + entrada georreferenciada informada pelo usuario",
        fontsize=9,
        color="#555555",
    )
    fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_combined_panel(
    base_municipalities: gpd.GeoDataFrame,
    heat_municipalities: gpd.GeoDataFrame,
    points: gpd.GeoDataFrame,
    title_base: str,
    title_marked: str,
    output_path: Path,
    dpi: int,
) -> None:
    cmap = LinearSegmentedColormap.from_list(
        "climate_intensity",
        ["#2c7bb6", "#5ab4ac", "#fee08b", "#fdae61", "#d7191c"],
    )

    fig, axes = plt.subplots(2, 1, figsize=(10, 18), facecolor="white")

    base_municipalities.plot(
        ax=axes[0],
        color="#f8f8f8",
        edgecolor="#8e8e8e",
        linewidth=0.35,
    )
    axes[0].set_title(title_base, fontsize=14, fontweight="bold", pad=8)
    add_cartographic_elements(axes[0])

    heat_municipalities.plot(
        ax=axes[1],
        column="intensidade_total",
        cmap=cmap,
        linewidth=0.30,
        edgecolor="#6e6e6e",
    )
    points.plot(
        ax=axes[1],
        color="#111111",
        markersize=28,
        marker="o",
        alpha=0.85,
        edgecolor="white",
        linewidth=0.45,
        zorder=5,
    )
    axes[1].set_title(title_marked, fontsize=14, fontweight="bold", pad=8)
    add_cartographic_elements(axes[1])

    fig.text(
        0.01,
        0.008,
        "Fonte: IBGE (malha municipal 2020, SIRGAS 2000) + entrada georreferenciada do usuario",
        fontsize=9,
        color="#555555",
    )
    fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def generate_pernambuco_heatmaps(
    input_path: Path,
    output_dir: Path,
    prefix: str,
    dpi: int,
) -> HeatmapOutputs:
    output_dir.mkdir(parents=True, exist_ok=True)

    input_df = read_input_table(input_path)
    munis_input_crs = load_municipal_boundaries_pe()
    points_input_crs = rows_to_points(input_df, munis_input_crs)
    munis_intensity_input_crs = aggregate_intensity_by_municipality(points_input_crs, munis_input_crs)

    munis_plot = munis_input_crs.to_crs(TARGET_CRS)
    points_plot = points_input_crs.to_crs(TARGET_CRS)
    heat_plot = munis_intensity_input_crs.to_crs(TARGET_CRS)

    base_map = output_dir / f"{prefix}_base_pernambuco.png"
    marked_map = output_dir / f"{prefix}_marcacoes_heatmap_pernambuco.png"
    combined_map = output_dir / f"{prefix}_painel_duplo_pernambuco.png"

    title_base = "Pernambuco - Limites Municipais (Base Cartografica)"
    title_marked = "Pernambuco - Marcacoes Georreferenciadas e Intensidade"

    plot_base_map(munis_plot, title_base, base_map, dpi)
    plot_marked_heatmap(heat_plot, points_plot, title_marked, marked_map, dpi)
    plot_combined_panel(munis_plot, heat_plot, points_plot, title_base, title_marked, combined_map, dpi)

    return HeatmapOutputs(base_map=base_map, marked_map=marked_map, combined_map=combined_map)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gerador de mapas cientificos de Pernambuco com marcacoes georreferenciadas"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Arquivo CSV/XLSX com colunas: municipio (opcional), latitude (opcional), longitude (opcional), intensidade (opcional)",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Diretorio de saida dos PNGs")
    parser.add_argument("--prefix", default="mapa_cientifico", help="Prefixo dos arquivos de saida")
    parser.add_argument("--dpi", type=int, default=300, help="Resolucao de exportacao")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    outputs = generate_pernambuco_heatmaps(
        input_path=Path(args.input),
        output_dir=Path(args.output_dir),
        prefix=args.prefix,
        dpi=args.dpi,
    )
    print(f"Mapa base: {outputs.base_map}")
    print(f"Mapa marcado: {outputs.marked_map}")
    print(f"Painel duplo: {outputs.combined_map}")
