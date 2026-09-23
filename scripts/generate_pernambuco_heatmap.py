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

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize
from matplotlib.patches import FancyArrowPatch
from shapely.geometry import Point

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = BASE_DIR / "static" / "maps"
TARGET_CRS = "EPSG:5880"  # SIRGAS 2000 / Brazil Polyconic (metros)
INPUT_CRS = "EPSG:4674"  # SIRGAS 2000 geographico (lat/lon)
CARTOGRAPHY_PATH = BASE_DIR / "data" / "municipios_pe_ibge.geojson"


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
    """Carrega a malha municipal versionada no repositorio, sem depender de rede."""
    if not CARTOGRAPHY_PATH.exists():
        raise FileNotFoundError(f"Cartografia municipal nao encontrada em {CARTOGRAPHY_PATH}")
    munis = gpd.read_file(CARTOGRAPHY_PATH).copy().to_crs(INPUT_CRS)
    if "name_muni" not in munis.columns:
        raise ValueError("Cartografia municipal sem a coluna obrigatoria 'name_muni'.")
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

    boundary_lookup = munis_gdf[["name_muni", "municipio_norm", "geometry"]].rename(
        columns={"municipio_norm": "municipio_norm_boundary"}
    )
    joined = gpd.sjoin(
        points,
        boundary_lookup,
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
        points_joined.dropna(subset=["municipio_norm_boundary"])
        .groupby("municipio_norm_boundary", as_index=False)["intensidade"]
        .sum()
        .rename(columns={"municipio_norm_boundary": "municipio_norm", "intensidade": "intensidade_total"})
    )

    result = munis_gdf.merge(agg, on="municipio_norm", how="left")
    result["intensidade_total"] = result["intensidade_total"].fillna(0.0)
    return result


def add_cartographic_elements(ax: plt.Axes) -> None:
    ax.set_axis_off()
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    width, height = xmax - xmin, ymax - ymin
    scale_m = 100_000.0 if width >= 300_000 else 50_000.0
    x0, y0 = xmin + width * 0.70, ymin + height * 0.055
    ax.plot([x0, x0 + scale_m], [y0, y0], color="#2d2d2d", linewidth=2, zorder=20)
    ax.plot([x0, x0], [y0-height*.008, y0+height*.008], color="#2d2d2d", linewidth=1, zorder=20)
    ax.plot([x0+scale_m, x0+scale_m], [y0-height*.008, y0+height*.008], color="#2d2d2d", linewidth=1, zorder=20)
    ax.text(x0 + scale_m/2, y0 + height*.018, f"{int(scale_m/1000)} km", ha="center", fontsize=8)
    north_arrow = FancyArrowPatch((0.94, 0.80), (0.94, 0.90), transform=ax.transAxes,
                                  arrowstyle="-|>", mutation_scale=16, linewidth=1.2, color="#2d2d2d")
    ax.add_patch(north_arrow)
    ax.text(0.94, 0.92, "N", transform=ax.transAxes, ha="center", va="bottom", fontsize=10, fontweight="bold")

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
    try:
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    finally:
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
    try:
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    finally:
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
    """Renderiza o painel final diretamente, sem manter dois PNGs 300 dpi em memoria."""
    cmap = LinearSegmentedColormap.from_list(
        "climate_intensity", ["#2c7bb6", "#5ab4ac", "#fee08b", "#fdae61", "#d7191c"]
    )
    # 16x8 a 300 dpi = 4800x2400 (~46 MB RGBA), muito menor que o antigo
    # painel 10x18 (~65 MB) e sem colorbar pesado criado pelo GeoPandas.
    fig, axes = plt.subplots(2, 1, figsize=(8, 10), facecolor="white")
    try:
        base_municipalities.plot(ax=axes[0], color="#f8f8f8", edgecolor="#8e8e8e", linewidth=0.30)
        axes[0].set_title(title_base, fontsize=12, fontweight="bold", pad=6)
        add_cartographic_elements(axes[0])

        values = heat_municipalities["intensidade_total"].astype(float)
        vmin, vmax = float(values.min()), float(values.max())
        if vmax <= vmin:
            vmax = vmin + 1.0
        heat_municipalities.plot(
            ax=axes[1], column="intensidade_total", cmap=cmap,
            vmin=vmin, vmax=vmax, linewidth=0.25, edgecolor="#6e6e6e",
        )
        points.plot(ax=axes[1], color="#111111", markersize=16, marker="o",
                    alpha=0.80, edgecolor="white", linewidth=0.35, zorder=5)
        axes[1].set_title(title_marked, fontsize=12, fontweight="bold", pad=6)
        add_cartographic_elements(axes[1])
        sm = ScalarMappable(norm=Normalize(vmin=vmin, vmax=vmax), cmap=cmap)
        sm.set_array([])
        fig.colorbar(sm, ax=axes[1], fraction=0.025, pad=0.02, label="Intensidade agregada por municipio")
        fig.text(0.01, 0.008, "Fonte: IBGE + entrada georreferenciada do usuario", fontsize=8, color="#555555")
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    finally:
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

    # Renderizacao sequencial e fechamento explicito de cada figura. O painel
    # combinado e o produto principal; mapas individuais usam 180 dpi para
    # preview, evitando estourar a memoria do worker gratuito do Render.
    preview_dpi = min(dpi, 180)
    plot_base_map(munis_plot, title_base, base_map, preview_dpi)
    plot_marked_heatmap(heat_plot, points_plot, title_marked, marked_map, preview_dpi)
    plot_combined_panel(munis_plot, heat_plot, points_plot, title_base, title_marked, combined_map, dpi)
    plt.close("all")

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
