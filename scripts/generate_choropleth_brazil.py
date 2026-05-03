#!/usr/bin/env python3
"""Gera mapa coropletico profissional de Pernambuco por municipios."""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import mapclassify as mc
import matplotlib.pyplot as plt
import pandas as pd
import contextily as ctx
from matplotlib.colors import ListedColormap
from matplotlib.patches import FancyArrowPatch, Patch
from matplotlib.lines import Line2D
from matplotlib_scalebar.scalebar import ScaleBar
from shapely.geometry import box

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "static" / "maps"
CARTOGRAPHY_PATH = BASE_DIR / "data" / "municipios_pe_ibge.geojson"
TARGET_CRS = "EPSG:31985"  # SIRGAS 2000 / UTM zone 25S
DEFAULT_DPI = 300
DEFAULT_SCHEME = "natural_breaks"
DEFAULT_CLASSES = 5

DISEASE_METADATA = {
    "scz": {
        "display_name": "Sindrome Congenita da Zika",
        "aliases": ["scz", "sindrome_congenita_da_zika", "sindrome_congenita_zika", "zika"],
    },
    "covid_19": {
        "display_name": "Covid 19",
        "aliases": ["covid_19", "covid19", "covid"],
    },
    "dengue": {
        "display_name": "Dengue",
        "aliases": ["dengue"],
    },
    "esquistossomose": {
        "display_name": "Esquistossomose",
        "aliases": ["esquistossomose", "esquisto"],
    },
    "tuberculose": {
        "display_name": "Tuberculose",
        "aliases": ["tuberculose", "tuberc"],
    },
    "monkeypox": {
        "display_name": "Monkeypox",
        "aliases": ["monkeypox", "mpox"],
    },
    "chikungunya": {
        "display_name": "Chikungunya",
        "aliases": ["chikungunya", "chikun"],
    },
    "oropouche": {
        "display_name": "Febre Oropouche",
        "aliases": ["oropouche", "febre_oropouche"],
    },
}

DISEASE_FILE_ALIASES = {
    key: meta["aliases"]
    for key, meta in DISEASE_METADATA.items()
}

PALETTE = ["#d9d7ef", "#b1addb", "#8e89c9", "#644ab1", "#4b0f94"]
NO_DATA_COLOR = "#f1f1f1"
BACKGROUND_COLOR = "#ffffff"
FRAME_COLOR = "#222222"


@dataclass
class ChoroplethResult:
    output_file: Path
    disease_key: str
    source_csv: Path | None
    variable_label: str
    has_local_data: bool


def normalize_text(text: str) -> str:
    no_accents = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\s+", " ", no_accents).strip().upper()


def normalize_token(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"\s+", "_", value)
    return re.sub(r"[^a-z0-9_]+", "", value)


def resolve_disease_key(disease_key: str) -> str:
    token = normalize_token(disease_key)
    if token in DISEASE_METADATA:
        return token
    for key, aliases in DISEASE_FILE_ALIASES.items():
        if token in aliases:
            return key
    return token


def resolve_disease_csv(disease_key: str) -> tuple[str, Path | None]:
    key = resolve_disease_key(disease_key)
    aliases = [normalize_token(alias) for alias in DISEASE_FILE_ALIASES.get(key, [key])]
    candidates = [BASE_DIR / "data" / "doencas", BASE_DIR / "data", BASE_DIR]

    # Busca direta por nomes esperados evita recursao profunda em ambientes de deploy.
    for base in candidates:
        if not base.exists() or not base.is_dir():
            continue
        for alias in aliases:
            candidate = base / f"{alias}.csv"
            if candidate.exists() and candidate.is_file():
                return key, candidate

    for base in candidates:
        if not base.exists() or not base.is_dir():
            continue
        for path in base.glob("*.csv"):
            if normalize_token(path.stem) in aliases:
                return key, path

    return key, None


def load_pernambuco_municipalities() -> gpd.GeoDataFrame:
    if not CARTOGRAPHY_PATH.exists():
        raise FileNotFoundError(f"Cartografia municipal nao encontrada em {CARTOGRAPHY_PATH}")

    municipalities = gpd.read_file(CARTOGRAPHY_PATH)
    municipalities = municipalities.copy()
    municipalities["join_name"] = municipalities["name_muni"].map(normalize_text)
    return municipalities.to_crs(TARGET_CRS)


def load_municipality_totals(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=";", skiprows=3, encoding="latin1")
    if "Total" not in df.columns:
        raise ValueError("Coluna 'Total' nao encontrada no CSV de agravo.")

    municipality_col = df.columns[0]
    clean = df[[municipality_col, "Total"]].copy()
    clean[municipality_col] = clean[municipality_col].astype(str).str.replace('"', "", regex=False)
    clean["municipio_nome"] = (
        clean[municipality_col].str.replace(r"^\d+\s+", "", regex=True).str.title().str.strip()
    )
    clean["total_casos"] = pd.to_numeric(
        clean["Total"].astype(str).str.replace("-", "0").str.replace(".", "", regex=False),
        errors="coerce",
    ).fillna(0)
    clean["join_name"] = clean["municipio_nome"].map(normalize_text)
    return clean.groupby("join_name", as_index=False)["total_casos"].sum()


def build_classification(values: pd.Series, scheme: str, n_classes: int) -> mc.classifiers.MapClassifier:
    clean = values.dropna().astype(float)
    unique_count = int(clean.nunique())
    actual_k = max(2, min(n_classes, unique_count))
    if scheme == "quantiles":
        return mc.Quantiles(clean, k=actual_k)
    return mc.NaturalBreaks(clean, k=actual_k)


def classifier_labels(classifier: mc.classifiers.MapClassifier, values: pd.Series) -> list[str]:
    lower = float(values.min())
    labels: list[str] = []
    for upper in classifier.bins:
        labels.append(f"{lower:,.0f} - {float(upper):,.0f}")
        lower = float(upper)
    return labels


def add_cartographic_elements(ax: plt.Axes) -> None:
    ax.set_axis_off()
    ax.add_artist(
        ScaleBar(
            dx=1,
            units="m",
            location="lower center",
            box_alpha=0.92,
            scale_loc="top",
            color="#2f2f2f",
            length_fraction=0.18,
        )
    )

    north_arrow = FancyArrowPatch(
        (0.93, 0.80),
        (0.93, 0.90),
        transform=ax.transAxes,
        arrowstyle="-|>",
        mutation_scale=16,
        linewidth=1.2,
        color="#2f2f2f",
    )
    ax.add_patch(north_arrow)
    ax.text(0.93, 0.92, "N", transform=ax.transAxes, ha="center", va="bottom", fontsize=11, fontweight="bold")


def set_standard_map_frame(ax: plt.Axes, gdf: gpd.GeoDataFrame) -> None:
    minx, miny, maxx, maxy = gdf.total_bounds
    x_span = maxx - minx
    y_span = maxy - miny
    ax.set_xlim(minx - x_span * 0.04, maxx + x_span * 0.04)
    ax.set_ylim(miny - y_span * 0.08, maxy + y_span * 0.08)
    ax.set_aspect("equal", adjustable="box")


def render_side_panel(ax: plt.Axes, title: str, subtitle: str, source_note: str, handles: list[Patch]) -> None:
    ax.set_facecolor("#10182a")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.text(0.0, 0.98, title, fontsize=12, fontweight="bold", color="#eef4ff", va="top")
    ax.text(0.0, 0.90, subtitle, fontsize=9.2, color="#c9d9ff", va="top", wrap=True)
    ax.text(0.0, 0.80, "Legenda", fontsize=10.5, fontweight="bold", color="#eef4ff")

    if handles:
        legend = ax.legend(
            handles=handles,
            loc="upper left",
            bbox_to_anchor=(0.0, 0.77),
            frameon=False,
            labelspacing=0.85,
            handlelength=1.5,
            handleheight=1.1,
            borderaxespad=0.0,
            fontsize=9,
        )
        for txt in legend.get_texts():
            txt.set_color("#d6e4ff")

    ax.text(0.0, 0.25, "Fonte e status", fontsize=10.5, fontweight="bold", color="#eef4ff")
    ax.text(0.0, 0.21, source_note, fontsize=8.8, color="#c9d9ff", va="top", wrap=True)


def generate_professional_choropleth(
    disease_key: str,
    title: str | None = None,
    output_filename: str | None = None,
    dpi: int = DEFAULT_DPI,
) -> ChoroplethResult:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    resolved_key, csv_path = resolve_disease_csv(disease_key)
    municipalities_pe = load_pernambuco_municipalities()
    variable_label = "Casos totais"
    display_name = DISEASE_METADATA.get(resolved_key, {}).get("display_name", resolved_key.replace("_", " ").title())
    resolved_title = title or f"Pernambuco | {display_name} por municipio"

    municipalities_pe["total_casos"] = pd.NA
    has_local_data = csv_path is not None
    if csv_path is not None:
        data = load_municipality_totals(csv_path)
        municipalities_pe = municipalities_pe.merge(data, on="join_name", how="left")
        municipalities_pe["total_casos"] = municipalities_pe["total_casos_y"].fillna(0)
        municipalities_pe = municipalities_pe.drop(columns=[col for col in ["total_casos_x", "total_casos_y"] if col in municipalities_pe.columns])

    values = pd.to_numeric(municipalities_pe["total_casos"], errors="coerce")
    has_classified_values = has_local_data and values.notna().any() and int(values.fillna(0).nunique()) > 1

    if output_filename:
        output_file = OUTPUT_DIR / output_filename
    else:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_DIR / f"mapa_profissional_{resolved_key}_{stamp}.png"

    # O padrão visual segue o layout de referência: basemap claro + coroplético em roxo + legenda compacta.
    gdf = municipalities_pe.to_crs("EPSG:3857")

    fig, ax = plt.subplots(figsize=(12, 9), facecolor=BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)

    # Janela fixa no Nordeste para reproduzir composição visual do mapa de referência.
    bbox_mercator = gpd.GeoSeries([box(-43.1, -11.7, -33.6, -1.7)], crs="EPSG:4326").to_crs("EPSG:3857")
    minx, miny, maxx, maxy = bbox_mercator.total_bounds
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    try:
        ctx.add_basemap(
            ax,
            source=ctx.providers.CartoDB.Positron,
            attribution_size=10,
        )
    except Exception:
        # Fallback resiliente para ambientes sem acesso/compatibilidade com tiles remotos.
        pass

    legend_handles: list[Line2D] = []
    if has_classified_values:
        classifier = build_classification(values, DEFAULT_SCHEME, DEFAULT_CLASSES)
        class_bins = [float(values.min())] + [float(v) for v in classifier.bins]
        class_ids = pd.Series(pd.NA, index=gdf.index, dtype="object")
        class_ids.loc[values.dropna().index] = classifier.yb.astype(int)
        palette = PALETTE[: len(class_bins) - 1]
        gdf["plot_color"] = class_ids.map(lambda idx: palette[int(idx)] if pd.notna(idx) else NO_DATA_COLOR)

        gdf.plot(
            ax=ax,
            color=gdf["plot_color"],
            edgecolor=FRAME_COLOR,
            linewidth=1.1,
            alpha=0.96,
        )

        for idx, color in enumerate(palette):
            low = class_bins[idx]
            high = class_bins[idx + 1]
            label = f"{low:.2f},  {high:.2f}"
            legend_handles.append(
                Line2D([0], [0], marker="o", color="none", markerfacecolor=color, markeredgecolor=color, markersize=10, label=label)
            )
    else:
        gdf.plot(ax=ax, color=NO_DATA_COLOR, edgecolor=FRAME_COLOR, linewidth=1.1, alpha=0.96)
        legend_handles.append(
            Line2D([0], [0], marker="o", color="none", markerfacecolor=NO_DATA_COLOR, markeredgecolor="#8a8a8a", markersize=10, label="Sem dados")
        )

    legend = ax.legend(
        handles=legend_handles,
        loc="upper right",
        frameon=True,
        framealpha=0.95,
        facecolor="white",
        edgecolor="#c8c8c8",
        fontsize=12,
    )
    for text in legend.get_texts():
        text.set_color("#1f1f1f")

    ax.set_title(display_name, fontsize=24, fontweight="normal", pad=10, color="#202020")
    ax.set_axis_off()

    fig.subplots_adjust(left=0.03, right=0.98, top=0.90, bottom=0.14)
    fig.text(0.12, 0.075, "Fonte: DATASUS", fontsize=15, color="#222222")
    fig.text(0.12, 0.043, "Elaboracao propria", fontsize=15, color="#222222")
    fig.savefig(output_file, dpi=dpi, facecolor=BACKGROUND_COLOR)
    plt.close(fig)

    return ChoroplethResult(
        output_file=output_file,
        disease_key=resolved_key,
        source_csv=csv_path,
        variable_label=variable_label,
        has_local_data=has_local_data,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gerador de mapa coropletico profissional")
    parser.add_argument("--disease", default="tuberculose", help="Chave do agravo (ex.: tuberculose)")
    parser.add_argument("--title", default=None, help="Titulo personalizado do mapa")
    parser.add_argument("--output", default=None, help="Nome do arquivo PNG de saida")
    parser.add_argument("--dpi", type=int, default=DEFAULT_DPI, help="Resolucao de exportacao")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    result = generate_professional_choropleth(
        disease_key=args.disease,
        title=args.title,
        output_filename=args.output,
        dpi=args.dpi,
    )
    print(f"Mapa exportado em: {result.output_file}")
