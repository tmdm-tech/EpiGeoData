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
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import BoundaryNorm
from matplotlib.cm import ScalarMappable
from matplotlib.patches import FancyArrowPatch, Patch
from matplotlib.lines import Line2D
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
    if "code_muni" in municipalities.columns:
        municipalities["codigo_ibge"] = municipalities["code_muni"].map(_normalize_ibge7)
    else:
        municipalities["codigo_ibge"] = None
    return municipalities.to_crs(TARGET_CRS)


def _normalize_ibge7(value: object) -> str | None:
    text = re.sub(r"\D", "", str(value or ""))
    return text if len(text) == 7 else None


def load_municipality_totals(csv_path: Path) -> pd.DataFrame:
    """Read a local epidemiological export and preserve IBGE code when present."""
    last_error = None
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(csv_path, sep=";", skiprows=3, encoding=encoding)
            break
        except (UnicodeDecodeError, pd.errors.ParserError) as exc:
            last_error = exc
    else:
        raise ValueError(f"Nao foi possivel ler o CSV epidemiologico: {last_error}")

    if "Total" not in df.columns:
        raise ValueError("Coluna 'Total' nao encontrada no CSV de agravo.")

    municipality_col = df.columns[0]
    clean = df[[municipality_col, "Total"]].copy()
    raw = clean[municipality_col].astype(str).str.replace('"', "", regex=False)
    clean["codigo_ibge"] = raw.str.extract(r"^(\d{6,7})", expand=False).map(_normalize_ibge7)
    clean["municipio_nome"] = raw.str.replace(r"^\d+\s+", "", regex=True).str.title().str.strip()
    clean["total_casos"] = pd.to_numeric(
        clean["Total"].astype(str).str.replace("-", "0").str.replace(".", "", regex=False),
        errors="coerce",
    ).fillna(0)
    clean["join_name"] = clean["municipio_nome"].map(normalize_text)
    return clean.groupby(["codigo_ibge", "join_name"], dropna=False, as_index=False)["total_casos"].sum()


def add_cartographic_elements(ax: plt.Axes) -> None:
    """Norte + barra de escala leve, sem matplotlib-scalebar."""
    ax.set_axis_off()
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    width = xmax - xmin
    height = ymax - ymin
    scale_m = 100_000.0
    if width < 300_000:
        scale_m = 50_000.0
    x0 = xmin + width * 0.72
    y0 = ymin + height * 0.055
    ax.plot([x0, x0 + scale_m], [y0, y0], color="#222222", linewidth=2.0, zorder=20)
    ax.plot([x0, x0], [y0 - height * 0.008, y0 + height * 0.008], color="#222222", linewidth=1.2, zorder=20)
    ax.plot([x0 + scale_m, x0 + scale_m], [y0 - height * 0.008, y0 + height * 0.008], color="#222222", linewidth=1.2, zorder=20)
    ax.text(x0 + scale_m / 2, y0 + height * 0.018, f"{int(scale_m/1000)} km", ha="center", va="bottom", fontsize=9)
    north_arrow = FancyArrowPatch(
        (0.93, 0.80), (0.93, 0.90), transform=ax.transAxes,
        arrowstyle="-|>", mutation_scale=18, linewidth=1.2, color="#222222",
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
        # Prefer the stable seven-digit IBGE key. Name matching is retained only
        # for legacy exports that do not carry a valid territorial code.
        coded = data[data["codigo_ibge"].notna()][["codigo_ibge", "total_casos"]]
        municipalities_pe = municipalities_pe.drop(columns=["total_casos"])
        if not coded.empty and municipalities_pe["codigo_ibge"].notna().any():
            municipalities_pe = municipalities_pe.merge(coded, on="codigo_ibge", how="left")
        else:
            municipalities_pe = municipalities_pe.merge(data[["join_name", "total_casos"]], on="join_name", how="left")
        municipalities_pe["total_casos"] = pd.to_numeric(municipalities_pe["total_casos"], errors="coerce").fillna(0)

    values = pd.to_numeric(municipalities_pe["total_casos"], errors="coerce")
    has_classified_values = has_local_data and values.notna().any() and int(values.fillna(0).nunique()) > 1

    if output_filename:
        output_file = OUTPUT_DIR / output_filename
    else:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_DIR / f"mapa_profissional_{resolved_key}_{stamp}.png"

    # Layout cartografico de publicacao: Pernambuco centralizado, limites municipais, titulo, legenda externa, norte e escala.
    gdf = municipalities_pe.to_crs(TARGET_CRS)

    fig, ax = plt.subplots(figsize=(12, 6), facecolor=BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)
    set_standard_map_frame(ax, gdf)

    legend_handles: list[Patch] = []
    if has_classified_values:
        clean_values = values.dropna().astype(float)
        # Quantis calculados pelo pandas evitam a dependencia pesada mapclassify
        # no caminho de download e sao estaveis mesmo com poucos valores unicos.
        requested = min(DEFAULT_CLASSES, max(2, int(clean_values.nunique())))
        _, edges = pd.qcut(clean_values, q=requested, retbins=True, duplicates="drop")
        class_bins = [float(v) for v in edges]
        palette = PALETTE[: max(1, len(class_bins) - 1)]
        if len(class_bins) < 2:
            class_bins = [float(clean_values.min()), float(clean_values.max()) + 1.0]
            palette = PALETTE[:1]
        norm = BoundaryNorm(class_bins, ncolors=len(palette), clip=True)
        gdf["plot_color"] = [
            palette[min(norm(float(v)), len(palette)-1)] if pd.notna(v) else NO_DATA_COLOR
            for v in values
        ]
        gdf.plot(ax=ax, color=gdf["plot_color"], edgecolor="#666666", linewidth=0.45)
        for idx, color in enumerate(palette):
            legend_handles.append(Patch(
                facecolor=color, edgecolor="#333333", linewidth=0.6,
                label=f"{class_bins[idx]:.1f} – {class_bins[idx + 1]:.1f}",
            ))
    else:
        gdf.plot(ax=ax, color=NO_DATA_COLOR, edgecolor="#777777", linewidth=0.45)
        legend_handles.append(
            Patch(facecolor=NO_DATA_COLOR, edgecolor="#777777", label="Sem dados locais")
        )

    # Contorno estadual mais espesso, como no modelo cartografico de referencia.
    gdf.dissolve().boundary.plot(ax=ax, color="#111111", linewidth=1.8)
    add_cartographic_elements(ax)

    ax.set_title(resolved_title, fontsize=18, fontweight="bold", pad=18, color="#111111")
    legend = ax.legend(
        handles=legend_handles,
        title=variable_label,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.14),
        ncol=min(6, max(1, len(legend_handles))),
        frameon=False,
        fontsize=9.5,
        title_fontsize=10.5,
    )
    ax.set_axis_off()

    fig.subplots_adjust(left=0.025, right=0.985, top=0.88, bottom=0.20)
    source_label = "DATASUS / cartografia municipal IBGE" if has_local_data else "Cartografia municipal IBGE"
    fig.text(0.025, 0.025, f"Fonte: {source_label}. Elaboracao: EpiGeoData.", fontsize=8.5, color="#333333")
    try:
        fig.savefig(output_file, dpi=dpi, facecolor=BACKGROUND_COLOR, bbox_inches="tight")
    finally:
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
