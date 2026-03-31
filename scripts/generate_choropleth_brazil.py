#!/usr/bin/env python3
"""Gera mapa coropletico profissional do Brasil (foco PE por municipios)."""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import geobr
import mapclassify as mc
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
from matplotlib_scalebar.scalebar import ScaleBar

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "static" / "maps"
TARGET_CRS = "EPSG:5880"  # SIRGAS 2000 / Brazil Polyconic (metros)
DEFAULT_DPI = 300

DISEASE_FILE_ALIASES = {
    "dengue": ["dengue"],
    "esquistossomose": ["esquistossomose"],
    "tuberculose": ["tuberculose"],
    "chikungunya": ["chikungunya"],
    "scz": ["scz", "sindrome_congenita_da_zika", "zika"],
}

REGION_HIGHLIGHTS = [
    ("Norte", "Norte"),
    ("Nordeste", "Nordeste"),
    ("Centro-Oeste", "Centro Oeste"),
    ("Sudeste", "Sudeste"),
    ("Sul", "Sul"),
]


@dataclass
class ChoroplethResult:
    output_file: Path
    disease_key: str
    source_csv: Path
    variable_label: str


def normalize_text(text: str) -> str:
    no_accents = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\s+", " ", no_accents).strip().upper()


def normalize_token(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"\s+", "_", value)
    return re.sub(r"[^a-z0-9_]+", "", value)


def resolve_disease_csv(disease_key: str) -> tuple[str, Path]:
    key = normalize_token(disease_key)
    aliases = DISEASE_FILE_ALIASES.get(key, [key])
    candidates = [BASE_DIR / "data" / "doencas", BASE_DIR / "data", BASE_DIR]

    for base in candidates:
        if not base.exists() or not base.is_dir():
            continue
        for path in base.rglob("*.csv"):
            if normalize_token(path.stem) in aliases:
                return key, path
    raise FileNotFoundError(f"Nenhum CSV encontrado para o agravo: {disease_key}")


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


def _legend_labels(classifier: mc.classifiers.MapClassifier, values: pd.Series) -> list[str]:
    labels: list[str] = []
    lower = float(values.min()) if len(values) else 0.0
    for upper in classifier.bins:
        labels.append(f"{int(round(lower))} - {int(round(float(upper)))}")
        lower = float(upper)
    return labels


def generate_professional_choropleth(
    disease_key: str,
    title: str | None = None,
    output_filename: str | None = None,
    dpi: int = DEFAULT_DPI,
) -> ChoroplethResult:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    resolved_key, csv_path = resolve_disease_csv(disease_key)

    states = geobr.read_state(year=2020, simplified=True)
    municipalities_pe = geobr.read_municipality(code_muni="PE", year=2020, simplified=True)
    data = load_municipality_totals(csv_path)

    municipalities_pe["join_name"] = municipalities_pe["name_muni"].map(normalize_text)
    municipalities_pe = municipalities_pe.merge(data, on="join_name", how="left")
    municipalities_pe["total_casos"] = municipalities_pe["total_casos"].fillna(0)

    states = states.to_crs(TARGET_CRS)
    municipalities_pe = municipalities_pe.to_crs(TARGET_CRS)

    values = municipalities_pe["total_casos"].astype(float)
    unique_count = int(values.nunique())
    class_count = max(2, min(5, unique_count if unique_count > 1 else 2))
    classifier = mc.Quantiles(values, k=class_count)
    municipalities_pe["classe"] = classifier.yb

    colors = ["#deebf7", "#c6dbef", "#9ecae1", "#6baed6", "#2171b5"][:class_count]
    cmap = ListedColormap(colors)

    variable_label = "Casos totais"
    resolved_title = title or f"Brasil (PE): {variable_label} de {resolved_key.title()} por Municipio"
    footer = (
        "Fonte: IBGE (Malhas Territoriais 2020, SIRGAS 2000) | "
        f"CSV local ({csv_path.name}) | Elaboracao: EpiGeoData"
    )

    if output_filename:
        output_file = OUTPUT_DIR / output_filename
    else:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_DIR / f"mapa_profissional_{resolved_key}_{stamp}.png"

    fig = plt.figure(figsize=(16, 11), facecolor="white")
    gs = fig.add_gridspec(nrows=5, ncols=2, width_ratios=[4.8, 1.6], wspace=0.04, hspace=0.10)

    ax_main = fig.add_subplot(gs[:, 0])
    states.plot(ax=ax_main, color="#f5f5f5", edgecolor="#b8b8b8", linewidth=0.35, zorder=1)
    municipalities_pe.plot(
        ax=ax_main,
        column="classe",
        cmap=cmap,
        edgecolor="#4f4f4f",
        linewidth=0.18,
        zorder=3,
    )
    ax_main.set_title(resolved_title, fontsize=18, fontweight="bold", pad=10)
    ax_main.set_axis_off()

    labels = _legend_labels(classifier, values)
    legend_handles = [
        Patch(facecolor=colors[i], edgecolor="#666666", linewidth=0.3, label=labels[i])
        for i in range(class_count)
    ]
    legend = ax_main.legend(
        handles=legend_handles,
        title=f"{variable_label} (quantis)",
        loc="lower left",
        bbox_to_anchor=(0.02, 0.02),
        frameon=True,
        framealpha=0.95,
        facecolor="white",
        edgecolor="#b0b0b0",
        fontsize=9,
        title_fontsize=10,
    )
    legend._legend_box.align = "left"

    ax_main.add_artist(
        ScaleBar(
            dx=1,
            units="m",
            location="lower right",
            box_alpha=0.85,
            scale_loc="top",
            color="#333333",
            length_fraction=0.20,
        )
    )

    ax_main.annotate(
        "N",
        xy=(0.95, 0.90),
        xytext=(0.95, 0.82),
        xycoords="axes fraction",
        textcoords="axes fraction",
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold",
        color="#333333",
        arrowprops=dict(arrowstyle="-|>", color="#333333", lw=1.2, shrinkA=0, shrinkB=0),
        zorder=10,
    )

    for idx, (label, source_name) in enumerate(REGION_HIGHLIGHTS):
        ax_reg = fig.add_subplot(gs[idx, 1])
        states.plot(ax=ax_reg, color="#efefef", edgecolor="#aaaaaa", linewidth=0.25)
        states[states["name_region"] == source_name].plot(
            ax=ax_reg,
            color="#2c7fb8",
            edgecolor="#1f4f73",
            linewidth=0.35,
        )
        ax_reg.set_title(label, fontsize=10, pad=2)
        ax_reg.set_axis_off()

    fig.text(0.01, 0.01, footer, ha="left", va="bottom", fontsize=9, color="#444444")
    fig.savefig(output_file, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    return ChoroplethResult(
        output_file=output_file,
        disease_key=resolved_key,
        source_csv=csv_path,
        variable_label=variable_label,
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
