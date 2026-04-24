#!/usr/bin/env python3
"""Pipeline completa para analise espacial epidemiologica com GWR e mapas PNG.

Este modulo executa:
1) leitura de dados tabulares e geoespaciais de municipios
2) join por codigo IBGE
3) projecao para CRS adequado
4) modelagem GWR
5) exportacao de mapas tematicos cientificos em PNG (>=300 DPI)
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import mapclassify as mc
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import ListedColormap
from matplotlib.patches import FancyArrowPatch, Patch
from matplotlib_scalebar.scalebar import ScaleBar
from mgwr.gwr import GWR
from mgwr.sel_bw import Sel_BW

plt.rcParams["font.family"] = "DejaVu Sans"
plt.rcParams["axes.titleweight"] = "bold"

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = BASE_DIR / "static" / "maps"
DEFAULT_TARGET_CRS = "EPSG:31985"  # SIRGAS 2000 / UTM zone 25S

IBGE_DATA_CANDIDATES = [
    "codigo_ibge",
    "cod_ibge",
    "ibge",
    "code_muni",
    "cod_mun",
    "cd_mun",
    "geocodigo",
    "id_municipio",
]

IBGE_SHAPE_CANDIDATES = [
    "code_muni",
    "codigo_ibge",
    "cod_ibge",
    "cd_mun",
    "cod_mun",
    "geocodigo",
    "id",
]

MUNICIPALITY_NAME_CANDIDATES = [
    "municipio",
    "nome_municipio",
    "name_muni",
    "nm_mun",
    "nome",
]


@dataclass
class EpidemiologicalGWROutput:
    joined_data_path: Path | None
    map_paths: dict[str, Path]
    gwr_bandwidth: float
    records_used: int
    dependent_var: str
    independent_vars: list[str]


def _normalize_text(value: str) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"\s+", " ", text)
    return text.lower()


def _slug(value: str) -> str:
    text = _normalize_text(value)
    text = text.replace(" ", "_")
    return re.sub(r"[^a-z0-9_]+", "", text)


def _normalize_colname(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "", str(value).strip().lower().replace(" ", "_"))


def _normalize_ibge_code(raw: Any) -> str:
    digits = re.sub(r"\D+", "", str(raw or ""))
    if not digits:
        return ""
    if len(digits) >= 7:
        return digits[:6]
    if len(digits) == 6:
        return digits
    return digits.zfill(6)


def _coerce_numeric(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.replace("%", "", regex=False).str.replace(" ", "", regex=False)
    both_mask = cleaned.str.contains(".", regex=False) & cleaned.str.contains(",", regex=False)
    comma_mask = ~cleaned.str.contains(".", regex=False) & cleaned.str.contains(",", regex=False)

    cleaned.loc[both_mask] = cleaned.loc[both_mask].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    cleaned.loc[comma_mask] = cleaned.loc[comma_mask].str.replace(",", ".", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def _zscore_matrix(matrix: np.ndarray) -> np.ndarray:
    means = np.nanmean(matrix, axis=0)
    stds = np.nanstd(matrix, axis=0)
    stds[stds == 0] = 1.0
    standardized = (matrix - means) / stds
    return np.nan_to_num(standardized)


def _prune_independent_vars(df: pd.DataFrame, columns: list[str], corr_threshold: float = 0.999) -> list[str]:
    active = list(columns)
    if not active:
        return active

    # Remove variaveis sem variacao, pois elas tornam o ajuste local singular.
    active = [col for col in active if float(df[col].std(ddof=0)) > 1e-12]

    # Remove preditores praticamente duplicados por correlacao quase perfeita.
    while len(active) > 1:
        corr = df[active].corr().abs().to_numpy()
        np.fill_diagonal(corr, 0.0)
        max_corr = float(np.nanmax(corr)) if corr.size else 0.0
        if max_corr < corr_threshold:
            break

        i, j = np.unravel_index(np.nanargmax(corr), corr.shape)
        mean_corr_i = float(np.nanmean(corr[i]))
        mean_corr_j = float(np.nanmean(corr[j]))
        drop_idx = i if mean_corr_i >= mean_corr_j else j
        active.pop(drop_idx)

    return active


def _fit_gwr_with_fallback(
    coords: np.ndarray,
    y_std: np.ndarray,
    joined_df: gpd.GeoDataFrame,
    indep_cols: list[str],
) -> tuple[float, Any, list[str]]:
    active_cols = _prune_independent_vars(joined_df, indep_cols)
    if not active_cols:
        raise ValueError("Nao foi possivel ajustar GWR: variaveis independentes invalidas (sem variacao).")

    last_error: Exception | None = None
    random = np.random.default_rng(42)

    while active_cols:
        x_raw = joined_df[active_cols].to_numpy(dtype=float)
        x_std = _zscore_matrix(x_raw)

        for jitter in (0.0, 1e-8, 1e-7, 1e-6):
            try:
                x_try = x_std if jitter == 0.0 else (x_std + random.normal(0.0, jitter, size=x_std.shape))
                selector = Sel_BW(coords, y_std, x_try, spherical=False)
                bw = selector.search()
                gwr_result = GWR(coords, y_std, x_try, bw=bw, spherical=False).fit()
                return float(bw), gwr_result, active_cols
            except Exception as error:  # noqa: PERF203 - tentativas controladas por robustez numerica
                last_error = error

        if len(active_cols) == 1:
            break

        # Em ultima instancia, remove o preditor mais correlacionado para estabilizar o ajuste.
        corr = joined_df[active_cols].corr().abs().to_numpy()
        np.fill_diagonal(corr, 0.0)
        i, j = np.unravel_index(np.nanargmax(corr), corr.shape)
        mean_corr_i = float(np.nanmean(corr[i]))
        mean_corr_j = float(np.nanmean(corr[j]))
        drop_idx = i if mean_corr_i >= mean_corr_j else j
        active_cols.pop(drop_idx)

    raise ValueError(f"Falha ao ajustar GWR devido a singularidade numerica: {last_error}")


def _read_tabular_data(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        try:
            df = pd.read_csv(path, sep=None, engine="python")
        except Exception:
            # Fallback para arquivos de boletim com delimitador ';'
            df = pd.read_csv(path, sep=";", engine="python", encoding="latin1")

    if df.empty:
        raise ValueError("Arquivo tabular vazio.")

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _resolve_column_name(df: pd.DataFrame, requested: str) -> str:
    by_norm = {_normalize_colname(col): col for col in df.columns}
    key = _normalize_colname(requested)
    if key in by_norm:
        return by_norm[key]
    raise ValueError(f"Coluna nao encontrada no dataset: {requested}")


def _detect_ibge_column(df: pd.DataFrame, candidates: list[str], explicit: str | None = None) -> str | None:
    if explicit:
        return _resolve_column_name(df, explicit)

    by_norm = {_normalize_colname(col): col for col in df.columns}
    for candidate in candidates:
        if candidate in by_norm:
            return by_norm[candidate]
    return None


def _extract_ibge_from_name_column(df: pd.DataFrame) -> pd.Series | None:
    by_norm = {_normalize_colname(col): col for col in df.columns}
    for name_col in MUNICIPALITY_NAME_CANDIDATES:
        if name_col not in by_norm:
            continue
        col = by_norm[name_col]
        extracted = df[col].astype(str).str.extract(r"^(\d{6,7})")[0].fillna("")
        if (extracted != "").any():
            return extracted
    return None


def _build_classification(values: pd.Series, scheme: str, k: int) -> mc.classifiers.MapClassifier:
    clean = values.dropna().astype(float)
    if clean.empty:
        raise ValueError("Serie sem valores validos para classificacao.")

    unique_count = int(clean.nunique())
    classes = max(2, min(k, unique_count))

    if scheme == "natural_breaks":
        return mc.NaturalBreaks(clean, k=classes)
    return mc.Quantiles(clean, k=classes)


def _classifier_labels(classifier: mc.classifiers.MapClassifier, values: pd.Series) -> list[str]:
    lower = float(values.min())
    labels: list[str] = []
    for upper in classifier.bins:
        labels.append(f"{lower:.3f} - {float(upper):.3f}")
        lower = float(upper)
    return labels


def _add_cartographic_elements(ax: plt.Axes) -> None:
    ax.set_axis_off()
    ax.add_artist(
        ScaleBar(
            dx=1,
            units="m",
            location="lower center",
            box_alpha=0.9,
            scale_loc="top",
            color="#2f2f2f",
            length_fraction=0.18,
        )
    )

    north_arrow = FancyArrowPatch(
        (0.94, 0.79),
        (0.94, 0.89),
        transform=ax.transAxes,
        arrowstyle="-|>",
        mutation_scale=16,
        linewidth=1.2,
        color="#2f2f2f",
    )
    ax.add_patch(north_arrow)
    ax.text(0.94, 0.91, "N", transform=ax.transAxes, ha="center", va="bottom", fontsize=11, fontweight="bold")


def _set_standard_map_frame(ax: plt.Axes, gdf: gpd.GeoDataFrame) -> None:
    minx, miny, maxx, maxy = gdf.total_bounds
    x_span = maxx - minx
    y_span = maxy - miny

    x_pad = x_span * 0.04
    y_pad = y_span * 0.08

    ax.set_xlim(minx - x_pad, maxx + x_pad)
    ax.set_ylim(miny - y_pad, maxy + y_pad)
    ax.set_aspect("equal", adjustable="box")


def _plot_scientific_choropleth(
    gdf: gpd.GeoDataFrame,
    value_column: str,
    output_path: Path,
    title: str,
    legend_title: str,
    scheme: str,
    n_classes: int,
    dpi: int,
    cmap: str,
    source_note: str,
) -> None:
    series = gdf[value_column].astype(float)
    valid = series.dropna()
    actual_k = max(2, min(n_classes, int(valid.nunique())))

    mc_scheme = "NaturalBreaks" if scheme in ("natural_breaks", "NaturalBreaks") else "Quantiles"

    gdf_plot = gdf[[value_column, "geometry"]].copy()
    gdf_plot[value_column] = gdf_plot[value_column].astype(float)

    fig, ax = plt.subplots(1, 1, figsize=(12, 6), facecolor="white")

    try:
        gdf_plot.plot(
            column=value_column,
            ax=ax,
            cmap=cmap,
            scheme=mc_scheme,
            classification_kwds={"k": actual_k},
            legend=True,
            legend_kwds={
                "title": legend_title,
                "loc": "upper right",
                "frameon": True,
                "framealpha": 0.92,
                "fontsize": 9,
                "title_fontsize": 10,
            },
            edgecolor="#3a3a3a",
            linewidth=0.2,
            missing_kwds={"color": "#f0f0f0", "label": "Sem dados"},
        )
    except Exception:
        gdf_plot.plot(ax=ax, color="#9ecae1", edgecolor="#3a3a3a", linewidth=0.2)

    ax.set_title(title, fontsize=14, fontweight="bold", pad=10)
    _set_standard_map_frame(ax, gdf_plot)
    _add_cartographic_elements(ax)
    fig.text(0.02, 0.02, source_note, fontsize=8, color="#555555")
    fig.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def generate_epidemiological_gwr_maps(
    tabular_data_path: str | Path,
    municipalities_path: str | Path,
    dependent_var: str,
    independent_vars: list[str],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    data_ibge_column: str | None = None,
    shape_ibge_column: str | None = None,
    classification_scheme: str = "natural_breaks",
    n_classes: int = 5,
    target_crs: str = DEFAULT_TARGET_CRS,
    dpi: int = 300,
    title_prefix: str = "Pernambuco - Analise Espacial Epidemiologica",
    save_joined_geodata: bool = True,
) -> EpidemiologicalGWROutput:
    """Executa pipeline completa de GWR e gera mapas cientificos em PNG.

    Args:
        tabular_data_path: CSV/XLSX com codigo de municipio (IBGE), variavel dependente e independentes.
        municipalities_path: Shapefile/GeoJSON de municipios.
        dependent_var: Variavel alvo (ex: taxa_doenca).
        independent_vars: Variaveis explicativas (clima + demografia).
    """

    if not independent_vars:
        raise ValueError("Informe ao menos uma variavel independente para o GWR.")

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    tabular_path = Path(tabular_data_path)
    shape_path = Path(municipalities_path)

    table = _read_tabular_data(tabular_path)
    munis = gpd.read_file(shape_path)

    dep_col = _resolve_column_name(table, dependent_var)
    indep_cols = [_resolve_column_name(table, col) for col in independent_vars]

    table_ibge_col = _detect_ibge_column(table, IBGE_DATA_CANDIDATES, explicit=data_ibge_column)
    if table_ibge_col is None:
        extracted = _extract_ibge_from_name_column(table)
        if extracted is None:
            raise ValueError("Nao foi possivel identificar codigo IBGE no dataset tabular.")
        table = table.copy()
        table["_ibge_code_derived"] = extracted
        table_ibge_col = "_ibge_code_derived"

    shape_ibge_col_resolved = _detect_ibge_column(munis, IBGE_SHAPE_CANDIDATES, explicit=shape_ibge_column)
    if shape_ibge_col_resolved is None:
        raise ValueError("Nao foi possivel identificar codigo IBGE no arquivo geoespacial de municipios.")

    table = table.copy()
    munis = munis.copy()

    table["_ibge_code"] = table[table_ibge_col].map(_normalize_ibge_code)
    munis["_ibge_code"] = munis[shape_ibge_col_resolved].map(_normalize_ibge_code)

    if (table["_ibge_code"] == "").all():
        raise ValueError("Codigos IBGE invalidos no dataset tabular.")
    if (munis["_ibge_code"] == "").all():
        raise ValueError("Codigos IBGE invalidos no arquivo geoespacial.")

    keep_cols = ["_ibge_code", dep_col, *indep_cols]
    data_subset = table[keep_cols].copy()
    data_subset = data_subset.drop_duplicates(subset=["_ibge_code"], keep="last")

    data_subset[dep_col] = _coerce_numeric(data_subset[dep_col])
    for col in indep_cols:
        data_subset[col] = _coerce_numeric(data_subset[col])

    joined = munis.merge(data_subset, on="_ibge_code", how="left")

    required_cols = [dep_col, *indep_cols]
    joined = joined.dropna(subset=required_cols + ["geometry"]).copy()

    if joined.empty:
        raise ValueError("Nenhum municipio com dados completos apos o join espacial.")

    if joined.crs is None:
        joined = joined.set_crs("EPSG:4674")

    joined = joined.to_crs(target_crs)

    centroids = joined.geometry.centroid
    coords = np.column_stack([centroids.x.to_numpy(), centroids.y.to_numpy()])

    y_raw = joined[[dep_col]].to_numpy(dtype=float)

    y_std = _zscore_matrix(y_raw)
    bw, gwr_result, active_indep_cols = _fit_gwr_with_fallback(coords, y_std, joined, indep_cols)

    joined["gwr_intercept"] = gwr_result.params[:, 0]
    for idx, var in enumerate(active_indep_cols, start=1):
        joined[f"gwr_{_slug(var)}"] = gwr_result.params[:, idx]
    joined["gwr_local_r2"] = gwr_result.localR2
    joined["gwr_residual"] = gwr_result.resid_response.reshape(-1)

    map_paths: dict[str, Path] = {}

    disease_slug = _slug(dep_col)
    disease_map = output_root / f"distribuicao_{disease_slug}.png"
    _plot_scientific_choropleth(
        gdf=joined,
        value_column=dep_col,
        output_path=disease_map,
        title=f"{title_prefix} - Distribuicao da Doenca",
        legend_title=f"{dep_col} ({classification_scheme})",
        scheme=classification_scheme,
        n_classes=n_classes,
        dpi=dpi,
        cmap="Blues",
        source_note="Fonte: dataset tabular + malha municipal IBGE. Elaboracao: EpiGeoData",
    )
    map_paths["disease_distribution"] = disease_map

    for var in active_indep_cols:
        out = output_root / f"gwr_{_slug(var)}.png"
        _plot_scientific_choropleth(
            gdf=joined,
            value_column=f"gwr_{_slug(var)}",
            output_path=out,
            title=f"{title_prefix} - Coeficiente local GWR ({var})",
            legend_title=f"Coef. local {var} ({classification_scheme})",
            scheme=classification_scheme,
            n_classes=n_classes,
            dpi=dpi,
            cmap="RdBu_r",
            source_note="Modelo: GWR (mgwr). Fonte: IBGE + variaveis epidemiologicas e socioambientais.",
        )
        map_paths[f"gwr_coef_{_slug(var)}"] = out

    r2_map = output_root / "gwr_r2_local.png"
    _plot_scientific_choropleth(
        gdf=joined,
        value_column="gwr_local_r2",
        output_path=r2_map,
        title=f"{title_prefix} - R2 local do modelo GWR",
        legend_title=f"R2 local ({classification_scheme})",
        scheme=classification_scheme,
        n_classes=n_classes,
        dpi=dpi,
        cmap="YlGn",
        source_note="Modelo: GWR (mgwr). Interpretar valores maiores como melhor ajuste local.",
    )
    map_paths["gwr_local_r2"] = r2_map

    resid_map = output_root / "gwr_residuos.png"
    _plot_scientific_choropleth(
        gdf=joined,
        value_column="gwr_residual",
        output_path=resid_map,
        title=f"{title_prefix} - Residuos locais do modelo GWR",
        legend_title=f"Residuos ({classification_scheme})",
        scheme=classification_scheme,
        n_classes=n_classes,
        dpi=dpi,
        cmap="RdBu_r",
        source_note="Modelo: GWR (mgwr). Residuos altos indicam areas com menor ajuste local.",
    )
    map_paths["gwr_residuals"] = resid_map

    joined_data_path: Path | None = None
    if save_joined_geodata:
        joined_data_path = output_root / "gwr_resultados_municipios.geojson"
        joined.to_crs("EPSG:4674").to_file(joined_data_path, driver="GeoJSON")

    return EpidemiologicalGWROutput(
        joined_data_path=joined_data_path,
        map_paths=map_paths,
        gwr_bandwidth=float(bw),
        records_used=int(len(joined)),
        dependent_var=dep_col,
        independent_vars=active_indep_cols,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline GWR para mapas epidemiologicos de Pernambuco")
    parser.add_argument("--table", required=True, help="Arquivo tabular CSV/XLSX")
    parser.add_argument("--municipalities", required=True, help="Shapefile/GeoJSON dos municipios")
    parser.add_argument("--dependent", required=True, help="Variavel dependente")
    parser.add_argument("--independent", required=True, nargs="+", help="Variaveis independentes")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Diretorio de saida")
    parser.add_argument("--scheme", default="natural_breaks", choices=["natural_breaks", "quantiles"], help="Esquema de classificacao")
    parser.add_argument("--classes", default=5, type=int, help="Numero de classes")
    parser.add_argument("--dpi", default=300, type=int, help="Resolucao PNG")
    parser.add_argument("--target-crs", default=DEFAULT_TARGET_CRS, help="CRS alvo")
    parser.add_argument("--data-ibge-column", default=None, help="Nome da coluna IBGE no tabular")
    parser.add_argument("--shape-ibge-column", default=None, help="Nome da coluna IBGE no geoespacial")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    output = generate_epidemiological_gwr_maps(
        tabular_data_path=args.table,
        municipalities_path=args.municipalities,
        dependent_var=args.dependent,
        independent_vars=args.independent,
        output_dir=args.output_dir,
        data_ibge_column=args.data_ibge_column,
        shape_ibge_column=args.shape_ibge_column,
        classification_scheme=args.scheme,
        n_classes=args.classes,
        target_crs=args.target_crs,
        dpi=args.dpi,
    )

    print(f"Registros usados: {output.records_used}")
    print(f"Bandwidth GWR: {output.gwr_bandwidth:.4f}")
    if output.joined_data_path:
        print(f"GeoJSON de resultados: {output.joined_data_path}")
    for key, path in output.map_paths.items():
        print(f"{key}: {path}")


if __name__ == "__main__":
    main()
