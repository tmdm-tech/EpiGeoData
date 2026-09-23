#!/usr/bin/env python3
"""Gera mapa coropletico profissional de Pernambuco por municipios."""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import BoundaryNorm
from matplotlib.cm import ScalarMappable
from matplotlib.patches import FancyArrowPatch, Patch
from matplotlib.lines import Line2D
from matplotlib.colors import ListedColormap
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


@lru_cache(maxsize=1)
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


def load_municipality_totals(csv_path: Path, selected_years: list[int] | None = None) -> pd.DataFrame:
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
    year_columns = [str(y) for y in (selected_years or []) if str(y) in df.columns]
    value_columns = year_columns if year_columns else ["Total"]
    clean = df[[municipality_col, *value_columns]].copy()
    raw = clean[municipality_col].astype(str).str.replace('"', "", regex=False)
    clean["codigo_ibge"] = raw.str.extract(r"^(\d{6,7})", expand=False).map(_normalize_ibge7)
    clean["municipio_nome"] = raw.str.replace(r"^\d+\s+", "", regex=True).str.title().str.strip()
    numeric = clean[value_columns].apply(
        lambda col: pd.to_numeric(col.astype(str).str.replace("-", "0").str.replace(".", "", regex=False), errors="coerce").fillna(0)
    )
    clean["total_casos"] = numeric.sum(axis=1)
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
    analysis_mode: str = "choropleth",
    selected_years: list[int] | None = None,
    selected_climates: list[str] | None = None,
    socio_variable: str = "",
    socio_scope: str = "",
    geres: str = "ALL",
    municipio_id: str = "",
) -> ChoroplethResult:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    resolved_key, csv_path = resolve_disease_csv(disease_key)
    municipalities_pe = load_pernambuco_municipalities().copy()
    variable_label = "Casos totais"
    display_name = DISEASE_METADATA.get(resolved_key, {}).get("display_name", resolved_key.replace("_", " ").title())
    resolved_title = title or f"Pernambuco | {display_name} por municipio"

    municipalities_pe["total_casos"] = pd.NA
    has_local_data = csv_path is not None
    if csv_path is not None:
        data = load_municipality_totals(csv_path, selected_years=selected_years)
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

    # Layout de publicacao: enquadramento pelo continente. Fernando de Noronha nao
    # pode ampliar artificialmente a extensao do mapa principal.
    gdf = municipalities_pe.to_crs(TARGET_CRS).copy()
    sys.setrecursionlimit(max(sys.getrecursionlimit(), 10000))
    gdf["geometry"] = gdf.geometry.simplify(25.0, preserve_topology=True)
    mainland = gdf[~gdf["join_name"].str.contains("FERNANDO DE NORONHA", na=False)].copy()

    # Territorial filter must alter the analytical geography, not only the title.
    # GERES composition follows the same official SES-PE grouping exposed by the frontend.
    GERES_MUNICIPALITIES = {
        "I GERES": ["ABREU E LIMA","ARACOIABA","CABO DE SANTO AGOSTINHO","CAMARAGIBE","CHA DE ALEGRIA","CHA GRANDE","GLORIA DO GOITA","IGARASSU","ILHA DE ITAMARACA","IPOJUCA","ITAPISSUMA","JABOATAO DOS GUARARAPES","MORENO","OLINDA","PAULISTA","POMBOS","RECIFE","SAO LOURENCO DA MATA","VITORIA DE SANTO ANTAO"],
        "II GERES": ["BOM JARDIM","BUENOS AIRES","CARPINA","CASINHAS","CUMARU","FEIRA NOVA","JOAO ALFREDO","LAGOA DE ITAENGA","LAGOA DO CARRO","LIMOEIRO","MACHADOS","NAZARE DA MATA","OROBO","PASSIRA","PAUDALHO","SALGADINHO","SURUBIM","TRACUNHAEM","VERTENTE DO LERIO","VICENCIA"],
        "III GERES": ["AGUA PRETA","AMARAJI","BARREIROS","BELEM DE MARIA","CATENDE","CORTES","ESCADA","GAMELEIRA","JAQUEIRA","JOAQUIM NABUCO","LAGOA DOS GATOS","MARAIAL","PALMARES","PRIMAVERA","QUIPAPA","RIBEIRAO","RIO FORMOSO","SAO BENEDITO DO SUL","SAO JOSE DA COROA GRANDE","SIRINHAEM","TAMANDARE","XEXEU"],
        "IV GERES": ["AGRESTINA","ALAGOINHA","ALTINHO","BARRA DE GUABIRABA","BELO JARDIM","BEZERROS","BONITO","BREJO DA MADRE DE DEUS","CACHOEIRINHA","CAMOCIM DE SAO FELIX","CARUARU","CUPIRA","FREI MIGUELINHO","GRAVATA","IBIRAJUBA","JATAUBA","JUREMA","PANELAS","PESQUEIRA","POCAO","RIACHO DAS ALMAS","SAIRE","SANHARO","SANTA CRUZ DO CAPIBARIBE","SANTA MARIA DO CAMBUCA","SAO BENTO DO UNA","SAO CAETANO","SAO JOAQUIM DO MONTE","TACAIMBO","TAQUARITINGA DO NORTE","TORITAMA","VERTENTES"],
        "V GERES": ["AGUAS BELAS","ANGELIM","BOM CONSELHO","BREJAO","CAETES","CALCADO","CANHOTINHO","CAPOEIRAS","CORRENTES","GARANHUNS","IATI","ITAIBA","JUCATI","JUPI","LAGOA DO OURO","LAJEDO","PALMEIRINA","PARANATAMA","SALOA","SAO JOAO","TEREZINHA"],
        "VI GERES": ["ARCOVERDE","BUIQUE","CUSTODIA","IBIMIRIM","INAJA","JATOBA","MANARI","PEDRA","PETROLANDIA","SERTANIA","TACARATU","TUPANATINGA","VENTUROSA"],
        "VII GERES": ["BELEM DO SAO FRANCISCO","CEDRO","MIRANDIBA","SALGUEIRO","SERRITA","TERRA NOVA","VERDEJANTE"],
        "VIII GERES": ["AFRANIO","CABROBO","DORMENTES","LAGOA GRANDE","OROCO","PETROLINA","SANTA MARIA DA BOA VISTA"],
        "IX GERES": ["ARARIPINA","BODOCO","EXU","GRANITO","IPUBI","MOREILANDIA","OURICURI","PARNAMIRIM","SANTA CRUZ","SANTA FILOMENA","TRINDADE"],
        "X GERES": ["AFOGADOS DA INGAZEIRA","BREJINHO","CARNAIBA","IGUARACY","INGAZEIRA","ITAPETIM","QUIXABA","SANTA TEREZINHA","SAO JOSE DO EGITO","SOLIDAO","TABIRA","TUPARETAMA"],
        "XI GERES": ["BETANIA","CALUMBI","CARNAUBEIRA DA PENHA","FLORES","FLORESTA","ITACURUBA","SANTA CRUZ DA BAIXA VERDE","SAO JOSE DO BELMONTE","SERRA TALHADA","TRIUNFO"],
        "XII GERES": ["ALIANCA","CAMUTANGA","CONDADO","FERREIROS","GOIANA","ITAMBE","ITAQUITINGA","MACAPARANA","SAO VICENTE FERRER","TIMBAUBA"],
    }
    geres_key = str(geres or "ALL").strip().upper()
    if geres_key not in ("", "ALL", "TODAS AS GERES"):
        allowed = set(GERES_MUNICIPALITIES.get(geres_key, []))
        if not allowed:
            raise ValueError(f"GERES desconhecida: {geres}")
        mainland = mainland[mainland["join_name"].isin(allowed)].copy()
        if mainland.empty:
            raise ValueError(f"GERES sem municípios compatíveis na malha IBGE: {geres}")
        resolved_title = f"{display_name} – {geres_key}, Pernambuco"
    if municipio_id:
        code = str(municipio_id).replace(".0","").strip()
        municipal = mainland[mainland["codigo_ibge"].astype(str).str.replace(".0","",regex=False) == code].copy()
        if municipal.empty:
            raise ValueError(f"Município IBGE não pertence ao recorte selecionado: {code}")
        mainland = municipal
        resolved_title = f"{display_name} – {mainland.iloc[0].get('name_muni', code)}, Pernambuco"

    fig, ax = plt.subplots(figsize=(12.8, 6.0), facecolor=BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)
    legend_handles: list[Patch] = []
    mode = normalize_token(analysis_mode)
    period = ""
    if selected_years:
        ys=sorted(set(int(y) for y in selected_years))
        period = str(ys[0]) if len(ys)==1 else f"{ys[0]}–{ys[-1]}"

    climate_labels = {
        "precipitacao": "Precipitação", "temperatura": "Temperatura",
        "cobertura_vegetal": "Cobertura vegetal", "relevo": "Relevo + Hidrografia",
        "queimadas": "Queimadas",
    }
    climate_text = " + ".join(climate_labels.get(normalize_token(v), str(v)) for v in (selected_climates or []))
    method_labels = {
        "gwr": "GWR", "kernel": "Kernel", "heat": "Mapa de calor",
        "density": "Densidade", "moran": "Moran Local (LISA)",
        "overlay": "Sobreposição multivariada", "choropleth": "Coroplético",
    }
    method_label = method_labels.get(mode, mode.replace("_", " ").title())

    if mode in {"gwr"}:
        # GWR permanece fail-closed: não rotular uma superfície suavizada como
        # regressão geograficamente ponderada sem painel clima-saúde harmonizado.
        raise RuntimeError(
            "GWR científico indisponível para esta seleção: o painel município-período "
            "de clima e desfecho ainda não satisfaz a validação necessária ao ajuste."
        )

    if mode in {"kernel", "heat", "density"} and has_classified_values:
        import numpy as np
        work = mainland.copy()
        y = pd.to_numeric(work["total_casos"], errors="coerce").fillna(0).to_numpy(dtype=float)
        xy = np.column_stack([work.geometry.centroid.x.to_numpy(), work.geometry.centroid.y.to_numpy()])
        grid_n = 180
        minx, miny, maxx, maxy = work.total_bounds
        gx, gy = np.meshgrid(np.linspace(minx, maxx, grid_n), np.linspace(miny, maxy, grid_n))
        span = max(maxx-minx, maxy-miny)
        bandwidth = span * (0.075 if mode == "kernel" else 0.11 if mode == "heat" else 0.055)
        surface = np.zeros_like(gx, dtype=float)
        weights = y if mode != "density" else np.where(y > 0, 1.0, 0.0)
        for (px, py), wt in zip(xy, weights):
            surface += float(wt) * np.exp(-((gx-px)**2 + (gy-py)**2) / (2 * bandwidth**2))
        ax.imshow(surface, extent=[minx,maxx,miny,maxy], origin="lower", cmap="magma", alpha=.72, zorder=1)
        work.boundary.plot(ax=ax, color="#666666", linewidth=.35, zorder=2)
        variable_label = f"{method_label} ponderado por casos"
        resolved_title = f"{method_label} de {display_name} – Pernambuco"
        legend_handles = [Patch(facecolor="#d95f0e", edgecolor="#333333", label=variable_label)]
    elif mode in {"moran","lisa","moran_local"} and has_classified_values:
        try:
            from libpysal.weights import Queen
            from esda.moran import Moran_Local
            import numpy as np
            work=mainland.copy()
            y=pd.to_numeric(work["total_casos"],errors="coerce").fillna(0).to_numpy(dtype=float)
            if len(np.unique(y)) < 2:
                raise ValueError("A variavel selecionada nao possui variacao espacial.")
            w=Queen.from_dataframe(work, use_index=True)
            w.transform="r"
            lisa=Moran_Local(y,w,permutations=999,seed=20260923,island_weight=0)
            sig=np.asarray(lisa.p_sim) < 0.05
            q=np.asarray(lisa.q)
            # PySAL quadrants: 1 HH, 2 LH, 3 LL, 4 HL.
            labels=np.full(len(work),"Não significativo",dtype=object)
            labels[sig & (q==1)]="Alto-Alto (perto-perto)"
            labels[sig & (q==3)]="Baixo-Baixo (longe-longe)"
            labels[sig & (q==4)]="Alto-Baixo (perto-longe)"
            labels[sig & (q==2)]="Baixo-Alto (longe-perto)"
            work["lisa_cluster"]=labels
            colors={"Alto-Alto (perto-perto)":"#e41a1c","Baixo-Baixo (longe-longe)":"#1f78b4",
                    "Alto-Baixo (perto-longe)":"#fdae6b","Baixo-Alto (longe-perto)":"#9bd7ea",
                    "Não significativo":"#f2f2f2"}
            work.plot(ax=ax,color=work["lisa_cluster"].map(colors),edgecolor="#666666",linewidth=.45)
            legend_handles=[Patch(facecolor=v,edgecolor="#333333",label=k) for k,v in colors.items()]
            variable_label="Cluster LISA (p < 0,05; 999 permutações)"
            resolved_title=f"Moran Local (LISA) de {display_name} – Pernambuco"
        except ImportError as exc:
            raise RuntimeError("Dependencias cientificas do Moran Local indisponiveis (esda/libpysal).") from exc
    elif has_classified_values:
        clean_values=pd.to_numeric(mainland["total_casos"],errors="coerce").dropna().astype(float)
        requested=min(DEFAULT_CLASSES,max(2,int(clean_values.nunique())))
        _,edges=pd.qcut(clean_values,q=requested,retbins=True,duplicates="drop")
        class_bins=[float(v) for v in edges]
        palette=PALETTE[:max(1,len(class_bins)-1)]
        if len(class_bins)<2:
            class_bins=[float(clean_values.min()),float(clean_values.max())+1.0]; palette=PALETTE[:1]
        norm=BoundaryNorm(class_bins,ncolors=len(palette),clip=True)
        mainland["plot_color"]=[palette[min(norm(float(v)),len(palette)-1)] if pd.notna(v) else NO_DATA_COLOR for v in mainland["total_casos"]]
        mainland.plot(ax=ax,color=mainland["plot_color"],edgecolor="#666666",linewidth=.45)
        legend_handles=[Patch(facecolor=color,edgecolor="#333333",linewidth=.6,label=f"{class_bins[i]:.1f} – {class_bins[i+1]:.1f}") for i,color in enumerate(palette)]
    else:
        mainland.plot(ax=ax,color=NO_DATA_COLOR,edgecolor="#777777",linewidth=.45)
        legend_handles=[Patch(facecolor=NO_DATA_COLOR,edgecolor="#777777",label="Sem dados locais")]

    mainland.dissolve().boundary.plot(ax=ax,color="#111111",linewidth=1.8)
    set_standard_map_frame(ax, mainland)
    add_cartographic_elements(ax)
    context_parts = [f"Método: {method_label}"]
    if climate_text: context_parts.append(f"Clima: {climate_text}")
    if geres and geres != "ALL": context_parts.append(f"GERES: {geres}")
    if municipio_id: context_parts.append(f"Município IBGE: {municipio_id}")
    if socio_variable: context_parts.append(f"IBGE: {socio_variable}" + (f" ({socio_scope})" if socio_scope else ""))
    title_text=resolved_title + (f"\nPeríodo: {period}" if period else "") + "\n" + " | ".join(context_parts)
    ax.set_title(title_text,fontsize=18,fontweight="bold",pad=16,color="#111111")
    ax.legend(handles=legend_handles,title=variable_label,loc="lower center",
              bbox_to_anchor=(0.5,-0.18),ncol=min(5,max(1,len(legend_handles))),
              frameon=False,fontsize=10,title_fontsize=11)
    ax.set_axis_off()
    fig.subplots_adjust(left=.02,right=.99,top=.88,bottom=.21)
    source_label="DATASUS / cartografia municipal IBGE" if has_local_data else "Cartografia municipal IBGE"
    fig.text(.02,.025,f"Fonte: {source_label}. Elaboração: EpiGeoData.",fontsize=9,color="#333333")
    try:
        fig.savefig(output_file,dpi=dpi,facecolor=BACKGROUND_COLOR)
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
