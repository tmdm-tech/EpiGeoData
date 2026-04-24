# EpigeoData

Aplicativo com interface web em Python 3 (Flask) voltado para organizacao de dados geograficos em multiplas camadas para execucao em plataforma online de mapeamento.

## Estrutura da interface

- Cabeçalho com identidade visual da EpigeoData e ícone em tons de roxo inspirado em geoprocessamento.
- Visão das camadas operacionais para base cartográfica, vigilância epidemiológica, ambiente e logística de campo.
- Fluxo de execução online com preparo, validação, publicação e monitoramento das camadas.
- Cartões com pacotes prontos para integração com serviços e rotinas de mapeamento.

## Executar o projeto

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

## Testes

```bash
python3 -m py_compile app.py
```

## Pipeline GWR Epidemiologica

Pipeline completa em Python para analise espacial e geracao de mapas cientificos em PNG (300 DPI):

- Script: `scripts/generate_epidemiological_gwr_maps.py`
- Biblioteca de modelagem: `mgwr`
- Entradas: dataset tabular + shapefile/GeoJSON municipal (join por codigo IBGE)
- Saidas: distribuicao da doenca, coeficientes locais GWR por variavel, R2 local, residuos

Exemplo de execucao via CLI:

```bash
python scripts/generate_epidemiological_gwr_maps.py \
	--table data/epidemiologia.csv \
	--municipalities data/municipios_pe.geojson \
	--dependent taxa_doenca \
	--independent chuva temperatura densidade_populacional renda \
	--scheme natural_breaks \
	--dpi 300
```

Endpoint da API Flask:

- `POST /api/maps/epidemiological-gwr`
- `POST /api/maps/epidemiological-gwr-upload` (multipart/form-data)

Payload JSON esperado:

```json
{
	"table_path": "data/epidemiologia.csv",
	"municipalities_path": "data/municipios_pe.geojson",
	"dependent_var": "taxa_doenca",
	"independent_vars": ["chuva", "temperatura", "densidade", "renda"],
	"classification_scheme": "natural_breaks",
	"n_classes": 5,
	"target_crs": "EPSG:31985",
	"dpi": 300
}
```

Exemplo de upload multipart:

Campos obrigatorios:

- `table_file`: CSV/XLSX com variaveis epidemiologicas
- `municipalities_file`: GeoJSON, SHP ou ZIP contendo shapefile
- `dependent_var`: nome da variavel dependente
- `independent_vars`: repetir no form-data (ou CSV de nomes)

Exemplo via `curl`:

```bash
curl -X POST http://localhost:5000/api/maps/epidemiological-gwr-upload \
	-F "table_file=@data/epidemiologia_demo_pe.csv" \
	-F "municipalities_file=@data/municipios_pe_ibge.geojson" \
	-F "dependent_var=taxa_doenca" \
	-F "independent_vars=chuva_mm" \
	-F "independent_vars=temperatura_c" \
	-F "independent_vars=densidade_pop" \
	-F "independent_vars=renda_media" \
	-F "classification_scheme=natural_breaks" \
	-F "n_classes=5" \
	-F "target_crs=EPSG:31985" \
	-F "dpi=300"
```

## Implantacao

- O deploy oficial deste projeto e feito no Render com runtime Python 3 (configurado em `render.yaml`).
- O build instala dependencias com `pip install -r requirements.txt`.
- O start e feito com `gunicorn app:app`.
- A cada push na branch `main`, o Render executa o auto deploy.
