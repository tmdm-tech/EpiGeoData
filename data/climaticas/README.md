# Dados Climáticos

Este diretório contém os arquivos de dados climáticos para geração de mapas sobrepostos.

## Estrutura

- `precipitacao/` - Dados de precipitação acumulada (GeoTIFF)
- `temperatura/` - Dados de temperatura (GeoTIFF)  
- `queimadas/` - Focos de calor e queimadas (GeoJSON)
- `cobertura_vegetal/` - Índice de vegetação (GeoTIFF)

## Formato dos Dados

### GeoTIFF (Precipitação, Temperatura, Cobertura Vegetal)
- Resolução: 30m (SRTM/Landsat)
- Sistema de coordenadas: EPSG:4326 (WGS84)
- Banda única com valores normalizados [0-255]

### GeoJSON (Queimadas)
- Features com propriedades:
  - `timestamp`: Data/hora do foco
  - `intensity`: Intensidade (1-5)
  - `confidence`: Confiança (0-100)

## Sincronização

Os dados são sincronizados automaticamente via:
- INPE (Instituto Nacional de Pesquisas Espaciais)
- FUNCEME (Fundação Cearense de Meteorologia)
- COPERNICUS (Sentinel-2)

## Uso na Aplicação

Acessar via: `/api/climate-layers/{tipo}/{municipio_id}`

Tipos disponíveis:
- `precipitacao`
- `temperatura`
- `queimadas`
- `cobertura_vegetal`
