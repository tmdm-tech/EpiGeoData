# Auditoria parcial e implementação inicial — etapas 1 e 2

## Evidências verificadas
- Render usa Python, `pip install -r requirements.txt`, `gunicorn app:app`, branch `main`; nenhuma configuração de produção foi alterada.
- `app.py` é aplicação Flask e contém catálogo de agravos e rotas para mapas GWR. O pipeline `scripts/generate_epidemiological_gwr_maps.py` aceita tabela e malha municipal e exporta mapas, mas não garante seleção V GERES nem alinhamento temporal.
- `Esquistossomose.csv` é positividade PCE municipal 2001–2023; não é taxa de detecção e não tem coluna de sexo no cabeçalho.
- `data/climaticas/temperatura.geojson` contém dois pontos em 2024-03-23 (Recife e Caruaru), insuficientes para análise municipal da V GERES.
- `data/epidemiologia_demo_pe.csv` é demonstração, não fonte validada para estimativas epidemiológicas.
- Existem `.venv`, `.dart_tool`, `__pycache__` e arquivos ZIP grandes versionados; não remover sem confirmar dependências.

## Implementado nesta branch
- `data_catalog.py`: inventário somente leitura, conferência de presença e metadados básicos, e bloqueio explícito de GWR sem dados harmonizados.
- `catalog_routes.py`: blueprint Flask `GET /api/catalog/datasets` e `POST /api/catalog/assess` (HTTP 422 quando bloqueado).
- `tests/test_data_catalog.py`: testes unitários para ausência de arquivos, contagem de observações e contrato HTTP.

## Integração ainda pendente
O blueprint **não está registrado em `app.py`**. Antes de considerar a etapa 2 concluída, inserir `from catalog_routes import catalog_blueprint` e `app.register_blueprint(catalog_blueprint)` imediatamente após `app = Flask(__name__)`, executar os testes e verificar endpoints na aplicação real. Não implantar na produção sem homologação.

## Limites
Esta é auditoria dos componentes examinados, não auditoria integral de cada arquivo do repositório. Não foram executados testes no ambiente remoto nem produzidos mapas, modelos ou dados. A função de prontidão é intencionalmente conservadora: retorna bloqueio até implementação e validação da harmonização e dos diagnósticos GWR.
