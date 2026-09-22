# Auditoria das etapas 1 e 2 — estado verificado

## Escopo e evidências
- Aplicação Flask: `app.py` continua sendo a aplicação base; `wsgi_catalog.py` importa essa instância e registra `catalog_blueprint`, evitando duplicar a aplicação.
- Render: `render.yaml` nesta branch aponta para `gunicorn wsgi_catalog:app --bind 0.0.0.0:$PORT`; a configuração da branch principal não foi modificada. A publicação efetiva depende da configuração do serviço Render e não foi verificada.
- GitHub Actions: a execução 35746191144 do commit 75f2728 concluiu com sucesso, incluindo instalação das dependências e testes de catálogo e integração. A mudança posterior em `render.yaml` (commit d4d7cb4) requer resultado de CI próprio; sucesso anterior não comprova o commit posterior.
- `Esquistossomose.csv`: positividade municipal do PCE entre 2001 e 2023; não é taxa de detecção e não tem estratificação por sexo.
- `data/climaticas/temperatura.geojson`: apenas duas observações pontuais em 2024-03-23 (Recife e Caruaru), incompatíveis com regressão municipal temporal da V GERES.
- `data/epidemiologia_demo_pe.csv`: demonstração, não observações epidemiológicas homologadas.
- `scripts/generate_epidemiological_gwr_maps.py`: não garante vinculação oficial à V GERES nem compatibilidade temporal/espacial entre fontes; resultados GWR não homologados.
- `data_catalog.py`: catálogo somente leitura, verificação básica de presença e bloqueio conservador de GWR; `catalog_routes.py` expõe GET `/api/catalog/datasets` e POST `/api/catalog/assess`.

## Critérios de aceite e situação
1. Catálogo e rotas implementados: sim, nesta branch.
2. Registro no processo Flask de desenvolvimento: sim, via `wsgi_catalog.py`, coberto por teste de integração.
3. Configuração declarativa Render nesta branch: sim, usa `wsgi_catalog:app`.
4. CI do commit 75f2728: aprovado (https://github.com/tmdm-tech/EpiGeoData/actions/runs/35746191144).
5. CI do commit mais recente, testes de regressão de todas as rotas e validação real do deploy: ainda exigem evidência específica.
6. Auditoria integral de todos os arquivos, proveniência oficial da malha, segurança, dependências, licenças e integração de fontes: não concluída.
7. Análise GWR válida para esquistossomose + V GERES + sexo + temperatura: bloqueada corretamente; faltam dados epidemiológicos estratificados, temperatura municipal no mesmo período, vínculo oficial GERES–IBGE e diagnósticos estatísticos.

## Riscos e próximos controles
- O campo `autoDeploy: true` no blueprint não prova qual branch o serviço Render acompanha; conferir configuração efetiva antes de qualquer implantação.
- O código legado de mapas deve ser submetido a testes de regressão e à validação de agregação por município/ano/sexo, preservando códigos IBGE de sete dígitos.
- A função `assess_analysis` sempre bloqueia, mesmo se novas fontes forem adicionadas: evoluir para verificações efetivas de esquema, cobertura, proveniência, granularidade e alinhamento antes de liberar modelos.
- Não inventar medições, interpolar dados sem protocolo nem tratar arquivo demonstrativo como evidência real.

**Conclusão:** implementação inicial integrada e CI do commit anterior verificados; não declarar etapas 1 e 2 100% encerradas até concluir auditoria integral, regressão e validação da configuração de implantação.
