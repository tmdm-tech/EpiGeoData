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

## Implantacao

- O deploy oficial deste projeto e feito no Render com runtime Python 3 (configurado em `render.yaml`).
- O build instala dependencias com `pip install -r requirements.txt`.
- O start e feito com `gunicorn app:app`.
- A cada push na branch `main`, o Render executa o auto deploy.
