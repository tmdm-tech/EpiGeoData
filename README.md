# EpigeoData

Aplicativo Flutter com uma interface voltada para organização de dados geográficos em múltiplas camadas para execução em plataforma online de mapeamento.

## Estrutura da interface

- Cabeçalho com identidade visual da EpigeoData e ícone em tons de roxo inspirado em geoprocessamento.
- Visão das camadas operacionais para base cartográfica, vigilância epidemiológica, ambiente e logística de campo.
- Fluxo de execução online com preparo, validação, publicação e monitoramento das camadas.
- Cartões com pacotes prontos para integração com serviços e rotinas de mapeamento.

## Executar o projeto

```bash
flutter pub get
flutter run
```

## Testes

```bash
flutter test
```

## Implantacao

- O deploy oficial deste projeto e feito com Docker via Render (configurado em `render.yaml`).
- O arquivo `requirements.txt` existe apenas para compatibilidade com detectores de build e nao define o runtime principal do app.
- A cada push na branch `main`, o Render executa o auto deploy.
