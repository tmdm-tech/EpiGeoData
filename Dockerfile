# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM ghcr.io/cirruslabs/flutter:stable AS build

WORKDIR /app

# Copiar somente os arquivos de dependência primeiro (cache de camadas)
COPY pubspec.yaml pubspec.lock ./
RUN flutter pub get

# Copiar o restante do código
COPY . .

# Build web em modo release
RUN flutter build web --release

# ── Stage 2: Serve ───────────────────────────────────────────────────────────
FROM nginx:1.27-alpine

# Copiar os arquivos do build para o Nginx
COPY --from=build /app/build/web /usr/share/nginx/html

# Configuração customizada para SPA (Flutter Web)
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
