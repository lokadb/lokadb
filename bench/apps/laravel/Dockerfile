# The e2e Laravel application (e2e/laravel) with the five HTTP routes of bench/load, served the way
# PHP is served: PHP_CLI_SERVER_WORKERS processes, one request each, so every request opens its own database connection
# (no persistent connections; the proxy and the cloud see a connection per request).
# Build context: the repository root.
FROM php:8.4-cli
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev unzip git \
 && docker-php-ext-install pdo_pgsql && rm -rf /var/lib/apt/lists/*
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer
WORKDIR /app
ENV COMPOSER_IPRESOLVE=4 COMPOSER_PROCESS_TIMEOUT=600
RUN composer create-project laravel/laravel . --no-interaction --prefer-dist --no-progress
COPY e2e/laravel/overlay/ /app/
COPY bench/apps/laravel/overlay/ /app/
ENV SESSION_DRIVER=array CACHE_STORE=array LOG_CHANNEL=stderr APP_ENV=production APP_DEBUG=false \
    PHP_CLI_SERVER_WORKERS=16
EXPOSE 8000
# `artisan migrate ...` for the schema; the container itself runs /app/serve.sh (see app_start in scenarios/http.sh)
ENTRYPOINT ["php", "artisan"]
