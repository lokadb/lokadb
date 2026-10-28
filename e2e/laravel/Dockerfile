FROM php:8.4-cli
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev unzip git \
 && docker-php-ext-install pdo_pgsql && rm -rf /var/lib/apt/lists/*
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer
WORKDIR /app
ENV COMPOSER_IPRESOLVE=4 COMPOSER_PROCESS_TIMEOUT=600
RUN composer create-project laravel/laravel . --no-interaction --prefer-dist --no-progress
COPY overlay/ /app/
ENTRYPOINT ["php", "artisan"]
