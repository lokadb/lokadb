#!/bin/sh
# The database settings the container got are written to .env, where Laravel reads them.
for v in DB_CONNECTION DB_HOST DB_PORT DB_DATABASE DB_USERNAME DB_PASSWORD; do
  eval "val=\$$v"
  sed -i "/^$v=/d" /app/.env
  echo "$v=$val" >>/app/.env
done
# PHP's built-in server with Laravel's router, as artisan serve runs it, but without artisan serve around it: that one
# parses the server's output and died on a line it did not expect (Carbon::createFromFormat, 26.10)
cd /app
PHP_CLI_SERVER_WORKERS=${PHP_CLI_SERVER_WORKERS:-16} exec php -S 0.0.0.0:8000 -t public vendor/laravel/framework/src/Illuminate/Foundation/resources/server.php
