# The e2e Django application (e2e/django) with the five HTTP views of bench/load, served by gunicorn:
# worker processes with persistent database connections (CONN_MAX_AGE), as Django is run in production.
# Build context: the repository root.
FROM python:3.12-slim
RUN pip install --no-cache-dir "django>=5.2,<7" "psycopg[binary]>=3.2" gunicorn
WORKDIR /app
COPY e2e/django/ /app/
COPY bench/apps/django/ /app/
RUN rm -f /app/Dockerfile && APP_RELEASE=1 python manage.py makemigrations docs
ENV DJANGO_SETTINGS_MODULE=benchsettings APP_RELEASE=1 WORKERS=16
EXPOSE 8000
CMD ["sh", "-c", "exec gunicorn benchhttp.wsgi --bind 0.0.0.0:8000 --workers $WORKERS --access-logfile -"]
