FROM python:3.12-slim
RUN pip install --no-cache-dir "django>=5.2,<7" "psycopg[binary]>=3.2"
WORKDIR /app
COPY . /app/
# Release 1 and release 2 migrations, generated the usual way.
RUN APP_RELEASE=1 python manage.py makemigrations docs \
 && APP_RELEASE=2 python manage.py makemigrations docs -n document_status
ENTRYPOINT ["python", "manage.py"]
