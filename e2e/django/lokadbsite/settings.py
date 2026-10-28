import os

SECRET_KEY = "demo"
DEBUG = False
INSTALLED_APPS = ["docs"]
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": os.environ.get("DB_HOST", "localhost"),
        "PORT": os.environ.get("DB_PORT", "5432"),
        "NAME": os.environ.get("DB_NAME", "django"),
        "USER": os.environ.get("DB_USER", "app"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "app"),
    }
}
# Which release of the application code is deployed (release 2 adds Document.status).
APP_RELEASE = int(os.environ.get("APP_RELEASE", "1"))
