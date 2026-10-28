from lokadbsite.settings import *  # noqa: F401,F403

ROOT_URLCONF = "benchhttp.urls"
ALLOWED_HOSTS = ["*"]
INSTALLED_APPS = ["docs"]
DATABASES["default"]["CONN_MAX_AGE"] = 600  # a worker keeps its connection
