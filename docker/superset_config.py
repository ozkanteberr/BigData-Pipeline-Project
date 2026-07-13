"""
Apache Superset configuration for the Olist Big Data Pipeline.

Environment variables used:
  DATABASE_URL     – SQLAlchemy connection string for Superset metadata DB
  REDIS_URL        – Redis URL for caching and Celery broker
  SUPERSET_SECRET_KEY – Secret key for session cookies
"""

import os

# -------------------------------------------------------------------
# Core
# -------------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "olist-bigdata-secret-change-in-prod")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://superset:superset_password@superset-db:5432/superset",
)

# -------------------------------------------------------------------
# Cache (Redis)
# -------------------------------------------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://superset-redis:6379/0")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": REDIS_URL,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": REDIS_URL,
}

# -------------------------------------------------------------------
# Feature flags
# -------------------------------------------------------------------
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# -------------------------------------------------------------------
# Misc
# -------------------------------------------------------------------
# Disable example data loading
SUPERSET_LOAD_EXAMPLES = False

# Allow embedding dashboards (optional, useful for demos)
SESSION_COOKIE_SAMESITE = "Lax"
ENABLE_CORS = True

# Prevent Superset from timing out on Spark queries
SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_TIMEOUT = 120
