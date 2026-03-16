import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "demo-secret-key-change-in-production")

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
)

# Disable CSRF for demo simplicity
WTF_CSRF_ENABLED = False

# Allow embedding dashboards
SESSION_COOKIE_SAMESITE = "Lax"

# Enable feature flags for Flight SQL
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
