"""Configuration centrale du projet (chemins, nom d'index, connexion Airflow)."""
import os
from pathlib import Path

# Dans le conteneur Airflow : BASE_DIR = /opt/airflow
BASE_DIR = Path(os.getenv("PROJECT_DIR", Path(__file__).resolve().parent.parent))
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
ES_CONFIG_DIR = BASE_DIR / "elasticsearch"

DEFAULT_INDEX = os.getenv("ES_INDEX", "ecommerce_orders")
ES_CONN_ID = "elasticsearch_default"          # Connection Airflow
ES_FALLBACK_HOST = os.getenv("ES_HOST", "http://localhost:9200")  # hors Airflow (tests locaux)

# --- Sources -----------------------------------------------------------------
# 1) Fichier CSV : export des commandes de la boutique (data/raw/orders.csv)
ORDERS_CSV = RAW_DIR / "orders.csv"

# 2) API publique REST DummyJSON : catalogue produits et clients
API_BASE_URL = os.getenv("API_BASE_URL", "https://dummyjson.com")
API_TIMEOUT = 30
API_ENDPOINTS = {
    "products":  {"path": "/products", "key": "products", "select": "id,title,category,price"},
    "customers": {"path": "/users",    "key": "users",    "select": "id,firstName,lastName,email,address"},
}

SOURCES = ("orders", "customers", "products")
