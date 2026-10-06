"""Étape 2 - TRANSFORMATION : nettoyage, normalisation, jointures et enrichissement."""
import logging

import pandas as pd

from scripts.config import STAGING_DIR

log = logging.getLogger(__name__)

HIGH_VALUE_THRESHOLD = 500.0  # commande "à forte valeur" à partir de 500


def transform(paths: dict, run_date: str) -> dict:
    """paths = {"orders": ..., "customers": ..., "products": ...} (chemins issus de XCom)."""
    orders = pd.read_csv(paths["orders"], dtype=str)
    customers = pd.read_csv(paths["customers"], dtype=str)
    products = pd.read_csv(paths["products"], dtype=str)
    rows_in = len(orders)
    stats = {}

    # T1 - Suppression des doublons -------------------------------------------------
    n = len(orders)
    orders = orders.drop_duplicates(subset="order_id", keep="first")
    stats["orders_duplicates_removed"] = n - len(orders)
    customers = customers.drop_duplicates(subset="customer_id", keep="first")
    products = products.drop_duplicates(subset="product_id", keep="first")

    # T2 - Conversion de types + gestion des valeurs manquantes ---------------------
    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce")
    orders["discount"] = pd.to_numeric(orders["discount"], errors="coerce").fillna(0.0).clip(0, 1)
    orders["payment_method"] = orders["payment_method"].fillna("unknown").str.strip().str.lower()
    products["price"] = pd.to_numeric(products["price"], errors="coerce")
    customers[["country", "state", "city"]] = customers[["country", "state", "city"]].fillna("Unknown")

    required = ["customer_id", "product_id", "quantity", "order_date"]
    n = len(orders)
    orders = orders.dropna(subset=required)
    stats["orders_missing_required_removed"] = n - len(orders)
    orders["quantity"] = orders["quantity"].astype(int)

    # T3 - Normalisation des dates (deux formats sources : AAAA-MM-JJ et JJ/MM/AAAA) --
    iso = pd.to_datetime(orders["order_date"], format="%Y-%m-%d", errors="coerce")
    fr = pd.to_datetime(orders["order_date"], format="%d/%m/%Y", errors="coerce")
    orders["order_date"] = iso.fillna(fr)
    n = len(orders)
    orders = orders.dropna(subset=["order_date"])
    stats["orders_bad_date_removed"] = n - len(orders)

    # T4 - Filtrage des quantités invalides -----------------------------------------
    n = len(orders)
    orders = orders[orders["quantity"] > 0]
    stats["orders_invalid_quantity_removed"] = n - len(orders)

    # T5 - Normalisation du texte (pays, e-mails, statuts) ----------------------------
    customers["country"] = customers["country"].str.strip().str.title()
    customers["email"] = customers["email"].str.strip().str.lower()
    customers["customer_name"] = customers["customer_name"].str.strip().str.title()
    orders["status"] = orders["status"].str.strip().str.lower()
    products["category"] = products["category"].str.strip().str.replace("-", " ").str.title()
    products["product_name"] = products["product_name"].str.strip()

    # T6 - Jointures (commandes x clients x produits) ---------------------------------
    n = len(orders)
    df = orders.merge(customers[["customer_id", "customer_name", "email", "country", "state", "city"]],
                      on="customer_id", how="inner")
    df = df.merge(products[["product_id", "product_name", "category", "price"]],
                  on="product_id", how="inner")
    stats["orders_orphans_removed"] = n - len(df)

    # T7 - Nouvelles colonnes -----------------------------------------------------------
    df = df.rename(columns={"price": "unit_price"})
    df["total_amount"] = (df["quantity"] * df["unit_price"] * (1 - df["discount"])).round(2)
    df["order_month"] = df["order_date"].dt.strftime("%Y-%m")
    df["order_weekday"] = df["order_date"].dt.day_name()
    df["is_high_value"] = df["total_amount"] >= HIGH_VALUE_THRESHOLD
    df["is_cancelled"] = (df["status"] == "cancelled").astype(int)
    df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["etl_run_date"] = run_date

    columns = ["order_id", "customer_id", "customer_name", "email", "country", "state", "city",
               "product_id", "product_name", "category", "quantity", "unit_price", "discount",
               "total_amount", "order_date", "order_month", "order_weekday", "status",
               "payment_method", "is_high_value", "is_cancelled", "etl_run_date"]
    df = df[columns].sort_values("order_id")

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / "orders_clean.jsonl"
    df.to_json(out, orient="records", lines=True, force_ascii=False)

    stats = {k: int(v) for k, v in stats.items()}
    log.info("[transform] %d lignes en entrée -> %d en sortie. Détail : %s", rows_in, len(df), stats)
    return {"path": str(out), "rows_in": int(rows_in), "rows_out": int(len(df)), "stats": stats}
