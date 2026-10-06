"""Étape 3 - VALIDATION : contrôles qualité avant chargement.

Si un contrôle échoue, la fonction lève une exception : la tâche Airflow passe en échec,
et les tâches suivantes (chargement, analyse) ne sont PAS exécutées.
"""
import json
import logging
from datetime import datetime, timedelta

import pandas as pd

from scripts.config import STAGING_DIR

log = logging.getLogger(__name__)

REQUIRED_FIELDS = ["order_id", "customer_id", "product_id", "order_date", "quantity",
                   "unit_price", "total_amount", "status", "country", "category"]
ALLOWED_STATUS = {"delivered", "shipped", "pending", "cancelled", "returned"}
MIN_RETENTION = 0.80  # au moins 80 % des lignes brutes doivent survivre à la transformation


def validate_orders(path: str, raw_rows: int) -> dict:
    with open(path, encoding="utf-8") as f:
        df = pd.DataFrame([json.loads(line) for line in f])

    checks = []

    def add(name, passed, detail=""):
        checks.append({"check": name, "passed": bool(passed), "detail": str(detail)})

    # 1. Nombre de lignes
    add("lignes > 0", len(df) > 0, f"{len(df)} lignes")
    retention = len(df) / raw_rows if raw_rows else 0
    add(f"rétention >= {MIN_RETENTION:.0%}", retention >= MIN_RETENTION, f"{retention:.1%} ({len(df)}/{raw_rows})")

    if len(df) > 0:
        # 2. Champs obligatoires
        missing = {c: int(df[c].isna().sum()) for c in REQUIRED_FIELDS if c not in df or df[c].isna().any()}
        add("champs obligatoires non nuls", not missing, missing or "ok")

        # 3. Unicité de l'identifiant
        dup = int(df["order_id"].duplicated().sum())
        add("order_id unique", dup == 0, f"{dup} doublons")

        # 4. Types et valeurs
        add("quantity entier > 0", pd.api.types.is_integer_dtype(df["quantity"]) and (df["quantity"] > 0).all())
        add("unit_price numérique > 0", pd.api.types.is_numeric_dtype(df["unit_price"]) and (df["unit_price"] > 0).all())
        add("total_amount numérique >= 0", pd.api.types.is_numeric_dtype(df["total_amount"]) and (df["total_amount"] >= 0).all())
        bad_status = sorted(set(df["status"]) - ALLOWED_STATUS)
        add("statuts autorisés", not bad_status, bad_status or "ok")

        # 5. Cohérence des dates
        dates = pd.to_datetime(df["order_date"], errors="coerce")
        in_range = dates.notna().all() and dates.min() >= datetime(2020, 1, 1) and dates.max() <= datetime.now() + timedelta(days=1)
        add("dates valides et plausibles", in_range, f"{dates.min()} -> {dates.max()}")

        # 6. Cohérence du calcul du montant
        expected = (df["quantity"] * df["unit_price"] * (1 - df["discount"])).round(2)
        add("total_amount cohérent", ((expected - df["total_amount"]).abs() < 0.011).all())

    report = {"rows_checked": int(len(df)), "passed": all(c["passed"] for c in checks), "checks": checks}
    (STAGING_DIR / "validation_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    for c in checks:
        log.info("[validate] %s %-35s %s", "OK  " if c["passed"] else "FAIL", c["check"], c["detail"])

    if not report["passed"]:
        failed = [c["check"] for c in checks if not c["passed"]]
        raise ValueError(f"Validation échouée : {failed}")
    log.info("[validate] Tous les contrôles sont passés.")
    return report
