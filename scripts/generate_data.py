"""Génère l'export CSV des commandes (data/raw/orders.csv).

Aucune API publique ne fournit un historique de commandes réaliste et daté : les commandes sont donc
simulées sous forme d'un export CSV de la boutique. Elles référencent les identifiants de clients
(1..208) et de produits (1..194) de l'API DummyJSON, avec lesquels elles seront jointes.

Le fichier contient volontairement des défauts (doublons, valeurs manquantes, dates en deux formats,
identifiants inconnus) pour justifier l'étape de transformation. Graine fixe = résultat reproductible.
"""
import csv
import logging
import random
from datetime import date, timedelta
from pathlib import Path

from scripts.config import ORDERS_CSV

log = logging.getLogger(__name__)

N_PRODUCTS = 194    # identifiants produits DummyJSON : 1..194
N_CUSTOMERS = 208   # identifiants clients DummyJSON : 1..208

STATUSES = ["delivered", "shipped", "pending", "cancelled", "returned"]
STATUS_W = [70, 10, 7, 8, 5]
PAYMENTS = ["card", "paypal", "mobile_money", "cash_on_delivery"]
PAYMENT_W = [55, 20, 15, 10]


def generate(output_path: Path = ORDERS_CSV, n_orders: int = 5000, seed: int = 42) -> dict:
    rnd = random.Random(seed)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start, end = date(2026, 1, 1), date(2026, 9, 28)
    span = (end - start).days
    product_ids = list(range(1, N_PRODUCTS + 1))
    weights = [1 / (k + 1) ** 0.5 for k in range(N_PRODUCTS)]  # quelques produits très populaires

    orders = []
    for i in range(1, n_orders + 1):
        d = start + timedelta(days=int(span * (rnd.random() ** 0.7)))  # croissance dans le temps
        order_date = d.isoformat() if rnd.random() < 0.85 else d.strftime("%d/%m/%Y")  # formats mélangés
        customer_id = rnd.randint(1, N_CUSTOMERS) if rnd.random() > 0.005 else 9999  # client inconnu
        quantity = rnd.choices([1, 2, 3, 4, 5], [50, 25, 12, 8, 5])[0]
        if rnd.random() < 0.02:
            quantity = ""                                # valeur manquante
        elif rnd.random() < 0.01:
            quantity = rnd.choice([0, -1])               # valeur invalide
        payment = rnd.choices(PAYMENTS, PAYMENT_W)[0] if rnd.random() > 0.02 else ""
        discount = rnd.choices([0, 0.05, 0.10, 0.20], [60, 20, 15, 5])[0] if rnd.random() > 0.03 else ""
        orders.append({
            "order_id": f"O{i:06d}", "customer_id": customer_id,
            "product_id": rnd.choices(product_ids, weights)[0],
            "quantity": quantity, "order_date": order_date,
            "status": rnd.choices(STATUSES, STATUS_W)[0],
            "payment_method": payment, "discount": discount,
        })
    orders += rnd.sample(orders, int(n_orders * 0.03))   # doublons volontaires
    rnd.shuffle(orders)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
        writer.writeheader()
        writer.writerows(orders)
    log.info("Export de commandes généré : %s (%d lignes)", output_path, len(orders))
    return {"orders": len(orders)}


def ensure_source_data() -> dict:
    """Génère le CSV de commandes uniquement s'il n'existe pas déjà."""
    if ORDERS_CSV.exists():
        log.info("%s déjà présent, rien à générer.", ORDERS_CSV)
        return {"generated": False}
    return {"generated": True, **generate()}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(generate())
