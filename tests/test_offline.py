"""Test hors-ligne : simule l'API DummyJSON (même format de réponse) et exécute Extract -> Transform -> Validate.
Lancer :  python -m tests.test_offline   (ou pytest)
"""
import logging

import requests

from scripts import config, extract
from scripts.generate_data import generate
from scripts.transform import transform
from scripts.validate import validate_orders

STATES = ["Alabama", "Texas", "Ohio", "Utah", "Iowa", "Maine", "Idaho", "Nevada", "Oregon", "Florida"]


class FakeResponse:
    def __init__(self, payload): self._p = payload
    def raise_for_status(self): pass
    def json(self): return self._p


def fake_get(url, params=None, timeout=None):
    if url.endswith("/products"):
        return FakeResponse({"products": [{"id": i, "title": f"Produit {i}", "category": ["beauty", "mens-shirts", "home-decoration"][i % 3],
                                           "price": round(5 + (i * 7) % 900 + 0.99, 2)} for i in range(1, 195)],
                             "total": 194, "skip": 0, "limit": 194})
    if url.endswith("/users"):
        return FakeResponse({"users": [{"id": i, "firstName": "Prenom", "lastName": f"Nom{i}", "email": f"User{i}@x.dummyjson.com",
                                        "address": {"city": "Phoenix", "state": STATES[i % 10], "country": "United States"}}
                                       for i in range(1, 209)], "total": 208, "skip": 0, "limit": 208})
    raise requests.ConnectionError(url)


def test_pipeline_offline(tmp_path=None):
    requests.get = fake_get
    generate()
    results = [extract.extract_source(s) for s in config.SOURCES]
    assert {r["origin"] for r in results} == {"csv", "api"}
    t = transform({r["source"]: r["path"] for r in results}, run_date="2026-09-30")
    print(t)
    report = validate_orders(t["path"], t["rows_in"])
    assert report["passed"]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    test_pipeline_offline()
    print("TEST OK")
