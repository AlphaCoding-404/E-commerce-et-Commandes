"""Génère kibana/dashboard.ndjson (data view + 11 visualisations + 1 dashboard).

Usage : python kibana/build_dashboard.py
Import dans Kibana : ./kibana/import_dashboard.sh  (ou Stack Management > Saved Objects > Import)
"""
import json
from pathlib import Path

DV_ID = "ecommerce-orders-dv"
OUT = Path(__file__).with_name("dashboard.ndjson")
NOT_CANCELLED = "not status:cancelled"
KVER = "8.13.4"


def search_source(query=""):
    return json.dumps({"query": {"query": query, "language": "kuery"}, "filter": [],
                       "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"})


def vis_object(vid, title, vis_state, query=""):
    vis_state["title"] = title
    return {"id": vid, "type": "visualization",
            "attributes": {"title": title, "visState": json.dumps(vis_state), "uiStateJSON": "{}",
                           "description": "", "version": 1,
                           "kibanaSavedObjectMeta": {"searchSourceJSON": search_source(query)}},
            "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                            "type": "index-pattern", "id": DV_ID}]}


def metric_agg(agg_id, agg_type, field=None):
    return {"id": agg_id, "enabled": True, "type": agg_type, "params": {"field": field} if field else {},
            "schema": "metric"}


def terms_agg(agg_id, field, size=10, schema="segment", order_by="1"):
    return {"id": agg_id, "enabled": True, "type": "terms", "schema": schema,
            "params": {"field": field, "orderBy": order_by, "order": "desc", "size": size,
                       "otherBucket": False, "otherBucketLabel": "Other",
                       "missingBucket": False, "missingBucketLabel": "Missing"}}


def date_hist_agg(agg_id, field="order_date", interval="M"):
    return {"id": agg_id, "enabled": True, "type": "date_histogram", "schema": "segment",
            "params": {"field": field, "useNormalizedEsInterval": True, "scaleMetricValues": False,
                       "interval": interval, "drop_partials": False, "min_doc_count": 1, "extended_bounds": {}}}


def metric_vis(agg):
    return {"type": "metric", "aggs": [agg], "params": {
        "addTooltip": True, "addLegend": False, "type": "metric",
        "metric": {"percentageMode": False, "useRanges": False, "colorSchema": "Green to Red",
                   "metricColorMode": "None", "colorsRange": [{"from": 0, "to": 10000}],
                   "labels": {"show": True}, "invertColors": False,
                   "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "", "fontSize": 48}}}}


def pie_vis(metric, bucket):
    return {"type": "pie", "aggs": [metric, bucket], "params": {
        "type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
        "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}}}


def xy_vis(kind, aggs, y_label, horizontal=False):
    """kind : 'histogram' (barres verticales), 'horizontal_bar' ou 'line'."""
    cat_pos, val_pos = ("left", "bottom") if horizontal else ("bottom", "left")
    series_type = "line" if kind == "line" else "histogram"
    return {"type": kind, "aggs": aggs, "params": {
        "type": kind, "grid": {"categoryLines": False},
        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": cat_pos, "show": True,
                          "style": {}, "scale": {"type": "linear"},
                          "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
        "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": val_pos,
                       "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                       "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                       "title": {"text": y_label}}],
        "seriesParams": [{"show": True, "type": series_type, "mode": "normal" if kind == "line" else "stacked",
                          "data": {"label": y_label, "id": "1"}, "valueAxis": "ValueAxis-1",
                          "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}],
        "addTooltip": True, "addLegend": False, "legendPosition": "right", "times": [],
        "addTimeMarker": False, "labels": {"show": False},
        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"}}}


def table_vis(aggs):
    return {"type": "table", "aggs": aggs, "params": {
        "perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
        "showTotal": False, "showToolbar": False, "totalFunc": "sum", "percentageCol": ""}}


visualizations = [
    ("kpi-revenue", "CA total (hors annulées)", metric_vis(metric_agg("1", "sum", "total_amount")), NOT_CANCELLED),
    ("kpi-orders", "Nombre de commandes", metric_vis(metric_agg("1", "count")), ""),
    ("kpi-basket", "Panier moyen", metric_vis(metric_agg("1", "avg", "total_amount")), NOT_CANCELLED),
    ("kpi-customers", "Clients actifs", metric_vis(metric_agg("1", "cardinality", "customer_id")), ""),
    ("revenue-month", "Evolution mensuelle du CA",
     xy_vis("line", [metric_agg("1", "sum", "total_amount"), date_hist_agg("2")], "CA"), NOT_CANCELLED),
    ("category-donut", "CA par catégorie",
     pie_vis(metric_agg("1", "sum", "total_amount"), terms_agg("2", "category")), NOT_CANCELLED),
    ("state-bar", "CA par État (top 10)",
     xy_vis("horizontal_bar", [metric_agg("1", "sum", "total_amount"), terms_agg("2", "state")], "CA", True),
     NOT_CANCELLED),
    ("top-products", "Top 10 produits (CA)",
     table_vis([metric_agg("1", "sum", "total_amount"), metric_agg("3", "sum", "quantity"),
                terms_agg("2", "product_name.keyword", 10, "bucket")]), NOT_CANCELLED),
    ("payment-pie", "Moyens de paiement",
     pie_vis(metric_agg("1", "count"), terms_agg("2", "payment_method")), ""),
    ("weekday-bar", "Commandes par jour de la semaine",
     xy_vis("histogram", [metric_agg("1", "count"), terms_agg("2", "order_weekday", 7)], "Commandes"), ""),
    ("status-bar", "Commandes par statut",
     xy_vis("histogram", [metric_agg("1", "count"), terms_agg("2", "status", 5)], "Commandes"), ""),
]

# (id visualisation, x, y, largeur, hauteur) - grille Kibana de 48 colonnes
layout = [
    ("kpi-revenue", 0, 0, 12, 8), ("kpi-orders", 12, 0, 12, 8), ("kpi-basket", 24, 0, 12, 8), ("kpi-customers", 36, 0, 12, 8),
    ("revenue-month", 0, 8, 32, 16), ("category-donut", 32, 8, 16, 16),
    ("state-bar", 0, 24, 24, 16), ("top-products", 24, 24, 24, 16),
    ("payment-pie", 0, 40, 16, 16), ("weekday-bar", 16, 40, 16, 16), ("status-bar", 32, 40, 16, 16),
]

objects = [{"id": DV_ID, "type": "index-pattern",
            "attributes": {"title": "ecommerce_orders", "name": "ecommerce_orders",
                           "timeFieldName": "order_date", "fields": "[]", "runtimeFieldMap": "{}"},
            "references": []}]
objects += [vis_object(v, t, s, q) for v, t, s, q in visualizations]

panels, refs = [], []
for i, (vid, x, y, w, h) in enumerate(layout):
    panels.append({"version": KVER, "type": "visualization", "panelIndex": str(i),
                   "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i)},
                   "embeddableConfig": {}, "panelRefName": f"panel_{i}"})
    refs.append({"name": f"panel_{i}", "type": "visualization", "id": vid})

objects.append({"id": "ecommerce-dashboard", "type": "dashboard",
                "attributes": {"title": "E-commerce - Tableau de bord des commandes", "description": "",
                               "hits": 0, "version": 1, "panelsJSON": json.dumps(panels),
                               "optionsJSON": json.dumps({"useMargins": True, "syncColors": False,
                                                          "syncCursor": True, "syncTooltips": False,
                                                          "hidePanelTitles": False}),
                               "timeRestore": True, "timeFrom": "2026-01-01T00:00:00.000Z", "timeTo": "now",
                               "refreshInterval": {"pause": True, "value": 0},
                               "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
                                   {"query": {"query": "", "language": "kuery"}, "filter": []})}},
                "references": refs})

with open(OUT, "w", encoding="utf-8") as f:
    for o in objects:
        f.write(json.dumps(o, ensure_ascii=False) + "\n")
print(f"{len(objects)} objets écrits dans {OUT}")
