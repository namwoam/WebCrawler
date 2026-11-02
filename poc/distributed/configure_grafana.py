"""Add the POC datasource/dashboard; credentials are read from the environment."""
import base64
import json
import os
import pathlib
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("GRAFANA_URL", "http://172.16.191.2:3000").rstrip("/")
USER = os.environ.get("GRAFANA_USER", "admin")
PASSWORD = os.environ["GRAFANA_PASSWORD"]
AUTH = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
UID = "crawler-poc-prometheus"


def request(path, data=None, method=None):
    req = urllib.request.Request(BASE + path, data=json.dumps(data).encode() if data is not None else None,
        headers={"Authorization": AUTH, "Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


source = {"uid": UID, "name": "Crawler POC Prometheus", "type": "prometheus", "access": "proxy",
          "url": "http://172.16.191.1:18090/prometheus", "isDefault": False,
          "jsonData": {"httpMethod": "POST", "timeInterval": "15s"}}
try:
    existing = request("/api/datasources/uid/" + UID)
    if existing["url"] != source["url"] or existing["type"] != "prometheus":
        raise RuntimeError("Datasource UID belongs to a different configuration")
    request("/api/datasources/uid/" + UID, source, "PUT")
except urllib.error.HTTPError as error:
    if error.code != 404:
        raise
    request("/api/datasources", source)

folders = request("/api/search?type=dash-folder&query=Crawler")
folder = next((f["uid"] for f in folders if f["title"] == "Crawler"), None)
if folder is None:
    folder = request("/api/folders", {"uid": "crawler-poc-folder", "title": "Crawler POC"})["uid"]

panels = []
datasource = {"type": "prometheus", "uid": UID}


def panel(title, expressions, *, x, y, w=12, h=8, unit="short", stat=False):
    expressions = expressions if isinstance(expressions, list) else [(title, expressions)]
    panels.append({"id": len(panels) + 1, "title": title, "type": "stat" if stat else "timeseries",
        "datasource": datasource, "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "legend": {"displayMode": "list", "placement": "bottom"}},
        "targets": [{"refId": chr(65+i), "expr": expr, "legendFormat": name, "datasource": datasource}
                    for i, (name, expr) in enumerate(expressions)]})


cards = [
    ("Ready nodes", 'sum(kube_node_status_condition{condition="Ready",status="true"})'),
    ("Running crawler workers", 'sum(kube_pod_status_phase{namespace="crawler-poc",pod=~"crawler-worker-.*",phase="Running"}) or vector(0)'),
    ("HTTP requests / sec", 'sum(rate(crawler_http_requests_total[1m])) or vector(0)'),
    ("Successful HTML / sec", 'sum(rate(crawler_urls_completed_total{outcome="success"}[1m])) or vector(0)'),
    ("URL error %", '100 * sum(rate(crawler_urls_completed_total{outcome="error"}[2m])) / clamp_min(sum(rate(crawler_urls_completed_total[2m])), 0.000001) or vector(0)'),
    ("Stored HTML", 'sum(crawler_html_bytes_stored) or vector(0)'),
]
for i, (title, expr) in enumerate(cards):
    panel(title, expr, x=i*4, y=0, w=4, h=4, stat=True,
          unit="bytes" if title == "Stored HTML" else "percent" if title == "URL error %" else "short")
panel("POC CPU by node — 40 core cap per node", [("{{node}}", 'sum by(node) (rate(container_cpu_usage_seconds_total{namespace="crawler-poc",container!="",container!="POD"}[2m]))')], x=0, y=4, unit="cores")
panel("POC memory by node", [("{{node}}", 'sum by(node) (container_memory_working_set_bytes{namespace="crawler-poc",container!="",container!="POD"})')], x=12, y=4, unit="bytes")
panel("HTTP request rate by node", [("{{node}}", 'sum by(node) (rate(crawler_http_requests_total[1m]))')], x=0, y=12, unit="reqps")
panel("URL completions", [("{{outcome}}", 'sum by(outcome) (rate(crawler_urls_completed_total[1m]))')], x=12, y=12, unit="ops")
panel("Active downloads and uploads", [("downloads {{node}}", 'sum by(node) (crawler_active_downloads)'), ("uploads {{node}}", 'sum by(node) (crawler_active_uploads)')], x=0, y=20)
panel("Host scheduling", [("pending URLs", 'sum(crawler_pending_urls)'), ("ready hosts", 'sum(crawler_ready_hosts)'), ("hosts cooling down", 'sum(crawler_cooldown_hosts)')], x=12, y=20)
panel("HTTP latency", [("p50", 'histogram_quantile(0.50,sum by(le)(rate(crawler_http_request_duration_seconds_bucket[2m])))'), ("p95", 'histogram_quantile(0.95,sum by(le)(rate(crawler_http_request_duration_seconds_bucket[2m])))')], x=0, y=28, unit="s")
panel("Result upload latency", [("p50", 'histogram_quantile(0.50,sum by(le)(rate(crawler_result_upload_duration_seconds_bucket[2m])))'), ("p95", 'histogram_quantile(0.95,sum by(le)(rate(crawler_result_upload_duration_seconds_bucket[2m])))')], x=12, y=28, unit="s")
panel("HTTP response status", [("{{status}}", 'sum by(status)(rate(crawler_http_requests_total[2m]))')], x=0, y=36, unit="reqps")
panel("POC CPU throttling", [("{{node}}", 'sum by(node)(rate(container_cpu_cfs_throttled_seconds_total{namespace="crawler-poc",container!="",container!="POD"}[2m]))')], x=12, y=36, unit="s")
panel("HTML volume usage", [("used", 'kubelet_volume_stats_used_bytes{namespace="crawler-poc",persistentvolumeclaim="crawler-poc-html"}'), ("capacity", 'kubelet_volume_stats_capacity_bytes{namespace="crawler-poc",persistentvolumeclaim="crawler-poc-html"}')], x=0, y=44, unit="bytes")
panel("HTML PVC request and phase", [("requested bytes", 'kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="crawler-poc",persistentvolumeclaim="crawler-poc-html"}')], x=12, y=44, unit="bytes")

dashboard = {"uid": "crawler-poc", "title": "Crawler POC — Distributed", "tags": ["crawler-poc", "kubernetes"],
    "description": "Three physical nodes, max 40 CPU each. Worker benchmarks: 10/25/50/75/100, baseline 50. Crawler metrics become available as application workers are deployed. Node CPU charts include POC namespace pods, excluding control-plane system processes.",
    "schemaVersion": 39, "version": 0, "refresh": "15s", "timezone": "browser",
    "time": {"from": "now-30m", "to": "now"}, "panels": panels, "editable": True}
here = pathlib.Path(__file__).parent
(here / "k8s/grafana-dashboard.json").write_text(json.dumps(dashboard, indent=2))
reply = request("/api/dashboards/db", {"dashboard": dashboard, "folderUid": folder, "overwrite": True})
health = request("/api/datasources/uid/" + UID + "/health")
print(json.dumps({"dashboard": BASE + reply["url"], "datasource_uid": UID, "health": health}))
