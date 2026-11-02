"""Render monitoring isolated from the production crawler's Prometheus."""
import json
import pathlib

NS = "crawler-poc"
items = []


def add(kind, name, *, api="v1", **kwargs):
    metadata = {"name": name, "labels": {"app.kubernetes.io/part-of": NS}}
    if kind not in ("ClusterRole", "ClusterRoleBinding"):
        metadata["namespace"] = NS
    item = {"apiVersion": api, "kind": kind, "metadata": metadata, **kwargs}
    items.append(item)
    return item


def service(name, port, *, target=None, nodeport=None):
    spec = {"selector": {"app": name}, "ports": [{"name": "http", "port": port,
            "targetPort": target or port}]}
    if nodeport:
        spec["type"] = "NodePort"; spec["ports"][0]["nodePort"] = nodeport
    add("Service", name, spec=spec)


def deployment(name, container, *, service_account=None, volumes=None):
    spec = {"containers": [container]}
    if service_account:
        spec["serviceAccountName"] = service_account
    if volumes:
        spec["volumes"] = volumes
    return add("Deployment", name, api="apps/v1", spec={"replicas": 1,
        "selector": {"matchLabels": {"app": name}},
        "template": {"metadata": {"labels": {"app": name}}, "spec": spec}})


add("ServiceAccount", "crawler-poc-prometheus")
add("ClusterRole", "crawler-poc-prometheus", api="rbac.authorization.k8s.io/v1", rules=[
    {"apiGroups": [""], "resources": ["nodes", "nodes/proxy", "pods", "services", "endpoints"],
     "verbs": ["get", "list", "watch"]},
    {"nonResourceURLs": ["/metrics"], "verbs": ["get"]}])
add("ClusterRoleBinding", "crawler-poc-prometheus", api="rbac.authorization.k8s.io/v1",
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "crawler-poc-prometheus"},
    subjects=[{"kind": "ServiceAccount", "name": "crawler-poc-prometheus", "namespace": NS}])

prom_config = """global:
  scrape_interval: 15s
  evaluation_interval: 15s
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']
  - job_name: kube-state-metrics
    static_configs:
      - targets: ['crawler-poc-state-metrics:8080']
  - job_name: kubernetes-cadvisor
    scheme: https
    bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
    tls_config:
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
    kubernetes_sd_configs:
      - role: node
    relabel_configs:
      - target_label: __address__
        replacement: kubernetes.default.svc:443
      - source_labels: [__meta_kubernetes_node_name]
        target_label: node
      - source_labels: [__meta_kubernetes_node_name]
        target_label: __metrics_path__
        replacement: /api/v1/nodes/$1/proxy/metrics/cadvisor
  - job_name: kubernetes-kubelet
    scheme: https
    bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
    tls_config:
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
    kubernetes_sd_configs:
      - role: node
    relabel_configs:
      - target_label: __address__
        replacement: kubernetes.default.svc:443
      - source_labels: [__meta_kubernetes_node_name]
        target_label: node
      - source_labels: [__meta_kubernetes_node_name]
        target_label: __metrics_path__
        replacement: /api/v1/nodes/$1/proxy/metrics
  - job_name: crawler-poc
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [crawler-poc]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: 'true'
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        action: keep
        regex: metrics
      - source_labels: [__meta_kubernetes_pod_node_name]
        target_label: node
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
"""
add("ConfigMap", "crawler-poc-prometheus", data={"prometheus.yml": prom_config})
deployment("crawler-poc-prometheus", {
    "name": "prometheus", "image": "prom/prometheus:v3.15.1",
    "args": ["--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus",
             "--storage.tsdb.retention.time=24h", "--storage.tsdb.retention.size=2GB",
             "--web.external-url=http://172.16.191.1:18090/prometheus", "--web.route-prefix=/"],
    "ports": [{"name": "http", "containerPort": 9090}],
    "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}, "limits": {"cpu": "1", "memory": "1Gi"}},
    "volumeMounts": [{"name": "config", "mountPath": "/etc/prometheus"},
                     {"name": "data", "mountPath": "/prometheus"}],
    "readinessProbe": {"httpGet": {"path": "/-/ready", "port": 9090}, "initialDelaySeconds": 3},
}, service_account="crawler-poc-prometheus", volumes=[
    {"name": "config", "configMap": {"name": "crawler-poc-prometheus"}},
    {"name": "data", "emptyDir": {"sizeLimit": "3Gi"}}])
service("crawler-poc-prometheus", 9090)

add("ServiceAccount", "crawler-poc-state-metrics")
add("ClusterRole", "crawler-poc-state-metrics", api="rbac.authorization.k8s.io/v1", rules=[
    {"apiGroups": [""], "resources": ["nodes", "pods", "persistentvolumes", "persistentvolumeclaims"], "verbs": ["get", "list", "watch"]},
    {"apiGroups": ["apps"], "resources": ["deployments", "replicasets", "statefulsets"], "verbs": ["get", "list", "watch"]}])
add("ClusterRoleBinding", "crawler-poc-state-metrics", api="rbac.authorization.k8s.io/v1",
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "crawler-poc-state-metrics"},
    subjects=[{"kind": "ServiceAccount", "name": "crawler-poc-state-metrics", "namespace": NS}])
deployment("crawler-poc-state-metrics", {
    "name": "state-metrics", "image": "registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.20.0",
    "args": ["--namespaces=crawler-poc,kube-system", "--resources=nodes,pods,persistentvolumes,persistentvolumeclaims,deployments,statefulsets"],
    "ports": [{"name": "http", "containerPort": 8080}],
    "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"cpu": "250m", "memory": "256Mi"}},
}, service_account="crawler-poc-state-metrics")
service("crawler-poc-state-metrics", 8080)

nginx_config = """events { worker_connections 4096; }
http {
  access_log off;
  resolver 10.243.0.10 valid=10s;
  server {
    listen 8080;
    client_max_body_size 300m;
    location = /gateway-healthz { return 200 'ok'; }
    location ^~ /_worker/ { return 404; }
    location = /prometheus { return 301 /prometheus/; }
    location /prometheus/ {
      set $prometheus_backend http://crawler-poc-prometheus.crawler-poc.svc.cluster.local:9090;
      rewrite ^/prometheus/(.*)$ /$1 break;
      proxy_pass $prometheus_backend;
      proxy_read_timeout 60s;
    }
    location / {
      set $api_backend http://crawler-api.crawler-poc.svc.cluster.local:8080;
      proxy_pass $api_backend;
      proxy_read_timeout 180s;
    }
  }
}
"""
add("ConfigMap", "crawler-poc-gateway", data={"nginx.conf": nginx_config})
deployment("crawler-poc-gateway", {
    "name": "gateway", "image": "nginx:stable-alpine",
    "ports": [{"name": "http", "containerPort": 8080}],
    "resources": {"requests": {"cpu": "50m", "memory": "16Mi"}, "limits": {"cpu": "1", "memory": "128Mi"}},
    "volumeMounts": [{"name": "config", "mountPath": "/etc/nginx/nginx.conf", "subPath": "nginx.conf"}],
    "readinessProbe": {"httpGet": {"path": "/gateway-healthz", "port": 8080}},
}, volumes=[{"name": "config", "configMap": {"name": "crawler-poc-gateway"}}])
service("crawler-poc-gateway", 8080, nodeport=30080)

target = pathlib.Path(__file__).parent / "k8s/monitoring.json"
target.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, indent=2))
print("Rendered independent Prometheus, state metrics and gateway")
