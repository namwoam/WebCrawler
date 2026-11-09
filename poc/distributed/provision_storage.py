"""Render a separate local-path provisioner with quota-backed POC pools."""
import json
import pathlib
import subprocess

HERE = pathlib.Path(__file__).parent
raw = subprocess.check_output([
    "kubectl", "create", "--dry-run=client", "-f",
    str(HERE / "k8s/local-path.upstream.yaml"), "-o", "json"], text=True)
items = []
decoder = json.JSONDecoder()
while raw.strip():
    raw = raw.lstrip()
    item, count = decoder.raw_decode(raw)
    items.append(item)
    raw = raw[count:]
names = {
    "local-path-storage": "crawler-poc",
    "local-path-provisioner-service-account": "crawler-poc-pool-provisioner",
    "local-path-provisioner-role": "crawler-poc-pool-provisioner-role",
    "local-path-provisioner-bind": "crawler-poc-pool-provisioner-bind",
    "local-path-provisioner": "crawler-poc-pool-provisioner",
    "local-path-config": "crawler-poc-pool-config",
    "local-path": "crawler-poc-pool",
    "rancher.io/local-path": "crawler-poc.io/pool",
}


def rename(value):
    if isinstance(value, str):
        return names.get(value, value)
    if isinstance(value, list):
        return [rename(v) for v in value]
    if isinstance(value, dict):
        return {k: rename(v) for k, v in value.items()}
    return value


rendered = []
for original in items:
    if original["kind"] == "Namespace":
        continue
    item = rename(original)
    item["metadata"].setdefault("labels", {})["app.kubernetes.io/part-of"] = "crawler-poc"
    if item["kind"] == "Deployment":
        spec = item["spec"]["template"]["spec"]
        spec["nodeSelector"] = {"kubernetes.io/hostname": "crawler-poc-1"}
        container = spec["containers"][0]
        container["command"][0] = "local-path-provisioner"
        container["command"] += [
            "--provisioner-name", "crawler-poc.io/pool",
            "--configmap-name", "crawler-poc-pool-config",
            "--service-account-name", "crawler-poc-pool-provisioner",
            "--helper-image", "docker.io/crawler-poc/storage-helper:0.1",
            "--allow-unsafe-helper-pod-template",
        ]
        container["resources"] = {"requests": {"cpu": "25m", "memory": "32Mi"},
                                  "limits": {"cpu": "250m", "memory": "256Mi"}}
    elif item["kind"] == "StorageClass":
        item["reclaimPolicy"] = "Retain"
        item["parameters"] = {"volumeType": "local"}
        item["allowedTopologies"] = [{"matchLabelExpressions": [{
            "key": "kubernetes.io/hostname", "values": ["crawler-poc-3", "crawler-poc-4"]}]}]
    elif item["kind"] == "ConfigMap":
        item["data"]["config.json"] = json.dumps({"nodePathMap": [
            {"node": "DEFAULT_PATH_FOR_NON_LISTED_NODES", "paths": []},
            *[{"node": f"crawler-poc-{n}", "paths": ["/var/lib/crawler-poc-pv/pool"]}
              for n in (3, 4)],
        ]})
        item["data"]["setup"] = (HERE / "storage-helper/setup.sh").read_text()
        item["data"]["teardown"] = "#!/bin/sh\nset -eu\necho 'Retained POC volumes require explicit reclamation'\nexit 1\n"
        item["data"]["helperPod.yaml"] = """apiVersion: v1
kind: Pod
metadata:
  name: helper-pod
spec:
  containers:
  - name: helper-pod
    image: docker.io/crawler-poc/storage-helper:0.1
    imagePullPolicy: IfNotPresent
    securityContext:
      privileged: true
    resources:
      requests:
        cpu: 10m
        memory: 32Mi
      limits:
        cpu: 200m
        memory: 128Mi
"""
    rendered.append(item)

(HERE / "k8s/pool-provisioner.json").write_text(json.dumps({
    "apiVersion": "v1", "kind": "List", "items": rendered}, indent=2))
(HERE / "k8s/storage.json").write_text(json.dumps({
    "apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {
        "name": "crawler-poc-html", "namespace": "crawler-poc",
        "labels": {"app.kubernetes.io/part-of": "crawler-poc"}},
    "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "crawler-poc-pool",
             "resources": {"requests": {"storage": "250Gi"}}}}, indent=2))
print("Rendered isolated quota-backed provisioner and dynamic 250Gi HTML claim")
