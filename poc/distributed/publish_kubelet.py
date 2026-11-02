"""Publish kubelet on a POC node, retaining its existing Docker volumes."""
import json
import pathlib
import subprocess
import sys

node = int(sys.argv[1])
assert node in (1, 3, 4)
name = f"crawler-poc-k3s-{node}"
spec = json.loads(subprocess.check_output(["docker", "inspect", name]))[0]
assert spec["Config"]["Labels"]["org.webcrawler.poc"] == "distributed-crawler"
subprocess.run(["docker", "update", "--cpus", "40", "--memory", "48g", "--memory-swap", "48g", name],
               check=True, capture_output=True)
ports = spec["HostConfig"]["PortBindings"]
destinations = {m["Destination"] for m in spec["Mounts"]}
identity_present = "/etc/rancher/node" in destinations
pv_present = node == 1 or "/var/lib/crawler-poc-pv" in destinations
pool_needed = node in (3, 4) and pathlib.Path(f"/data/crawler-poc-r14725077/node-{node}/pool-300Gi.img").exists()
pool_entrypoint = spec["Config"]["Entrypoint"] == ["/usr/local/bin/poc-node-entrypoint.sh"]
if "10250/tcp" in ports and identity_present and pv_present and (not pool_needed or pool_entrypoint):
    print(f"node {node}: kubelet, identity and data mounts already configured")
    raise SystemExit(0)
root = pathlib.Path.home() / "crawler-poc"
backup = root / f"node-{node}-before-metrics.json"
backup.write_text(json.dumps(spec, indent=2)); backup.chmod(0o600)
envfile = root / f"node-{node}-environment"
envfile.write_text("\n".join(spec["Config"]["Env"]) + "\n"); envfile.chmod(0o600)
args = ["docker", "run", "-d", "--name", name, "--hostname", spec["Config"]["Hostname"],
        "--label", "org.webcrawler.poc=distributed-crawler", "--privileged",
        "--restart", "unless-stopped", "--cpus", "40", "--memory", "48g",
        "--memory-swap", "48g", "--pids-limit", "8192", "--ulimit", "nofile=65535:65535",
        "--env-file", str(envfile), "--network", "crawler-poc-net", "--ip", f"10.250.{node}.10"]
for path, options in spec["HostConfig"]["Tmpfs"].items():
    args += ["--tmpfs", path + (":" + options if options else "")]
for mount in spec["Mounts"]:
    if mount["Type"] == "volume":
        source = mount["Name"]
    elif mount["Type"] == "bind":
        source = mount["Source"]
    else:
        continue
    args += ["-v", source + ":" + mount["Destination"] + (":ro" if not mount["RW"] else "")]
for container_port, bindings in ports.items():
    for binding in bindings:
        args += ["-p", f"{binding['HostIp']}:{binding['HostPort']}:{container_port}"]
if not identity_present:
    args += ["-v", f"crawler-poc-identity-{node}:/etc/rancher/node"]
if not pv_present:
    args += ["-v", f"/data/crawler-poc-r14725077/node-{node}:/var/lib/crawler-poc-pv"]
if "10250/tcp" not in ports:
    args += ["-p", f"172.16.191.{node}:10250:10250"]
if pool_needed:
    if "/usr/local/bin/poc-node-entrypoint.sh" not in destinations:
        args += ["-v", str(root / "pool-entrypoint.sh") + ":/usr/local/bin/poc-node-entrypoint.sh:ro"]
    args += ["--entrypoint", "/usr/local/bin/poc-node-entrypoint.sh"]
args += [spec["Config"]["Image"], *spec["Config"]["Cmd"]]
subprocess.run(["docker", "stop", "--time", "30", name], check=True, capture_output=True)
subprocess.run(["docker", "rm", name], check=True, capture_output=True)  # Deliberately no -v.
result = subprocess.run(args, text=True, capture_output=True)
if result.returncode:
    raise SystemExit(result.stderr)
print(f"node {node}: kubelet, persistent identity and data mounts configured; existing volumes retained")
