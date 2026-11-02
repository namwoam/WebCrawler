"""Create an isolated three-host K3s POC using the existing Docker daemons."""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import pathlib
import secrets
import shlex
import subprocess

NODES = (1, 3, 4)
IMAGE = "rancher/k3s:v1.37.1-k3s1"
LABEL = "org.webcrawler.poc=distributed-crawler"


def remote(node: int, script: str, *, stdin: str | None = None) -> str:
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
               f"r14725077@172.16.191.{node}", script]
    result = subprocess.run(command, input=stdin, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"host {node}: {result.stderr or result.stdout}")
    return result.stdout


def prepare(node: int, token: str) -> None:
    script = r'''import pathlib,sys,subprocess,json
root=pathlib.Path.home()/"crawler-poc"
root.mkdir(mode=0o700,exist_ok=True)
env=root/"cluster.env"
value=sys.stdin.read()
if env.exists() and env.read_text()!=value:
 raise SystemExit("A different cluster token already exists; refusing to overwrite")
env.write_text(value);env.chmod(0o600)
(root/"resolv.conf").write_text("nameserver 8.8.8.8\nnameserver 1.1.1.1\n")
'''
    remote(node, "python3 -c " + shlex.quote(script), stdin="K3S_TOKEN=" + token + "\n")
    print(f"host {node}: prepared", flush=True)
    remote(node, "docker pull " + shlex.quote(IMAGE))
    print(f"host {node}: image ready", flush=True)


def start(node: int) -> None:
    name = f"crawler-poc-k3s-{node}"
    check = remote(node, "docker ps -a --filter name=" + name + " --format '{{.Names}}'").strip()
    if check == name:
        existing = remote(node, "docker inspect " + name + " --format '{{index .Config.Labels \"org.webcrawler.poc\"}}'").strip()
        if existing != "distributed-crawler":
            raise RuntimeError("Refusing to reuse a container without the POC label")
        print(f"host {node}: existing node retained", flush=True)
        return
    network = remote(node, "docker network ls --filter name='^crawler-poc-net$' --format '{{.Name}}'").strip()
    if not network:
        remote(node, f"docker network create --label {LABEL} --subnet 10.250.{node}.0/24 crawler-poc-net")
    args = ["docker", "run", "-d", "--name", name, "--hostname", name,
            "--label", LABEL, "--restart", "unless-stopped", "--privileged",
            "--cpus", "40", "--memory", "48g", "--memory-swap", "48g",
            "--pids-limit", "8192", "--ulimit", "nofile=65535:65535",
            "--tmpfs", "/run", "--tmpfs", "/var/run",
            "--network", "crawler-poc-net", "--ip", f"10.250.{node}.10",
            "--env-file", "/home/r14725077/crawler-poc/cluster.env",
            "-v", f"crawler-poc-k3s-{node}:/var/lib/rancher/k3s",
            "-v", f"crawler-poc-identity-{node}:/etc/rancher/node",
            "-v", "/home/r14725077/crawler-poc/resolv.conf:/etc/poc-resolv.conf:ro",
            "-p", f"172.16.191.{node}:8472:8472/udp",
            "-p", f"172.16.191.{node}:10250:10250",
            "-p", f"172.16.191.{node}:18090:30080"]
    if node == 1:
        args += ["-p", "172.16.191.1:16443:6443"]
    else:
        args += ["-e", "K3S_URL=https://172.16.191.1:16443"]
        args += ["-v", f"/data/crawler-poc-r14725077/node-{node}:/var/lib/crawler-poc-pv"]
    args += [IMAGE, "server" if node == 1 else "agent",
             "--node-name", f"crawler-poc-{node}",
             "--node-ip", f"10.250.{node}.10",
             "--node-external-ip", f"172.16.191.{node}",
             "--flannel-iface", "eth0", "--resolv-conf", "/etc/poc-resolv.conf",
             "--node-label", f"crawler-poc/physical-host=172.16.191.{node}"]
    if node == 1:
        args += ["--disable", "traefik", "--disable", "servicelb",
                 "--disable", "metrics-server", "--flannel-backend", "vxlan",
                 "--flannel-external-ip", "--cluster-cidr", "10.242.0.0/16",
                 "--service-cidr", "10.243.0.0/16", "--cluster-dns", "10.243.0.10",
                 "--tls-san", "172.16.191.1", "--advertise-address", "172.16.191.1",
                 "--advertise-port", "16443"]
    print(f"host {node}: started {remote(node, shlex.join(args)).strip()}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    secret = args.state_dir / "cluster-token"
    if not secret.exists():
        secret.write_text(secrets.token_hex(32)); secret.chmod(0o600)
    token = secret.read_text().strip()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda n: prepare(n, token), NODES))
    start(1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(start, (3, 4)))
    print(json.dumps({"nodes": NODES, "image": IMAGE, "api": "https://172.16.191.1:16443"}))


if __name__ == "__main__":
    main()
