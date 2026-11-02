#!/bin/sh
set -eu
root=/var/lib/crawler-poc-pv
mkdir -p "$root/pool"
if ! grep -qs " $root/pool " /proc/mounts; then
    mount -t ext4 -o loop,prjquota,noatime "$root/pool-300Gi.img" "$root/pool"
fi
exec /bin/k3s "$@"
