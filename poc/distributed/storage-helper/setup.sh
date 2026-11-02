#!/bin/sh
# Runs only against the POC filesystem, inside a privileged provisioner helper.
set -eu
parent=$(dirname "$VOL_DIR")
name=$(basename "$VOL_DIR")
case "$parent" in /var/lib/crawler-poc-pv/pool) ;; *) echo "Unexpected pool path" >&2; exit 1 ;; esac
size=${VOL_SIZE_BYTES:?missing requested volume size}
exec 9>"$parent/.allocation.lock"
flock -x 9
db="$parent/.allocations.tsv"
touch "$db"
existing=$(awk -v n="$name" '$1 == n {print $3}' "$db")
if [ -n "$existing" ]; then
    project=$existing
else
    used=$(awk '{s += $2} END {printf "%.0f", s}' "$db")
    total=$(df -B1 --output=size "$parent" | tail -1 | tr -d ' ')
    if [ $((used + size)) -gt "$total" ]; then
        echo "POC pool has insufficient unallocated capacity" >&2
        exit 1
    fi
    project=$(awk 'BEGIN {m=10000} $3 >= m {m=$3+1} END {print m}' "$db")
fi
mkdir -p "$VOL_DIR"
chattr -p "$project" +P "$VOL_DIR"
blocks=$(((size + 1023) / 1024))
setquota -P "$project" 0 "$blocks" 0 0 "$parent"
chmod 0777 "$VOL_DIR"
if [ -z "$existing" ]; then
    printf '%s\t%s\t%s\n' "$name" "$size" "$project" >> "$db"
    sync "$db"
fi
echo "Allocated $size bytes with project quota $project"
