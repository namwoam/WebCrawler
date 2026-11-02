#!/bin/sh
# Administrative resize for an existing quota-backed local PV (no CSI resizer).
set -eu
pool=/pool
name=${VOLUME_DIRECTORY:?}
size=${NEW_SIZE_BYTES:?}
case "$name" in pvc-*_crawler-poc_crawler-poc-html) ;; *) echo 'Unexpected volume' >&2; exit 1;; esac
exec 9>"$pool/.allocation.lock"
flock -x 9
db="$pool/.allocations.tsv"
project=$(awk -v n="$name" '$1==n {print $3}' "$db")
old=$(awk -v n="$name" '$1==n {print $2}' "$db")
[ -n "$project" ] && [ "$size" -ge "$old" ]
used=$(awk -v n="$name" '$1!=n {s+=$2} END {printf "%.0f",s}' "$db")
total=$(df -B1 --output=size "$pool" | tail -1 | tr -d ' ')
[ $((used+size)) -le "$total" ] || { echo 'Insufficient pool capacity' >&2; exit 1; }
[ -d "$pool/$name" ]
setquota -P "$project" 0 $(((size+1023)/1024)) 0 0 "$pool"
awk -v n="$name" -v size="$size" 'BEGIN {OFS="\t"} $1==n {$2=size} {print $1,$2,$3}' "$db" > "$db.resize"
mv "$db.resize" "$db"
sync "$db"
echo "Quota expanded from $old to $size bytes, project $project"
repquota -P "$pool"
