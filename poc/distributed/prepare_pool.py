"""Reserve and format a new, isolated 300Gi POC filesystem; never format host disks."""
import json
import pathlib
import shutil
import subprocess
import sys

node = int(sys.argv[1])
assert node in (3, 4)
root = pathlib.Path(f"/data/crawler-poc-r14725077/node-{node}")
image = root / "pool-300Gi.img"
size = 300 * 1024**3
label = f"CRAWLER_POC_{node}"
if image.exists():
    assert image.stat().st_size == size, "Existing image size differs; refusing overwrite"
    info = subprocess.check_output(["/usr/sbin/blkid", "-p", "-o", "export", str(image)], text=True)
    assert f"LABEL={label}" in info and "TYPE=ext4" in info, "Unexpected filesystem; refusing overwrite"
else:
    assert shutil.disk_usage(root).free > size + 50 * 1024**3, "Insufficient free space"
    subprocess.run(["fallocate", "-l", str(size), str(image)], check=True)
    image.chmod(0o600)
    subprocess.run(["/usr/sbin/mkfs.ext4", "-F", "-q", "-m", "0", "-L", label,
                    "-O", "project,quota", "-E",
                    "quotatype=prjquota,nodiscard,lazy_itable_init=1,lazy_journal_init=1", str(image)], check=True)
(root / "pool").mkdir(exist_ok=True)
print(json.dumps({"node": node, "image": str(image), "capacity_bytes": size,
                  "reserved_bytes": image.stat().st_blocks * 512, "filesystem": "ext4", "quota": "project"}))
