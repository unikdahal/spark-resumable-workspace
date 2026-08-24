#!/usr/bin/env bash
# Clone the active upstream forks listed in repos.yaml.
# Does not use git submodules. Existing checkouts are left untouched.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
YAML="$ROOT/workspace/repos.yaml"
ROLE_FILTER="${1:-active-upstream}"

python3 - "$YAML" "$ROOT" "$ROLE_FILTER" <<'PY'
import os, subprocess, sys

yaml_path, root, role_filter = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(yaml_path).read()
repos, cur = [], None
for raw in text.splitlines():
    line = raw.rstrip()
    if line.startswith("  - id:"):
        if cur:
            repos.append(cur)
        cur = {"id": line.split(":", 1)[1].strip()}
    elif cur is not None and line.startswith("    ") and ":" in line:
        key, val = line.strip().split(":", 1)
        cur[key.strip()] = val.strip()
if cur:
    repos.append(cur)

wanted = [r for r in repos if r.get("role") == role_filter]
if not wanted:
    sys.exit(f"no repos with role={role_filter}")

for r in wanted:
    path = os.path.join(root, r["local_path"])
    url, commit = r["url"], r["commit"]
    if os.path.isdir(os.path.join(path, ".git")) or os.path.isfile(os.path.join(path, ".git")):
        print(f"present  {r['local_path']}  (left as-is)")
        continue
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    print(f"clone    {r['local_path']}  {url}")
    subprocess.check_call(["git", "clone", url, path])
    subprocess.check_call(["git", "-C", path, "checkout", "--detach", commit])
    print(f"pinned   {r['local_path']}  {commit}")
PY
