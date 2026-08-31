#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT

awk '
  /      - name: Inspect Dependency Dashboard and report health/ { monitor = 1 }
  monitor && index($0, "        run: |") { script = 1; next }
  script && /^          / { sub(/^          /, ""); print; next }
  script && /^$/ { print; next }
  script { exit }
' "$repo_root/.github/workflows/renovate-config-lint.yaml" > "$test_dir/monitor.sh"

# The fixture is literal Markdown, including backticks.
# shellcheck disable=SC2016
dashboard_body='## Repository Problems

These problems occurred while renovating this repository. [View logs](https://developer.mend.io//github/Netcracker/example).

 - ⚠️ WARN: Package lookup failures

## Updates

---

> Renovate failed to look up the following dependencies: `Failed to look up go package example.com/module: no-result`.'

jq -n --arg body "$dashboard_body" '[{
  author: {login: "app/renovate"},
  body: $body,
  number: 34,
  title: "Dependency Dashboard",
  url: "https://github.com/Netcracker/example/issues/34"
}]' > "$test_dir/dashboard.json"

mkdir "$test_dir/bin"
cat > "$test_dir/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1 $2" == "api repos/Netcracker/example" ]]; then
  echo true
elif [[ "$1 $2 $*" == *"issue list"*"--author renovate[bot]"* ]]; then
  cat "$DASHBOARD_JSON"
elif [[ "$1 $2" == "issue list" ]]; then
  echo '[]'
else
  exit 0
fi
EOF
chmod +x "$test_dir/bin/gh"

export DASHBOARD_JSON="$test_dir/dashboard.json"
export GH_REPO='Netcracker/example'
export GITHUB_REPOSITORY='Netcracker/example'
export GITHUB_RUN_ID='1'
export GITHUB_STEP_SUMMARY="$test_dir/summary.md"
export LOOKUP_REASON=''
export LOOKUP_RESULT='success'
export VALIDATION_REASON=''
export VALIDATION_RESULT='success'

run_monitor() {
  rm -f "$GITHUB_STEP_SUMMARY"
  PATH="$test_dir/bin:$PATH" bash "$test_dir/monitor.sh" > "$test_dir/output.log" 2>&1
}

if ! run_monitor; then
  cat "$test_dir/output.log"
  cat "$GITHUB_STEP_SUMMARY"
  echo 'Expected a successful local lookup to override Dashboard lookup warnings.' >&2
  exit 1
fi

grep -q '^Healthy$' "$GITHUB_STEP_SUMMARY"

export LOOKUP_REASON='Local Renovate dependency lookup failed'
export LOOKUP_RESULT='failure'

if run_monitor; then
  echo 'Expected a failed local lookup to keep the health check unhealthy.' >&2
  exit 1
fi

grep -q '^Unhealthy$' "$GITHUB_STEP_SUMMARY"
grep -q '^- Local Renovate dependency lookup failed$' "$GITHUB_STEP_SUMMARY"
