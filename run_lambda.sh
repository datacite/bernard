#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_lambda.sh <path/to/lambda_runner.py>

Additional environment variables need to be added to docker-compose.yml
EOF
}

[[ $# -eq 1 ]] || { usage; exit 1; }

SCRIPT_PATH="$1"
[[ -f "$SCRIPT_PATH" ]] || { echo "Error: file not found: $SCRIPT_PATH" >&2; exit 1; }

LAMBDA_DIR="$(dirname "$(realpath "$SCRIPT_PATH")")"
LAMBDA_PATH="$(basename "$SCRIPT_PATH")"
export LAMBDA_DIR LAMBDA_PATH

docker compose run --rm lambda
