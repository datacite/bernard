#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_lambda.sh <path/to/lambda_runner.py> [KEY=VALUE]...
EOF
}

[[ $# -ge 1 ]] || { usage; exit 1; }

SCRIPT_PATH="$1"
shift

LAMBDA_DIR="$(dirname "$(realpath "$SCRIPT_PATH")")"
LAMBDA_PATH="$(basename "$SCRIPT_PATH")"

# Defaults for LocalStack use
: "${AWS_ACCESS_KEY_ID:=test}"
: "${AWS_SECRET_ACCESS_KEY:=test}"
: "${AWS_DEFAULT_REGION:=us-east-1}"
: "${AWS_ENDPOINT_URL:=http://localstack:4566}"

export LAMBDA_DIR LAMBDA_PATH
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_ENDPOINT_URL

# Parse trailing KEY=VALUE args
env_args=()
for spec in "$@"; do
  if [[ "$spec" != *=* ]]; then
    echo "Error: invalid argument '$spec' (expected KEY=VALUE)"
    usage
    exit 1
  fi

  key="${spec%%=*}"
  val="${spec#*=}"

  if [[ -z "$key" ]]; then
    echo "Error: invalid assignment '$spec' (empty key)"
    exit 1
  fi

  # Export so compose process sees it, and pass through to container
  export "$key=$val"
  env_args+=(-e "$key")
done

# Always pass required AWS vars explicitly
docker compose run --rm \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_DEFAULT_REGION \
  -e AWS_ENDPOINT_URL \
  "${env_args[@]}" \
  lambda
