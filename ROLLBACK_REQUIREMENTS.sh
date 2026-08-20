#!/usr/bin/env bash
set -euo pipefail
TARGET="${1:-requirements.txt}"
cp "${TARGET}.orig" "$TARGET"
printf 'rollback restored %s from %s\n' "$TARGET" "${TARGET}.orig"