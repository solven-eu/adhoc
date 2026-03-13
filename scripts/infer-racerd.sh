#!/bin/bash
# Run Infer RacerD race-condition analysis locally.
#
# Prerequisites (see CONVENTIONS.MD §Infer/RacerD for full instructions):
#   macOS/Linux: download the pre-built binary from
#     https://github.com/facebook/infer/releases
#   then add /opt/infer/bin to your PATH.
#
# Usage:
#   ./scripts/infer-racerd.sh            # analyse whole project
#   ./scripts/infer-racerd.sh -pl adhoc  # analyse one module
#
# Results are written to infer-out/ in the current directory.
# https://fbinfer.com/docs/next/checker-racerd

set -euo pipefail

if ! command -v infer &>/dev/null; then
  echo "ERROR: 'infer' not found. Install it first:" >&2
  echo "  macOS/Linux: download from https://github.com/facebook/infer/releases" >&2
  echo "               then add /opt/infer/bin to your PATH." >&2
  echo "  Linux:  see CONVENTIONS.MD §Infer/RacerD" >&2
  exit 1
fi

echo "==> Running Infer RacerD ($(infer --version | head -1))..."
infer run --racerdonly -- \
  mvn compile -Pfast --file pom.xml --batch-mode "$@"

echo ""
echo "==> Infer RacerD complete. Printing report:"
infer report
