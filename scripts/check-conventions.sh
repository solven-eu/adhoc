#!/usr/bin/env bash
# Run all convention checkers over the codebase.
# Each checker is informational: the overall exit code is non-zero if any checker reports violations.
# See CONVENTIONS.MD for the rules each script enforces.
#
# Usage:  ./scripts/check-conventions.sh
#         Scans all src/main/java trees under the current directory.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Discover all src/main/java trees under cwd (covers any module, present or future).
# Use a portable while-read loop compatible with bash and zsh.
SRCDIRS=()
while IFS= read -r d; do SRCDIRS+=("$d"); done < <(find . -type d -path '*/src/main/java' | sort)

if [ "${#SRCDIRS[@]}" -eq 0 ]; then
    echo "No src/main/java directories found under $(pwd)." >&2
    exit 1
fi

total_violations=0

run_checker() {
    local name="$1"
    local script="$2"
    echo "=== $name ==="
    perl "$script" "${SRCDIRS[@]}" || total_violations=$((total_violations + $?))
    echo ""
}

run_checker "Stepdown Rule"      "$SCRIPT_DIR/check-convention-stepdown.pl"
run_checker "Linked collections" "$SCRIPT_DIR/check-convention-linked.pl"
run_checker "Guava collections"  "$SCRIPT_DIR/check-convention-guava.pl"
run_checker "Logging"            "$SCRIPT_DIR/check-convention-logging.pl"

# Informational only — always exits 0. Printed so the AI agent and reviewers can
# see outstanding TODOs without blocking CI.
echo "=== TODO report ==="
perl "$SCRIPT_DIR/report-todos.pl" "${SRCDIRS[@]}"
echo ""

if [ "$total_violations" -gt 0 ]; then
    echo "Total: $total_violations convention violation(s) found across all checkers."
    exit 1
else
    echo "All convention checks passed."
fi
