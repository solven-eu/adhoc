#!/usr/bin/env bash
#
# Release driver for adhoc.
#
# Reads the SNAPSHOT version from the root pom, checks that the corresponding
# release tag does not yet exist (locally or on origin), creates a dedicated
# `release/v<version>` branch off master, and runs the maven-release-plugin
# from there. The branch is then ready to be opened as a PR against the
# protected master branch so the release commits and SNAPSHOT bump land via
# the normal review flow.
#
# Usage:
#   ./scripts/release.sh
#
# Preconditions checked:
#   1. Current branch is master.
#   2. Working tree is clean (no modified or untracked files).
#   3. Local master matches origin/master (no missing pull / no extra commits).
#   4. Root pom version ends in -SNAPSHOT.
#   5. No tag v<release> exists locally or on origin.
#   6. No branch release/v<release> exists locally or on origin.
#
# After preconditions pass the script prompts once before invoking the
# (irreversible) `mvn release:prepare release:perform` sequence.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${REPO_ROOT}"

info()  { printf "\033[1;34m[release]\033[0m %s\n" "$*"; }
fail()  { printf "\033[1;31m[release]\033[0m %s\n" "$*" >&2; exit 1; }

# 1. Branch must be master.
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
[[ "${CURRENT_BRANCH}" == "master" ]] || fail "Not on master (current: ${CURRENT_BRANCH})."

# 2. Working tree must be clean.
[[ -z "$(git status --porcelain)" ]] || fail "Working tree has uncommitted or untracked changes."

# 3. Local master must match origin/master.
info "Fetching from origin (heads + tags)..."
git fetch --tags origin

LOCAL_HEAD=$(git rev-parse HEAD)
REMOTE_HEAD=$(git rev-parse origin/master)
[[ "${LOCAL_HEAD}" == "${REMOTE_HEAD}" ]] \
    || fail "Local master (${LOCAL_HEAD:0:10}) differs from origin/master (${REMOTE_HEAD:0:10})."

# 4. Read SNAPSHOT version from root pom.
SNAPSHOT_VERSION=$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout --non-recursive)
info "Root pom version: ${SNAPSHOT_VERSION}"
[[ "${SNAPSHOT_VERSION}" == *-SNAPSHOT ]] \
    || fail "Root pom version is not a SNAPSHOT (got: ${SNAPSHOT_VERSION})."

RELEASE_VERSION="${SNAPSHOT_VERSION%-SNAPSHOT}"
RELEASE_TAG="v${RELEASE_VERSION}"
RELEASE_BRANCH="release/${RELEASE_TAG}"
info "Release version : ${RELEASE_VERSION}"
info "Release tag     : ${RELEASE_TAG}"
info "Release branch  : ${RELEASE_BRANCH}"

# 5. Release tag must not exist anywhere.
if git rev-parse -q --verify "refs/tags/${RELEASE_TAG}" >/dev/null; then
    fail "Local tag ${RELEASE_TAG} already exists."
fi
if git ls-remote --exit-code --tags origin "refs/tags/${RELEASE_TAG}" >/dev/null 2>&1; then
    fail "Remote tag ${RELEASE_TAG} already exists on origin."
fi

# 6. Release branch must not exist anywhere.
if git rev-parse -q --verify "refs/heads/${RELEASE_BRANCH}" >/dev/null; then
    fail "Local branch ${RELEASE_BRANCH} already exists."
fi
if git ls-remote --exit-code --heads origin "refs/heads/${RELEASE_BRANCH}" >/dev/null 2>&1; then
    fail "Remote branch ${RELEASE_BRANCH} already exists on origin."
fi

# Confirm before doing anything irreversible.
echo
read -r -p "About to release ${RELEASE_VERSION} from a fresh branch ${RELEASE_BRANCH}. Proceed? [y/N] " REPLY
[[ "${REPLY,,}" == "y" || "${REPLY,,}" == "yes" ]] || fail "Aborted by user."

# 7. Create + push the release branch. Pushing first lets the maven-release-plugin
#    push its prepare commits (release commit, next-SNAPSHOT commit, tag) onto an
#    already-tracked branch instead of trying to create one.
info "Creating branch ${RELEASE_BRANCH}..."
git checkout -b "${RELEASE_BRANCH}"

info "Pushing branch ${RELEASE_BRANCH} so subsequent release commits track origin..."
git push --set-upstream origin "${RELEASE_BRANCH}"

# 8. Drive the release.
info "Running mvn release:clean release:prepare release:perform..."
mvn release:clean release:prepare -Pfast release:perform -Pactiveviam --batch-mode

info "Release ${RELEASE_VERSION} complete."
info "Tag ${RELEASE_TAG} and the next-SNAPSHOT bump live on branch ${RELEASE_BRANCH}."
info "Open a pull request from ${RELEASE_BRANCH} to master so the bump lands through review."
