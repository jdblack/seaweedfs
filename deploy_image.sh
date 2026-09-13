#!/usr/bin/env bash
#
# Usage:
#   ./deploy_image.sh                  # build + push <ver> and <ver>-g<sha>
#   ./deploy_image.sh --no-push        # build + tag locally only
#   ./deploy_image.sh --tag extra      # also push an extra tag
#
# Tags pushed: the moving version alias (e.g. 4.46) plus an immutable tag whose
# suffix is the short git SHA (e.g. 4.46-g2180ca856), so every commit is unique.
# A push refuses to run on a dirty working tree.
#
# Environment overrides:
#   RUNTIME=podman|docker   container runtime       (default: podman)
#   REGISTRY=host           container registry      (default: ghcr.io)
#   PROJECT=name            registry namespace      (default: jdblack)
#   IMAGE=name              repository/image name   (default: jblack-seaweedfs)
#   PLATFORM=linux/amd64    build platform          (default: linux/amd64)
#   VERSION=4.46            override the moving version alias (default: constants.go)
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME="${RUNTIME:-podman}"
REGISTRY="${REGISTRY:-ghcr.io}"
PROJECT="${PROJECT:-jdblack}"
IMAGE="${IMAGE:-jblack-seaweedfs}"
PLATFORM="${PLATFORM:-linux/amd64}"

DO_PUSH=1
EXTRA_TAGS=()

usage() {
  # Print the comment header only (stop at the first non-comment line).
  awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "${BASH_SOURCE[0]}"
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-push|-n)        DO_PUSH=0; shift ;;
    --tag)               EXTRA_TAGS+=("$2"); shift 2 ;;
    -h|--help)           usage ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

CONSTANTS="$ROOT/weed/util/version/constants.go"

[[ -f "$CONSTANTS" ]] || { echo "ERROR: $CONSTANTS not found (run from repo root?)" >&2; exit 1; }

MAJOR="$(sed -nE 's/^[[:space:]]*MAJOR_VERSION[[:space:]]*=[[:space:]]*int32\(([0-9]+)\).*/\1/p' "$CONSTANTS" | head -1)"
MINOR="$(sed -nE 's/^[[:space:]]*MINOR_VERSION[[:space:]]*=[[:space:]]*int32\(([0-9]+)\).*/\1/p' "$CONSTANTS" | head -1)"
# Moving version alias, e.g. 4.46. Override with VERSION=.
BASE="${VERSION:-$(printf '%d.%02d' "${MAJOR:-0}" "${MINOR:-0}")}"

COMMIT="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"

# Immutable, unique-per-commit tag derived from git: <base>-g<short sha>, e.g.
# 4.46-g2180ca856. Each commit gets its own tag without asking a registry.
BUILD="${BASE}-g${COMMIT}"

# A dirty tree means the tag would not describe the bytes being built. Refuse to
# push; --no-push still allows a local build, marked with a -dirty suffix.
CLEAN=1
if [[ -n "$(git -C "$ROOT" status --porcelain)" ]]; then
  CLEAN=0
  BUILD="${BUILD}-dirty"
fi
if [[ "$CLEAN" -eq 0 ]]; then
  if [[ "$DO_PUSH" -eq 1 ]]; then
    echo "ERROR: refusing to push: working tree is not clean (commit or stash first):" >&2
    git -C "$ROOT" status --short >&2
    exit 1
  fi
  echo "NOTE: working tree is not clean; building a local-only (-dirty) image (--no-push)."
  git -C "$ROOT" status --short
  echo
fi

REF="${REGISTRY}/${PROJECT}/${IMAGE}"
TAGS=("$BASE" "$BUILD")         # moving alias + unique per-commit tag
for t in "${EXTRA_TAGS[@]}"; do
  TAGS+=("$t")
done

# ---------------------------------------------------------------------------
# 1. Cross-compile the Go binary for linux/amd64 (CGO off, like the release)
# ---------------------------------------------------------------------------
echo ">> [1/3] go build ./weed (GOOS=linux GOARCH=amd64 CGO_ENABLED=0) -> docker/weed"
(
  cd "$ROOT"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/seaweedfs/seaweedfs/weed/util/version.COMMIT=${COMMIT}" \
    -o docker/weed ./weed
)
if ! file "$ROOT/docker/weed" | grep -q 'x86-64'; then
  echo "ERROR: build did not produce an x86-64 binary" >&2
  file "$ROOT/docker/weed" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 2. Build the image (Go-only, Dockerfile.local)
# ---------------------------------------------------------------------------
echo ">> [2/3] building image for $PLATFORM"
(
  cd "$ROOT/docker"
  "$RUNTIME" build --platform "$PLATFORM" -t "$REF:${TAGS[0]}" -f Dockerfile.local .
)

for t in "${TAGS[@]}"; do
  if [[ "$t" != "${TAGS[0]}" ]]; then
    "$RUNTIME" tag "$REF:${TAGS[0]}" "$REF:$t"
  fi
done

echo "==> image built:"
for t in "${TAGS[@]}"; do
  "$RUNTIME" image inspect "$REF:$t" --format "    $REF:$t  {{.Os}}/{{.Architecture}}  id={{.Id}}"
done

# ---------------------------------------------------------------------------
# 3. Push
# ---------------------------------------------------------------------------
if [[ "$DO_PUSH" -eq 0 ]]; then
  echo ">> [3/3] SKIPPED push (--no-push)"
  echo "Done. Push later with:  ./deploy_image.sh   (or: $RUNTIME push $REF:${TAGS[0]})"
  exit 0
fi

echo ">> [3/3] pushing to $REF"
for t in "${TAGS[@]}"; do
  echo "    -> $REF:$t"
  "$RUNTIME" push "$REF:$t"
done

echo
echo "Done. Verify:"
echo "    $RUNTIME run --rm --platform $PLATFORM $REF:${TAGS[0]} version"

