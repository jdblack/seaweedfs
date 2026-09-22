#!/usr/bin/env bash
#
# Usage:
#   ./deploy_image.sh                  # build + push <ver> and <ver>-g<sha>
#   ./deploy_image.sh --no-push        # build + tag locally only
#   ./deploy_image.sh --tag extra      # also push an extra tag
#
# Builds the same image upstream's docker/Dockerfile.go_build produces — /usr/bin/weed
# plus the two Rust binaries /usr/bin/weed-volume and /usr/bin/weed-worker — from this
# working tree, via docker/Dockerfile.fork. That file explains why it is a copy: the
# upstream one git-clones upstream's repo, so it cannot build a fork.
#
# Tags pushed: the moving version alias (e.g. 4.47) plus an immutable tag whose
# suffix is the short git SHA (e.g. 4.47-g2180ca856), so every commit is unique.
# A non-empty TAGS appends the upstream variant suffix to both (5BytesOffset ->
# _large_disk), and picks the matching Rust source image.
# A push refuses to run on a dirty working tree.
#
# Environment overrides:
#   RUNTIME=podman|docker   container runtime       (default: podman)
#   REGISTRY=host           container registry      (default: ghcr.io)
#   PROJECT=name            registry namespace      (default: jdblack)
#   IMAGE=name              repository/image name   (default: jblack-seaweedfs)
#   PLATFORM=linux/amd64    build platform          (default: linux/amd64; arm64 works)
#   VERSION=4.46            override the moving version alias (default: constants.go)
#   TAGS=5BytesOffset       go build tags. Non-empty also suffixes the image tags
#                           (_large_disk) and selects the matching Rust source image,
#                           because OffsetSize 4 vs 5 is an on-disk format: never
#                           mix variants inside one cluster.
#   UPSTREAM_TAG=4.47       upstream release whose image carries the Rust binaries
#   RUST_SOURCE_IMAGE=...   image to copy /usr/bin/weed-{volume,worker} from
#                           (default: chrislusf/seaweedfs:${UPSTREAM_TAG}${SUFFIX})
#   WITH_RUST=1             stage those binaries; 0 leaves the empty placeholders,
#                           which is what a chart >= 4.45 lance sidecar refuses to exec
#   SOURCE_URL=...          OCI source label      (default: the `mine` remote)
#   SKIP_SMOKE=1            skip the post-build container checks
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
TAGS="${TAGS:-}"
UPSTREAM_TAG="${UPSTREAM_TAG:-4.47}"
WITH_RUST="${WITH_RUST:-1}"
SKIP_SMOKE="${SKIP_SMOKE:-0}"

DO_PUSH=1
EXTRA_TAGS=()

usage() {
  # Print the comment header only (stop at the first non-comment line).
  awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "${BASH_SOURCE[0]}"
  exit 0
}

die() { echo "ERROR: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-push|-n)        DO_PUSH=0; shift ;;
    --tag)               EXTRA_TAGS+=("$2"); shift 2 ;;
    -h|--help)           usage ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

CONSTANTS="$ROOT/weed/util/version/constants.go"

[[ -f "$CONSTANTS" ]] || die "$CONSTANTS not found (run from repo root?)"

MAJOR="$(sed -nE 's/^[[:space:]]*MAJOR_VERSION[[:space:]]*=[[:space:]]*int32\(([0-9]+)\).*/\1/p' "$CONSTANTS" | head -1)"
MINOR="$(sed -nE 's/^[[:space:]]*MINOR_VERSION[[:space:]]*=[[:space:]]*int32\(([0-9]+)\).*/\1/p' "$CONSTANTS" | head -1)"
# Moving version alias, e.g. 4.46. Override with VERSION=.
BASE="${VERSION:-$(printf '%d.%02d' "${MAJOR:-0}" "${MINOR:-0}")}"

COMMIT="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"

# ---------------------------------------------------------------------------
# Variant: build tags are compiled into the on-disk format (OffsetSize 4 vs 5), so
# the variant belongs in the tag, not only in the shell that ran this.
# ---------------------------------------------------------------------------
case "$TAGS" in
  "")           SUFFIX="" ;;
  5BytesOffset) SUFFIX="_large_disk" ;;
  *)            SUFFIX="_$(printf '%s' "$TAGS" | tr ',' '_' | tr -cs 'A-Za-z0-9_' '_')" ;;
esac

case "$PLATFORM" in
  */amd64) ARCH="amd64"; FILE_MATCH="x86-64" ;;
  */arm64|*/aarch64) ARCH="arm64"; FILE_MATCH="aarch64" ;;
  *) die "PLATFORM=$PLATFORM: only linux/amd64 and linux/arm64 are supported (upstream prebuilds cover those two)" ;;
esac

# Rust binaries do not come from the release tarballs: those are built for
# *-unknown-linux-gnu and would need gcompat on this alpine base. The copies
# inside upstream's image are the musl ones built for exactly this base.
RUST_SRC="${RUST_SOURCE_IMAGE:-chrislusf/seaweedfs:${UPSTREAM_TAG}${SUFFIX}}"
SOURCE_URL="${SOURCE_URL:-$(git -C "$ROOT" remote get-url mine 2>/dev/null | sed -E 's#^git@github.com:#https://github.com/#; s#\.git$##' || true)}"
SOURCE_URL="${SOURCE_URL:-unknown}"

WORKER_DEST="$ROOT/docker/weed-worker-prebuilt/weed-worker-${ARCH}"
VOLUME_DEST="$ROOT/docker/weed-volume-prebuilt/weed-volume-${ARCH}"

# Immutable, unique-per-commit tag derived from git: <base>-g<short sha>, e.g.
# 4.46-g2180ca856. Each commit gets its own tag without asking a registry.
BUILD="${BASE}${SUFFIX}-g${COMMIT}"

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
IMAGE_TAGS=("${BASE}${SUFFIX}" "$BUILD")   # moving alias + unique per-commit tag
for t in "${EXTRA_TAGS[@]}"; do
  IMAGE_TAGS+=("$t")
done

echo "==> image:      $REF:${IMAGE_TAGS[0]}"
echo "    platform:   $PLATFORM ($ARCH)"
echo "    variant:    TAGS=${TAGS:-<none>}  suffix=${SUFFIX:-<none>}"
echo "    rust:       WITH_RUST=$WITH_RUST  source=${RUST_SRC}"
echo

# ---------------------------------------------------------------------------
# 1. Cross-compile the Go binary for the target arch (CGO off, like the release)
# ---------------------------------------------------------------------------
echo ">> [1/5] go build ./weed (GOOS=linux GOARCH=$ARCH CGO_ENABLED=0${TAGS:+, -tags $TAGS}) -> docker/weed"
TAG_FLAGS=()
[[ -n "$TAGS" ]] && TAG_FLAGS=(-tags "$TAGS")
(
  cd "$ROOT"
  GOOS=linux GOARCH="$ARCH" CGO_ENABLED=0 go build \
    "${TAG_FLAGS[@]}" \
    -ldflags "-s -w -extldflags -static -X github.com/seaweedfs/seaweedfs/weed/util/version.COMMIT=${COMMIT}" \
    -o docker/weed ./weed
)
if ! file "$ROOT/docker/weed" | grep -q "$FILE_MATCH"; then
  echo "ERROR: build did not produce a $ARCH binary" >&2
  file "$ROOT/docker/weed" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 2. Stage the Rust binaries where docker/Dockerfile.fork looks for them
# ---------------------------------------------------------------------------
mkdir -p "$(dirname "$WORKER_DEST")" "$(dirname "$VOLUME_DEST")"

if [[ "$WITH_RUST" -eq 1 ]]; then
  echo ">> [2/5] staging /usr/bin/weed-worker and /usr/bin/weed-volume from ${RUST_SRC}"
  "$RUNTIME" pull --platform "$PLATFORM" "$RUST_SRC" >/dev/null
  TMP_CONTAINER="seaweedfs-rust-src-$$"
  cleanup_container() { "$RUNTIME" rm -f "$TMP_CONTAINER" >/dev/null 2>&1 || true; }
  trap cleanup_container EXIT
  "$RUNTIME" create --platform "$PLATFORM" --name "$TMP_CONTAINER" "$RUST_SRC" >/dev/null
  "$RUNTIME" cp "${TMP_CONTAINER}:/usr/bin/weed-worker" "$WORKER_DEST"
  "$RUNTIME" cp "${TMP_CONTAINER}:/usr/bin/weed-volume" "$VOLUME_DEST"
  cleanup_container
  trap - EXIT

  for pair in "weed-worker:$WORKER_DEST" "weed-volume:$VOLUME_DEST"; do
    name="${pair%%:*}"; path="${pair#*:}"
    size="$(wc -c < "$path" | tr -d ' ')"
    if [[ "$size" -lt 1000000 ]]; then
      die "$name lifted from $RUST_SRC is $size bytes: that is upstream's zero-byte placeholder (its CI skipped this arch)"
    fi
    echo "    $name  ${size} bytes"
  done
else
  echo ">> [2/5] WITH_RUST=0: clearing staged Rust binaries so the placeholders are used"
  rm -f "$WORKER_DEST" "$VOLUME_DEST"
fi

# ---------------------------------------------------------------------------
# 3. Build the image
# ---------------------------------------------------------------------------
echo ">> [3/5] building image for $PLATFORM"
(
  cd "$ROOT/docker"
  "$RUNTIME" build \
    --platform "$PLATFORM" \
    --build-arg "TARGETARCH=${ARCH}" \
    --label "org.opencontainers.image.title=SeaweedFS" \
    --label "org.opencontainers.image.description=SeaweedFS (jdblack fork: EC vacuum, bitrot scan)" \
    --label "org.opencontainers.image.version=${BASE}" \
    --label "org.opencontainers.image.revision=${COMMIT}" \
    --label "org.opencontainers.image.source=${SOURCE_URL}" \
    --label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --label "io.seaweedfs.fork.build-tags=${TAGS:-none}" \
    --label "io.seaweedfs.fork.rust-source=$([[ "$WITH_RUST" -eq 1 ]] && echo "$RUST_SRC" || echo none)" \
    -t "$REF:${IMAGE_TAGS[0]}" -f Dockerfile.fork .
)

for t in "${IMAGE_TAGS[@]}"; do
  if [[ "$t" != "${IMAGE_TAGS[0]}" ]]; then
    "$RUNTIME" tag "$REF:${IMAGE_TAGS[0]}" "$REF:$t"
  fi
done

echo "==> image built:"
for t in "${IMAGE_TAGS[@]}"; do
  "$RUNTIME" image inspect "$REF:$t" --format "    $REF:$t  {{.Os}}/{{.Architecture}}  id={{.Id}}"
done

# ---------------------------------------------------------------------------
# 4. Smoke: the container has to start, and the Rust binaries have to run. The
#    entrypoint always execs /usr/bin/weed, so the Rust checks override it.
# ---------------------------------------------------------------------------
smoke() {
  local image="$1" rc=0 vw out ver
  # Capture first: `run | grep -m1` closes the pipe early, and with `set -o pipefail`
  # that turns podman's SIGPIPE exit into a pipeline failure.
  out="$("$RUNTIME" run --rm --platform "$PLATFORM" "$image" version 2>&1 || true)"
  ver="$(printf '%s\n' "$out" | awk '/^version /{print; exit}')"
  echo "    weed:        ${ver:-(no version line: $(printf '%s' "$out" | head -1))}"
  if [[ "$WITH_RUST" -eq 1 ]]; then
    if "$RUNTIME" run --rm --platform "$PLATFORM" --entrypoint /usr/bin/weed-worker "$image" --help >/dev/null 2>&1; then
      echo "    weed-worker: runs in this image (clap --help exits 0)"
    else
      echo "    weed-worker: FAILED to run (musl/arch mismatch?)" >&2
      rc=1
    fi
    vw="$("$RUNTIME" run --rm --platform "$PLATFORM" --entrypoint /bin/sh "$image" -c 'wc -c < /usr/bin/weed-volume' 2>/dev/null || echo 0)"
    if [[ "${vw:-0}" -gt 1000000 ]]; then
      echo "    weed-volume: present in this image ($vw bytes)"
    else
      echo "    weed-volume: FAILED (placeholder, ${vw:-0} bytes)" >&2
      rc=1
    fi
  fi
  return $rc
}

if [[ "$SKIP_SMOKE" -eq 1 ]]; then
  echo ">> [4/5] SKIPPED smoke (SKIP_SMOKE=1)"
elif smoke "$REF:${IMAGE_TAGS[0]}"; then
  echo ">> [4/5] smoke ok"
else
  if [[ "$DO_PUSH" -eq 1 ]]; then
    die "smoke failed; refusing to push (SKIP_SMOKE=1 overrides)"
  fi
  echo ">> [4/5] WARNING: smoke failed (build is local-only, nothing pushed)"
fi

# ---------------------------------------------------------------------------
# 5. Push
# ---------------------------------------------------------------------------
if [[ "$DO_PUSH" -eq 0 ]]; then
  echo ">> [5/5] SKIPPED push (--no-push)"
  echo "Done. Push later with:  ./deploy_image.sh   (or: $RUNTIME push $REF:${IMAGE_TAGS[0]})"
  exit 0
fi

echo ">> [5/5] pushing to $REF"
for t in "${IMAGE_TAGS[@]}"; do
  echo "    -> $REF:$t"
  "$RUNTIME" push "$REF:$t"
done

echo
echo "Done. Verify:"
echo "    $RUNTIME run --rm --platform $PLATFORM $REF:${IMAGE_TAGS[0]} version"
if [[ "$WITH_RUST" -eq 1 ]]; then
  echo "    $RUNTIME run --rm --platform $PLATFORM --entrypoint /usr/bin/weed-worker $REF:${IMAGE_TAGS[0]} --help"
fi


