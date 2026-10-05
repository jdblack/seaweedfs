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
#   WITH_RUST=0             DEFAULT. /usr/bin/weed-volume and /usr/bin/weed-worker stay
#                           the zero-byte placeholders upstream's builder writes; nothing
#                           is built or fetched. Safe because nothing here execs them:
#                           the Rust volume server is opt-in and test-only, and a chart
#                           >= 4.45 lance sidecar is kept out with s3.lancePort=0
#                           (worker.namespaceUrl must stay empty too).
#   WITH_RUST=1             build the two Rust binaries from THIS TREE and include them.
#                           Never anything from an upstream image: weed-volume must carry
#                           this fork's master.proto fields (data_shards/parity_shards)
#                           or it reports no per-volume EC ratio. Slow: the lance worker
#                           compiles arrow/datafusion.
#                           (RUST=1 is accepted as an alias.)
#   RUST_BIN_DIR=...        where those built binaries live, named weed-worker-<arch> and
#                           weed-volume-<arch> (default: docker/rust-prebuilt). Reused
#                           when they are newer than the Rust sources; point it at
#                           CI-built artifacts to skip the local build.
#   RUST_BUILD_IMAGE=...    builder image (default rust:1.98.1-alpine3.23: rust 1.98.1 +
#                           cargo + the musl toolchain preinstalled, >= the workspace's
#                           rust-version 1.94.1; only musl-dev/protobuf-dev/cmake/build-base
#                           are added). When PLATFORM is not the host arch the compile runs
#                           emulated, which is slow; on a native amd64 host or arm64 Mac it
#                           runs at full speed.
#   RUST_REGISTRY_VOLUME=   named volume caching the cargo registry (default
#                           seaweedfs-rust-cache), so dependencies are downloaded once
#   RUST_TARGET_VOLUME=     named volume caching compiled dependencies (default
#                           seaweedfs-rust-target), so arrow/datafusion is not rebuilt
#                           from scratch on every run
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
WITH_RUST="${WITH_RUST:-${RUST:-1}}"
SKIP_SMOKE="${SKIP_SMOKE:-0}"

DO_PUSH=1
EXTRA_TAGS=()

usage() {
  # Print the comment header only (stop at the first non-comment line).
  awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "${BASH_SOURCE[0]}"
  exit 0
}

die() { echo "ERROR: $*" >&2; exit 1; }

sha256_of() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
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

# Rust binaries are ours: built from this tree, musl-linked for this alpine base.
# Nothing is ever lifted from an upstream image.
RUST_BIN_DIR="${RUST_BIN_DIR:-$ROOT/docker/rust-prebuilt}"
RUST_BUILD_IMAGE="${RUST_BUILD_IMAGE:-docker.io/library/rust:1.98.1-alpine3.23}"
# Named volumes: the cargo registry and the compiled dependencies survive between
# runs, so arrow/datafusion is not recompiled from scratch every time. The registry
# holds sources (arch-independent, safe to share); compiled artifacts are per-arch,
# so that one is separate per target to avoid cross-arch thrash.
RUST_REGISTRY_VOLUME="${RUST_REGISTRY_VOLUME:-seaweedfs-rust-cache}"
RUST_TARGET_VOLUME="${RUST_TARGET_VOLUME:-seaweedfs-rust-target-${ARCH}}"

# WITH_RUST is one switch: 1 builds both binaries from this tree and includes them,
# 0 (the default) leaves the placeholders.
case "${WITH_RUST,,}" in
  0|no|none|false) WITH_RUST=0 ;;
  1|yes|true|build|stage) WITH_RUST=1 ;;
  *) die "unknown WITH_RUST=$WITH_RUST (expected 0 or 1)" ;;
esac
SOURCE_URL="${SOURCE_URL:-$(git -C "$ROOT" remote get-url mine 2>/dev/null | sed -E 's#^git@github.com:#https://github.com/#; s#\.git$##' || true)}"
SOURCE_URL="${SOURCE_URL:-unknown}"

WORKER_SRC="$RUST_BIN_DIR/weed-worker-${ARCH}"
VOLUME_SRC="$RUST_BIN_DIR/weed-volume-${ARCH}"
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
echo "    rust:       WITH_RUST=$WITH_RUST$([[ "$WITH_RUST" -eq 1 ]] && echo "  build from this tree -> $RUST_BIN_DIR" || echo "  placeholders; nothing built or fetched")"
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
# 2. Stage the Rust binaries — OURS, built from this tree, or the placeholders.
#    Nothing is ever lifted from an upstream image.
# ---------------------------------------------------------------------------
mkdir -p "$(dirname "$WORKER_DEST")" "$(dirname "$VOLUME_DEST")"

build_rust_binaries() {
  # Builder is the official rust image, not bare alpine: rust 1.98.1 + cargo + the
  # musl toolchain come preinstalled, so there is no multi-minute `apk add rust`
  # (which crashed QEMU here under emulation). Only the aux packages are installed:
  # musl-dev/build-base/cmake for aws-lc-sys (the C+asm behind tls-aws-lc), protoc
  # for tonic-prost-build.
  #
  # The registry and the target dir live in named volumes, so arrow/datafusion is
  # compiled once and reused by later runs instead of being rebuilt from scratch.
  local vol_build
  if [[ "$TAGS" == *5BytesOffset* ]]; then
    vol_build="cargo build --release"
  else
    vol_build="cargo build --release --no-default-features"
  fi
  mkdir -p "$RUST_BIN_DIR"
  echo "    building weed-worker + weed-volume from this tree in $RUST_BUILD_IMAGE"
  echo "    (registry cache: $RUST_REGISTRY_VOLUME, target cache: $RUST_TARGET_VOLUME)"
  "$RUNTIME" run --rm --platform "$PLATFORM" \
    -v "$ROOT":/src:ro \
    -v "$RUST_BIN_DIR":/out \
    -v "$RUST_REGISTRY_VOLUME":/usr/local/cargo/registry \
    -v "$RUST_TARGET_VOLUME":/target \
    -e CARGO_TARGET_DIR=/target \
    "$RUST_BUILD_IMAGE" sh -euxc "
      apk add --no-cache musl-dev protobuf-dev cmake build-base perl
      mkdir -p /build/weed/util/version
      cp -a /src/seaweed-worker /src/seaweed-volume /src/seaweed-common /build/
      # Both crates reach into the Go tree, so the copies above sit beside /build/weed
      # for these relative paths to resolve:
      #   crates/core/build.rs   -> ../../../weed/pb/plugin.proto   (the wire contract)
      #   build.rs               -> ../weed/pb/filer.proto
      #   src/server/ui.rs       -> ../weed/static/**              (embedded UI assets)
      #   src/version.rs         -> ../weed/util/version/constants.go
      # If a crate starts embedding more of weed/, this list has to grow — the build
      # fails loudly, naming the missing path.
      cp -a /src/weed/pb /build/weed/pb
      cp -a /src/weed/static /build/weed/static
      cp -a /src/weed/util/version/constants.go /build/weed/util/version/
      cd /build/seaweed-worker && cargo build --release -p weed-lance-worker
      cd /build/seaweed-volume && $vol_build
      cp /target/release/weed-worker /out/weed-worker-$ARCH
      cp /target/release/weed-volume /out/weed-volume-$ARCH
    "
}

RUST_LABEL="none"

if [[ "$WITH_RUST" -eq 0 ]]; then
  echo ">> [2/5] WITH_RUST=0: placeholders (nothing built, nothing fetched)"
  rm -f "$WORKER_DEST" "$VOLUME_DEST"
else
  echo ">> [2/5] WITH_RUST=1: building/using our weed-worker and weed-volume for $ARCH in $RUST_BIN_DIR"

  # Rebuild when either binary is missing, or when the Rust sources are newer than
  # them — silently reusing stale artifacts would bake an old worker into the image.
  rust_needs_build() {
    [[ -s "$WORKER_SRC" && -s "$VOLUME_SRC" ]] || return 0
    [[ -n "$(find "$ROOT/seaweed-worker" "$ROOT/seaweed-volume" "$ROOT/seaweed-common" \
      -type f \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'Cargo.lock' \) \
      -newer "$WORKER_SRC" -print -quit 2>/dev/null)" ]]
  }

  if rust_needs_build; then
    build_rust_binaries || die "the Rust build failed (see the output above). Check the builder image and network access; alternatively build on a native $ARCH host and drop the artifacts into $RUST_BIN_DIR."
  else
    echo "    reusing the binaries already in $RUST_BIN_DIR (newer than the Rust sources)"
  fi

  RUST_SHAS=()
  for pair in "weed-worker:$WORKER_SRC:$WORKER_DEST" "weed-volume:$VOLUME_SRC:$VOLUME_DEST"; do
    name="${pair%%:*}"; rest="${pair#*:}"; src="${rest%%:*}"; dest="${rest#*:}"
    [[ -s "$src" ]] || die "$name not found at $src after the build. These are deliberately never fetched from an upstream image: weed-volume has to carry this fork's master.proto fields or it reports no per-volume EC ratio."
    file "$src" | grep -q 'ELF' || die "$name at $src is not an ELF binary ($(file -b "$src" | cut -c1-60)); it must be a linux/$ARCH build for this alpine image"
    file "$src" | grep -q "$FILE_MATCH" || die "$name at $src is not built for $ARCH"
    install -m 0755 "$src" "$dest"
    sha="$(sha256_of "$src")"
    RUST_SHAS+=("$name=${sha}")
    echo "    $name  $(wc -c < "$src" | tr -d ' ') bytes  sha256=${sha:0:12}...  $(basename "$src")"
  done
  RUST_LABEL="$(IFS=,; echo "${RUST_SHAS[*]}")"
fi

# ---------------------------------------------------------------------------
# 3. Build the image
#
# Every tag is applied by the build itself (`-t` repeated) and the image id comes
# from `--iidfile`, so nothing depends on resolve-then-retag. That matters: an
# untag/tag dance here silently left `:4.47` missing on this podman, and earlier a
# build that reported tagging `:4.47` left the tag on an older image — either way a
# push could ship the wrong bytes. The loop after the build asserts each tag really
# resolves to the id we just built, and dies rather than hand over a mislabelled tag.
# ---------------------------------------------------------------------------
echo ">> [3/5] building image for $PLATFORM"
IIDFILE="$(mktemp)"
BUILD_FLAGS=(-f Dockerfile.fork .)
for t in "${IMAGE_TAGS[@]}"; do
  BUILD_FLAGS=(-t "$REF:$t" "${BUILD_FLAGS[@]}")
done
(
  cd "$ROOT/docker"
  "$RUNTIME" build \
    --platform "$PLATFORM" \
    --build-arg "TARGETARCH=${ARCH}" \
    --iidfile "$IIDFILE" \
    --label "org.opencontainers.image.title=SeaweedFS" \
    --label "org.opencontainers.image.description=SeaweedFS (jdblack fork: EC vacuum, bitrot scan)" \
    --label "org.opencontainers.image.version=${BASE}" \
    --label "org.opencontainers.image.revision=${COMMIT}" \
    --label "org.opencontainers.image.source=${SOURCE_URL}" \
    --label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --label "io.seaweedfs.fork.build-tags=${TAGS:-none}" \
    --label "io.seaweedfs.fork.rust-binaries=${RUST_LABEL}" \
    "${BUILD_FLAGS[@]}"
)

IMAGE_ID="$(tr -d '[:space:]' < "$IIDFILE" | sed 's/^sha256://')"
rm -f "$IIDFILE"
[[ -n "$IMAGE_ID" ]] || die "the build did not report an image id"

echo "==> image built:"
for t in "${IMAGE_TAGS[@]}"; do
  got="$("$RUNTIME" image inspect "$REF:$t" --format '{{.Id}}' 2>/dev/null || true)"
  [[ "$got" == "$IMAGE_ID" ]] || die "$REF:$t resolves to '${got:-nothing}', expected $IMAGE_ID"
  # No early exit in the awk: it would SIGPIPE the producer and, under pipefail,
  # turn a successful run into exit 141.
  images_out="$("$RUNTIME" images --format '{{.ID}} {{.Repository}}:{{.Tag}}' 2>/dev/null || true)"
  listed="$(printf '%s\n' "$images_out" | awk -v r="$REF:$t" '$2 == r { found = $1 } END { print found }')"
  echo "    $REF:$t  linux/$ARCH  id=${got:0:12}"
  if [[ -n "$listed" && "$listed" != "${IMAGE_ID:0:12}" ]]; then
    echo "      NOTE: 'podman images' lists this tag as $listed (stale local record); the push goes by id"
  fi
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
  ver="$(printf '%s\n' "$out" | awk '/^version /{ found = $0 } END { print found }')"
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
elif smoke "$IMAGE_ID"; then
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

# Push by IMAGE_ID, not by tag: on this machine `podman images`/`podman run` can
# resolve a tag to a different (older) image than `podman image inspect` reports,
# and a push by tag would then ship the wrong bytes. A push by id to a destination
# reference is unambiguous.
echo ">> [5/5] pushing to $REF"
for t in "${IMAGE_TAGS[@]}"; do
  echo "    -> $REF:$t"
  "$RUNTIME" push "$IMAGE_ID" "$REF:$t"
done

echo
echo "Done. Verify:"
echo "    $RUNTIME run --rm --platform $PLATFORM $REF:${IMAGE_TAGS[0]} version"
if [[ "$WITH_RUST" -eq 1 ]]; then
  echo "    $RUNTIME run --rm --platform $PLATFORM --entrypoint /usr/bin/weed-worker $REF:${IMAGE_TAGS[0]} --help"
fi


