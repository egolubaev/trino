#!/usr/bin/env bash
# Build the fork Docker image (trino:481-<arch>) from the current sources.
#
#   ./build-image.sh            # full: install trino-main + server tarball + docker image
#   ./build-image.sh --server   # skip the trino-main install (use when only server/plugin code changed)
#
# Steps:
#   1. install trino-main (+ test-jar) to ~/.m2 so the server assembly and any plugin tests see your changes
#   2. rebuild the trino-server tarball (provisio pulls the fresh trino-main from ~/.m2 -- no full reactor)
#   3. core/docker/build.sh assembles + smoke-tests the image
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

cd "$FORK_ROOT"

if [[ "${1:-}" != "--server" ]]; then
  echo "==> [1/3] installing core/trino-main to ~/.m2"
  ./mvnw -pl core/trino-main install -DskipTests "${MVN_FLAGS[@]}"
fi

echo "==> [2/3] rebuilding trino-server tarball"
./mvnw -pl core/trino-server-core,core/trino-server install -DskipTests "${MVN_FLAGS[@]}"

echo "==> [3/3] building docker image $IMAGE_TAG"
./core/docker/build.sh -a "$ARCH"

echo "==> done. Image: $IMAGE_TAG"
echo "    Next: ./restart-trino.sh   (recreate only the trino service with the new image)"
