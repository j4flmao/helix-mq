#!/usr/bin/env bash
# Package existing binaries. Release builds set REQUIRE_STATIC=1.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
version="$(cat VERSION)"
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'invalid VERSION' >&2; exit 1; }
case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64) arch=arm64 ;;
    *) echo 'unsupported release architecture' >&2; exit 1 ;;
esac
bin="${BUILD:-build}"
name="helix-mq-$version-linux-$arch"
mkdir -p dist
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/$name/bin" "$stage/$name/docs" "$stage/$name/examples"
for program in helixd helixctl; do
    test -x "$bin/$program"
    if [[ ${REQUIRE_STATIC:-0} == 1 ]]; then
        file "$bin/$program" | grep -q 'statically linked'
    fi
    install -m 755 "$bin/$program" "$stage/$name/bin/$program"
done
"$stage/$name/bin/helixd" --version | grep -Fx "helixd $version"
cp README.md LICENSE CHANGELOG.md VERSION PLAN.md CONTRIBUTING.md SECURITY.md CODE_OF_CONDUCT.md "$stage/$name/"
cp docs/*.md "$stage/$name/docs/"
cp examples/cluster-local.conf "$stage/$name/examples/"
tar --sort=name --mtime="@${SOURCE_DATE_EPOCH:-0}" --owner=0 --group=0 --numeric-owner \
    -C "$stage" -cf - "$name" | gzip -n >"dist/$name.tar.gz"
(cd dist && sha256sum "$name.tar.gz" >"$name.tar.gz.sha256")
echo "dist/$name.tar.gz"
