#!/usr/bin/env bash

## This script helps to download relup base version packages

if [[ -n "$DEBUG" ]]; then set -x; fi
set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

PROFILE="${1:-}"
if [ "$PROFILE" = "" ]; then
    PROFILE="emqx"
fi

OTP_VSN=${OTP_VSN:-$(scripts/get-otp-vsn.sh)}

case $PROFILE in
    "emqx")
        DIR='broker'
        EDITION='community'
        ;;
    "emqx-ee")
        DIR='enterprise'
        EDITION='enterprise'
        ;;
    "emqx-edge")
        DIR='edge'
        EDITION='edge'
        ;;
esac

SYSTEM="${SYSTEM:-$(./scripts/get-distro.sh)}"

ARCH="${ARCH:-$(uname -m)}"
case "$ARCH" in
    x86_64)
        ARCH='amd64'
        ;;
    aarch64)
        ARCH='arm64'
        ;;
    arm*)
        ARCH=arm
        ;;
esac

SHASUM="sha256sum"
if [ "$SYSTEM" = "macos" ]; then
    SHASUM="shasum -a 256"
fi

mkdir -p _upgrade_base
pushd _upgrade_base >/dev/null

for tag in $(../scripts/relup-base-vsns.sh $EDITION | xargs echo -n); do
    filename="$PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.tar.gz"
    url="https://www.emqx.com/downloads/$DIR/$tag/$filename"
    echo "downloading ${filename} ..."
    ## if the file does not exist (not downloaded yet)
    ## and there is such a package to downlaod
    if [ ! -f "$filename" ] && curl -I -m 10 -o /dev/null -s -w "%{http_code}" "${url}" | grep -q -oE "^[23]+" ; then
        curl -L -o "${filename}" "${url}"
        curl -L -o "${filename}.sha256" "${url}.sha256"
        ## https://askubuntu.com/questions/1202208/checking-sha256-checksum
        echo "$(cat "${filename}.sha256")  ${filename}" | $SHASUM -c || exit 1
    fi
done

popd >/dev/null
