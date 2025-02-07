#!/usr/bin/env bash

## Used in CI. this scripts wraps format_app.py
## and check git diff

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

APPS=()
APPS+=( 'apps/emqx' 'apps/emqx_modules' 'apps/emqx_gateway')
APPS+=( 'apps/emqx_authn' 'apps/emqx_authz' )
APPS+=( 'lib-ee/emqx_enterprise_conf' 'lib-ee/emqx_license' )
APPS+=( 'apps/emqx_exhook')
APPS+=( 'apps/emqx_retainer' 'apps/emqx_slow_subs')
APPS+=( 'apps/emqx_management')

for app in "${APPS[@]}"; do
    echo "$app ..."
    ./scripts/format_app.py -a "$app" -f
done

DIFF_FILES="$(git diff --name-only)"
if [ "$DIFF_FILES" != '' ]; then
    echo "ERROR: Below files need reformat"
    echo "$DIFF_FILES"
    exit 1
fi
