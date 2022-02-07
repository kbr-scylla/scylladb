#!/bin/bash

git ls-files \
    | xargs ls -Fd \
    | grep -vE '\.db$|\.crc32$' \
    | grep -v '^scripts/license.sh$' \
    | grep -v '[/@]$' \
    | sed 's/\*$//' \
    | xargs file --mime-type --no-pad  | sed -n '/: text/ { s/: text.*$//; p}' \
    | xargs sed -i "/SPDX-License-Identifier:/s/AGPL-3.0-or-later/ScyllaDB-Proprietary/"
