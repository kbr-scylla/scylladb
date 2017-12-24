#!/bin/bash

git ls-files \
    | xargs ls -Fd \
    | grep -v '^scripts/license.awk$' \
    | grep -v '[/@]$' \
    | sed 's/\*$//' \
    | xargs awk -i inplace -f scripts/license.awk
