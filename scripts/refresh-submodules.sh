#!/bin/bash
#
# Refresh git submodules by fast-forward merging them to the tip of the
# master branch of their respective repositories and committing the
# update with a default commit message of "git submodule summary".
#
# Copyright (C) 2020-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#

set -euo pipefail

# The following is the default list of submodules to refresh. To only refreh
# some of them, pass the list of modules to refresh as arguments. For example,
# "scripts/refresh-submodules.sh seastar tools/java" only refreshes the
# two submodules seastar and tools/java.
submodules=(
    seastar
    tools/jmx
    tools/java
    tools/python3
)

REMOTE=$(git rev-parse --abbrev-ref --symbolic-full-name  @{upstream})
BRANCH=${REMOTE##*/}

for submodule in "${@:-${submodules[@]}}"; do
    GIT_DIR="$submodule/.git" git pull --ff-only origin ${BRANCH}
    SUMMARY=$(git submodule summary $submodule)
    if grep '^ *<' <<< "$SUMMARY"; then
        echo "Non fast-forward changes detected! Fire three red flares from your flare pistol."
        exit 1
    fi
    if [ ! -z "$SUMMARY" ]; then
        git commit --edit -m "Update $submodule submodule" -m "$SUMMARY" $submodule
    fi
done
