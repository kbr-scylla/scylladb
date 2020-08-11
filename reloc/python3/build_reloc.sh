#!/bin/bash -e
#
# Copyright (C) 2019 ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#

TARGET=build/release/scylla-python3-package.tar.gz

if [ -f "$TARGET" ]; then
    rm "$TARGET"
fi

./SCYLLA-VERSION-GEN
mkdir -p build/python3
PYVER=$(python3 -V | cut -d' ' -f2)
echo "$PYVER" > build/python3/SCYLLA-VERSION-FILE
ln -fv build/SCYLLA-RELEASE-FILE build/python3/SCYLLA-RELEASE-FILE
./dist/debian/python3/debian_files_gen.py

PACKAGES="python3-pyyaml python3-urwid python3-pyparsing python3-requests python3-pyudev python3-setuptools python3-psutil python3-distro python3-psutil"
./scripts/create-relocatable-package-python3.py --output "$TARGET" $PACKAGES
