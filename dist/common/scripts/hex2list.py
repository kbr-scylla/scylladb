#!/usr/bin/python3
#
# Copyright 2016 ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#
#
# hex2list.py is cpuset format converter (see cpuset(7)).
# It reads "Mask Format" parameter from stdin,
# outputs "List Format" parameter to stdout.
#
# Here's example to convert '00000000,000e3862' to List Format:
# $ echo 00000000,000e3862 | hex2list.py
# 1,5-6,11-13,17-19
#

from scylla_util import hex2list

print(hex2list(input()))
