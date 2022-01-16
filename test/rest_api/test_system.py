# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

def test_system_uptime_ms(rest_api):
    resp = rest_api.send('GET', "system/uptime_ms")
    resp.raise_for_status()
