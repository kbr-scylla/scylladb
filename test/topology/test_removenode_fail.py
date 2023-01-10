#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
import logging
import asyncio
import random

from test.pylib.rest_client import inject_error

@pytest.mark.asyncio
#@pytest.mark.xfail
async def test_majority_loss(manager, random_tables):
    # 4 servers, one dead
    await manager.server_add()
    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[0].server_id)

    logging.info('Removenode 1')
    # Removenode will fail before removing the server from group 0
    async with inject_error(manager.api, servers[1].ip_addr, 'removenode_fail_before_group0',
                            one_shot=True):
        try:
            await manager.remove_node(servers[1].server_id, servers[0].server_id)
        except Exception as e:
            # Note: the exception printed here is only '500 internal server error', need to look in test.py log.
            logging.info(f'Injected exception: {e}')


    logging.info('Removenode 2')
    # Another attempt at removenode won't work: "Host ID not found in the cluster"
    try:
        await manager.remove_node(servers[1].server_id, servers[0].server_id)
    except Exception as e:
        # Note: the exception printed here is only '500 internal server error', need to look in test.py log.
        logging.info(f'Not injected exception: {e}')


    logging.info('Stop server')
    # We have 3 members of token ring, removenode says the 4th member is not there.
    # But shutting down one of the nodes now will lead to group 0 majority loss.
    await manager.server_stop_gracefully(servers[1].server_id)

    # Trying to do a group 0 operation now will fail (timeout, the new server will be stuck trying to join).
    logging.info('Adding server')
    await manager.server_add()
