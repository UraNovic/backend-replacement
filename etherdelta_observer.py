#!/usr/bin/env python

from app import App
import asyncio
from config import ED_WS_SERVERS
from datetime import datetime
from decimal import Decimal
import json
import logging
from order_enums import OrderSource, OrderState
from order_hash import make_order_hash
from pprint import pformat
from random import sample
from socketio_client import SocketIOClient
from time import time
from taskq import TaskQ, QueueEmpty
from web3 import Web3
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

NUM_OBSERVERS = 6
CHECK_TOKENS_INTERVAL = 25
CHECK_TOKENS_PER_PONG = 5
CHECK_TOKENS_SPEED = NUM_OBSERVERS * CHECK_TOKENS_PER_PONG / CHECK_TOKENS_INTERVAL
CHECK_TOKENS_SLEEP = 4

ZERO_ADDR = "0x0000000000000000000000000000000000000000"
logger = logging.getLogger('etherdelta_observer')
logger.setLevel(logging.DEBUG)

tokens_queue = TaskQ()
async def fetch_db_tokens(column, table):
    async with App().db.acquire_connection() as conn:
        select_stmt = "SELECT DISTINCT {} FROM {}".format(column, table)
        return { Web3.toHex(r[column]) for r in await conn.fetch(select_stmt)}

async def enqueue_tokens(tokens):
    for token in tokens:
        if token not in tokens_queue:
            await tokens_queue.put(token, 0)
    logger.debug("%i tokens now in the queue", len(tokens_queue))

async def enqueue_listed_tokens():
    with open("tokens.json") as f:
        tokens = { token["addr"] for token in json.load(f) }

    logger.debug("Gathered %i listed tokens", len(tokens))
    await enqueue_tokens(tokens)

async def enqueue_active_tokens():
    tokens = await fetch_db_tokens("token_get", "orders") \
                | await fetch_db_tokens("token_give", "orders") \
                | await fetch_db_tokens("token_get", "trades") \
                | await fetch_db_tokens("token_give", "trades") \
                | await fetch_db_tokens("token", "transfers")

    logger.debug("Gathered %i active tokens from DB records", len(tokens))
    await enqueue_tokens(tokens)

async def enqueue_ticker_tokens(ticker_list):
    logger.debug("Gathered %i ticker tokens", len(ticker_list))
    await enqueue_tokens([t["tokenAddr"] for t in ticker_list])

async def process_orders(orders):
    not_deleted_filter = lambda order: "deleted" not in order or not order["deleted"]
    logger.info("Processing %i orders", len(orders))
    orders = list(filter(not_deleted_filter, orders))
    logger.debug("Filtered orders: %i", len(orders))

    for order in orders:
        try:
            await record_order(order)
        except Exception as e:
            logger.critical("Error while processing order '%s'", order)
            raise e

async def record_order(order):
    insert_statement = """INSERT INTO orders
        (
            "source", "signature",
            "token_give", "amount_give", "token_get", "amount_get",
            "expires", "nonce", "user", "state", "v", "r", "s", "date"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT ON CONSTRAINT index_orders_on_signature DO NOTHING;"""

    signature = make_order_hash(order)
    insert_args = (
        OrderSource.OFFCHAIN.name,
        Web3.toBytes(hexstr=signature),
        Web3.toBytes(hexstr=order["tokenGive"]),
        Decimal(order["amountGive"]),
        Web3.toBytes(hexstr=order["tokenGet"]),
        Decimal(order["amountGet"]),
        int(order["expires"]),
        int(order["nonce"]),
        Web3.toBytes(hexstr=order["user"]),
        OrderState.OPEN.name,
        int(order["v"]),
        Web3.toBytes(hexstr=order["r"]),
        Web3.toBytes(hexstr=order["s"]),
        datetime.utcnow()
    )

    async with App().db.acquire_connection() as connection:
        await connection.execute(insert_statement, *insert_args)
    # logger.info("recorded order token=%s, signature=%s, user=%s, expires=%i",
    #             order["tokenGive"] if order["tokenGive"] != ZERO_ADDR else order["tokenGet"],
    #             signature, order["user"], int(order["expires"]))

async def on_connect(io_client, event):
    logger.info("ED API client connected to %s", io_client.ws_url)
    # Gather tokens that we know
    await enqueue_listed_tokens()
    await enqueue_active_tokens()
    # ...and those we don't know
    await io_client.emit("getMarket", { "token": ZERO_ADDR })

async def on_error(io_client, event, error):
    logger.critical("ED API client (connected to %s) error: %s", io_client.ws_url, error)

async def on_disconnect(io_client, event):
    logger.info("ED API client disconnected from %s", io_client.ws_url)

async def on_orders(io_client, event, payload=None):
    logger.debug("Received orders event")
    orders = [*payload["buys"], *payload["sells"]]
    await process_orders(orders)
    await enqueue_tokens([
        *[t["tokenGet"] for t in orders],
        *[t["tokenGive"] for t in orders]
    ])

async def on_market(io_client, event, payload):
    logger.debug("Received market event, orders present? %s", "orders" in payload)
    if "orders" not in payload:
        # The beautiful market API
        return

    if len(payload["orders"]["buys"]) > 0:
        token = payload["orders"]["buys"][0]["tokenGet"]
    elif len(payload["orders"]["sells"]) > 0:
        token = payload["orders"]["sells"][0]["tokenGive"]
    else:
        logger.debug("No orders in market event")
        return

    orders = [*payload["orders"]["buys"], *payload["orders"]["sells"]]
    await process_orders(orders)
    next_check = time() + len(tokens_queue) / CHECK_TOKENS_SPEED
    logger.debug("Order refresh for token=%s, next check at %i", token, next_check)
    await tokens_queue.put(token, next_check)

    # Add tokens from ticker
    if "returnTicker" in payload:
        await enqueue_ticker_tokens(payload["returnTicker"].values())

async def send_token_queries(io_client):
    for _ in range(CHECK_TOKENS_PER_PONG):
        try:
            logger.debug("token get_nowait")
            token = await tokens_queue.get_nowait()
            logger.debug("token acquired")
            await tokens_queue.put(token, time() + 15)
            logger.debug("token put back")
        except QueueEmpty:
            logger.debug("Tokens queue was empty")
            break # better luck next time!
        else:
            await io_client.emit("getMarket", { "token": token })
            await asyncio.sleep(CHECK_TOKENS_SLEEP)

async def on_pong(io_client, event):
    logger.info("Connection to %s alive: pong received", io_client.ws_url)
    await enqueue_active_tokens()
    logger.debug("Next tokens to check: %s", tokens_queue.peek_head(60))
    logger.debug("Last tokens to check: %s", tokens_queue.peek_tail(5))
    await send_token_queries(io_client)

async def main(my_id, num_observers):
    ws_url = ED_WS_SERVERS[my_id]
    io_client = SocketIOClient(ws_url)
    io_client.on("orders", on_orders)
    io_client.on("market", on_market)
    io_client.on("pong", on_pong) # Schedules full refreshes
    io_client.on("connect", on_connect)
    io_client.on("disconnect", on_disconnect)

    last_attempt = None # TODO: Exponential backoff
    while True:
        try:
            await io_client.start()
        except (ConnectionClosed, InvalidStatusCode) as e:
            logger.warn("Connection with %s lost with %s", ws_url, e)
            await asyncio.sleep(5.0)
            continue
    # Exceptions to handle:
    # - websockets.exceptions.InvalidStatusCode: LIKE Status code not 101: 521
    # - websockets.exceptions.ConnectionClosed: LIKE WebSocket connection is closed: code = 1006 (connection closed abnormally [internal]),

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(App().db.establish_connection())

    tasks = [ asyncio.ensure_future(main(i, NUM_OBSERVERS))
                for i in range(0, NUM_OBSERVERS) ]
    loop.run_until_complete(asyncio.gather(*tasks))
