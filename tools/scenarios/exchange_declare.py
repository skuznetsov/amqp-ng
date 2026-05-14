"""Scenario: exchange-declare.

Declare a durable direct exchange. Captures exchange.declare / declare-ok.
"""
from _common import connection, log

EXCHANGE = "amqp-ng.corpus.exchange"

with connection() as conn:
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True, auto_delete=False)
    log("exchange declared")
    ch.exchange_delete(exchange=EXCHANGE)
    ch.close()
