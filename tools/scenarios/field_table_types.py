"""Scenario: field-table-types.

Declare a queue with x-* arguments covering multiple field-table value
types so the corpus contains the type bytes used by RabbitMQ:
  - longstr (S):  "x-route"      -> "abc"
  - short-short-int / signed (b): negative 8-bit fits via int (pika maps via heuristics)
  - long-long-int (l):            very large int
  - boolean (t):                  True
  - field-table (F):              nested dict
  - field-array (A):              list of mixed
  - timestamp (T):                int seconds (pika encodes as 'l' unless wrapped in datetime)
Note: pika is conservative about which AMQP type each Python value
maps to. The recorded bytes show what RabbitMQ actually accepted; the
01-types doc walks through what is observed, not theoretical.
"""
import time

from _common import connection, log

QUEUE = "amqp-ng.corpus.field-table-types"

arguments = {
    "x-route": "abc",
    "x-int-small": 42,
    "x-int-large": 10_000_000_000,
    "x-bool": True,
    "x-table": {"nested-key": "nested-value", "n-int": 7},
    "x-array": [1, "two", True],
    "x-timestamp-secs": int(time.time()),
}

with connection() as conn:
    ch = conn.channel()
    ch.queue_declare(
        queue=QUEUE,
        durable=False,
        auto_delete=True,
        arguments=arguments,
    )
    log(f"declared with {len(arguments)} x-args")
    ch.queue_delete(queue=QUEUE)
    ch.close()
