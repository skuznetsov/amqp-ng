"""Scenario: queue-declare.

Declare a durable, non-exclusive, non-auto-delete queue with a name.
Captures queue.declare / declare-ok plus the message-count / consumer-count
fields in declare-ok.
"""
from _common import connection, log

QUEUE = "amqp-ng.corpus.queue-declare"

with connection() as conn:
    ch = conn.channel()
    result = ch.queue_declare(queue=QUEUE, durable=True, exclusive=False, auto_delete=False)
    log(f"declared {result.method.queue} msgs={result.method.message_count} consumers={result.method.consumer_count}")
    ch.queue_delete(queue=QUEUE)
    ch.close()
