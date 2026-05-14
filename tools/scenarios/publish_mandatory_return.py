"""Scenario: publish-mandatory-return.

Publish with mandatory=True to a routing key that no queue is bound to.
The broker MUST send basic.return + header + body before basic.ack.
Captures the return frame plus the routed (but unroutable) sequence.
"""
import time

import pika
from _common import connection, log

with connection() as conn:
    ch = conn.channel()
    ch.confirm_delivery()
    returned = []
    ch.add_on_return_callback(lambda channel, method, props, body: returned.append((method, body)))
    try:
        ch.basic_publish(
            exchange="",
            routing_key="amqp-ng.corpus.never-bound",
            body=b"unroutable-payload",
            properties=pika.BasicProperties(content_type="application/octet-stream"),
            mandatory=True,
        )
    except pika.exceptions.UnroutableError as e:
        log(f"got UnroutableError: {e}")
    # Drain any pending return frames.
    conn.process_data_events(time_limit=1.0)
    log(f"returns captured client-side: {len(returned)}")
    ch.close()
