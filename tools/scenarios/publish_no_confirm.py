"""Scenario: publish-no-confirm.

Publish one small message (fire-and-forget) to the default exchange with
the queue name as routing key. Captures basic.publish + content-header
+ body frame.
"""
from _common import connection, log

QUEUE = "amqp-ng.corpus.publish-no-confirm"
BODY = b"hello-amqp-ng"

with connection() as conn:
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=False, auto_delete=True)
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=BODY,
    )
    log(f"published {len(BODY)} bytes")
    ch.queue_delete(queue=QUEUE)
    ch.close()
