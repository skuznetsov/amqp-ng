"""Scenario: basic-get.

Issue basic.get on an empty queue (captures basic.get-empty), then publish
a message and basic.get again (captures basic.get-ok + content header + body).
"""
from _common import connection, log

QUEUE = "amqp-ng.corpus.basic-get"

with connection() as conn:
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=False, auto_delete=True)

    method, props, body = ch.basic_get(queue=QUEUE, auto_ack=True)
    log(f"first get: method={method!r}")

    ch.basic_publish(exchange="", routing_key=QUEUE, body=b"polled-payload")
    method, props, body = ch.basic_get(queue=QUEUE, auto_ack=True)
    log(f"second get: method={method!r} body={body!r}")

    ch.queue_delete(queue=QUEUE)
    ch.close()
