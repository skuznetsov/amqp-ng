"""Scenario: publish-nack.

Declare a queue with x-max-length=0 and x-overflow=reject-publish, then
publish into it with confirms on. The broker MUST nack the publish.
Captures basic.nack with delivery-tag=1.
"""
import pika
from _common import connection, log

QUEUE = "amqp-ng.corpus.publish-nack"

with connection() as conn:
    ch = conn.channel()
    ch.queue_declare(
        queue=QUEUE,
        durable=False,
        auto_delete=True,
        arguments={"x-max-length": 0, "x-overflow": "reject-publish"},
    )
    ch.confirm_delivery()
    try:
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=b"will-be-nacked",
            mandatory=False,
        )
        log("UNEXPECTED: publish acked")
    except pika.exceptions.NackError as e:
        log(f"got NackError: {e}")
    ch.queue_delete(queue=QUEUE)
    ch.close()
