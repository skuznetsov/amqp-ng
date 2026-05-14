"""Scenario: publish-confirm.

Enable publisher confirms, publish three messages, wait for acks.
Captures confirm.select / select-ok, three basic.publish sequences,
and three basic.ack frames (may arrive multiple-ack collapsed).
"""
import pika
from _common import connection, log

QUEUE = "amqp-ng.corpus.publish-confirm"

with connection() as conn:
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=False, auto_delete=True)
    ch.confirm_delivery()
    for i in range(3):
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=f"msg-{i}".encode(),
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=False,
        )
        log(f"published msg-{i}")
    ch.queue_delete(queue=QUEUE)
    ch.close()
