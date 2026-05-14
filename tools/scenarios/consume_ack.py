"""Scenario: consume-ack.

Declare a queue, publish one message, start a consumer (no_ack=False),
receive the delivery, ack it, then cancel consume and close.
Captures basic.consume / consume-ok, basic.deliver + content header + body,
and basic.ack with delivery-tag=1.
"""
from _common import connection, log

QUEUE = "amqp-ng.corpus.consume-ack"

with connection() as conn:
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=False, auto_delete=True)
    ch.basic_publish(exchange="", routing_key=QUEUE, body=b"consume-me")
    log("published 1")

    received = []

    def on_msg(channel, method, props, body):
        log(f"delivered tag={method.delivery_tag} body={body!r}")
        received.append((method, body))
        channel.basic_ack(delivery_tag=method.delivery_tag)
        channel.basic_cancel(consumer_tag=method.consumer_tag)
        channel.stop_consuming()

    ch.basic_consume(queue=QUEUE, on_message_callback=on_msg, auto_ack=False)
    ch.start_consuming()
    log(f"received {len(received)} messages")
    ch.queue_delete(queue=QUEUE)
    ch.close()
