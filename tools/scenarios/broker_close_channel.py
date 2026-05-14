"""Scenario: broker-close-channel.

Passive-declare a queue that does not exist. Broker MUST respond with
channel.close reply-code=404 (NOT_FOUND). Captures the broker-initiated
channel.close path.
"""
import pika
from _common import connection, log

with connection() as conn:
    ch = conn.channel()
    try:
        ch.queue_declare(queue="amqp-ng.corpus.does-not-exist", passive=True)
        log("UNEXPECTED: passive declare succeeded")
    except pika.exceptions.ChannelClosedByBroker as e:
        log(f"got ChannelClosedByBroker: {e.reply_code} {e.reply_text}")
