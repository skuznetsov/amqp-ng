"""Scenario: multi-frame-body.

Negotiate a small frame_max via channel_max/frame_max (pika passes
frame_max via ConnectionParameters), publish a body larger than
frame_max - 8, force the broker (and pika) to split the body into
two body frames. Captures basic.publish + header + body[0] + body[1].
"""
import pika
from _common import PROXY_HOST, PROXY_PORT, wait_proxy_listening, log

QUEUE = "amqp-ng.corpus.multi-frame-body"
FRAME_MAX = 4096
BODY_LEN = FRAME_MAX * 3  # forces ~3 body frames

wait_proxy_listening()
params = pika.ConnectionParameters(
    host=PROXY_HOST,
    port=PROXY_PORT,
    credentials=pika.PlainCredentials("guest", "guest"),
    frame_max=FRAME_MAX,
    heartbeat=60,
    socket_timeout=5.0,
)
conn = pika.BlockingConnection(params)
try:
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=False, auto_delete=True)
    body = bytes((i & 0xff for i in range(BODY_LEN)))
    ch.basic_publish(exchange="", routing_key=QUEUE, body=body)
    log(f"published {BODY_LEN} bytes at frame_max={FRAME_MAX}")
    ch.queue_delete(queue=QUEUE)
    ch.close()
finally:
    conn.close()
