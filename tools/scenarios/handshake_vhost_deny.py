"""Scenario: handshake-vhost-deny.

Attempt to open a non-existent vhost. Broker closes with
reply-code 530 (NOT_ALLOWED) after connection.open.
"""
import pika
from _common import conn_params, log, wait_proxy_listening

wait_proxy_listening()
try:
    pika.BlockingConnection(conn_params(vhost="/does-not-exist"))
    log("UNEXPECTED: opened non-existent vhost")
except pika.exceptions.AMQPConnectionError as e:
    log(f"got connection error: {e}")
