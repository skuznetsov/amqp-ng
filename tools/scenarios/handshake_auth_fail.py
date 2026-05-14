"""Scenario: handshake-auth-fail.

Attempt connection with bad password. Broker closes via connection.close
with reply-code 403 OR drops the socket after start-ok (RabbitMQ default
behavior is to close-with-reply when authentication_failure_close
capability is advertised by the client).
"""
import pika
from _common import conn_params, log, wait_proxy_listening

wait_proxy_listening()
try:
    pika.BlockingConnection(conn_params(user="guest", password="wrong-password"))
    log("UNEXPECTED: connected with bad password")
except pika.exceptions.ProbableAuthenticationError as e:
    log(f"got auth failure (probable): {e}")
except pika.exceptions.AMQPConnectionError as e:
    log(f"got connection error: {e}")
