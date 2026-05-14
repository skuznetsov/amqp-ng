"""Common harness for AMQP corpus drivers.

All drivers connect to 127.0.0.1:5673 (the capture proxy), which forwards to
the real RabbitMQ on 127.0.0.1:5672. Each driver focuses on eliciting a
specific wire-level scenario; the proxy records the byte stream.

Drivers run as one-shot scripts. Use module-level functions, not classes.
"""
from __future__ import annotations

import os
import socket
import sys
import time
from contextlib import contextmanager

import pika

PROXY_HOST = "127.0.0.1"
PROXY_PORT = int(os.environ.get("AMQP_PROXY_PORT", "5673"))


def conn_params(
    *,
    heartbeat: int = 60,
    user: str = "guest",
    password: str = "guest",
    vhost: str = "/",
    blocked_connection_timeout: int = 5,
    socket_timeout: float = 5.0,
    connection_attempts: int = 1,
) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=PROXY_HOST,
        port=PROXY_PORT,
        virtual_host=vhost,
        credentials=pika.PlainCredentials(user, password),
        heartbeat=heartbeat,
        blocked_connection_timeout=blocked_connection_timeout,
        socket_timeout=socket_timeout,
        connection_attempts=connection_attempts,
    )


def wait_proxy_listening(port: int = PROXY_PORT, deadline: float = 3.0) -> None:
    """No-op: the runner waits for the proxy's READY marker before invoking us.

    Kept as a function for backward compatibility with scenarios that import
    it; opening a probe socket here would consume the proxy's single-shot
    accept slot.
    """
    return


@contextmanager
def connection(**kwargs):
    params = conn_params(**kwargs)
    conn = pika.BlockingConnection(params)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def log(msg: str) -> None:
    print(f"[driver] {msg}", file=sys.stderr, flush=True)
