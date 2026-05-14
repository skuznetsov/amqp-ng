"""Scenario: handshake-success.

Open a vanilla connection with default guest/guest on vhost `/`,
then close gracefully. Captures:
- AMQP protocol header
- connection.start / start-ok (PLAIN, guest)
- connection.tune / tune-ok
- connection.open / open-ok
- connection.close / close-ok
"""
from _common import connection, log

with connection(heartbeat=60) as conn:
    log("connected; closing")
