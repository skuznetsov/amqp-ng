"""Scenario: channel-open-close.

Open one channel, then close it cleanly. Captures channel.open / open-ok
and channel.close / close-ok with reply-code 200.
"""
from _common import connection, log

with connection() as conn:
    ch = conn.channel()
    log(f"channel {ch.channel_number} open")
    ch.close()
    log("channel closed")
