"""Scenario: heartbeat.

Negotiate heartbeat=2 (smallest reasonable value), then idle for 5 seconds
to elicit multiple heartbeat frames in BOTH directions. Captures type=8
frames with channel=0 and an empty payload.
"""
import time

from _common import connection, log

with connection(heartbeat=2) as conn:
    # 5s idle covers ~2.5 send-cadence intervals (heartbeat/2 = 1s).
    deadline = time.time() + 5.0
    while time.time() < deadline:
        conn.process_data_events(time_limit=0.5)
    log("idle period elapsed")
