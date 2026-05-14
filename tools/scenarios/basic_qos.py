"""Scenario: basic-qos.

Issue basic.qos with prefetch_count=10, global=False. Captures basic.qos
/ qos-ok.
"""
from _common import connection, log

with connection() as conn:
    ch = conn.channel()
    ch.basic_qos(prefetch_size=0, prefetch_count=10, global_qos=False)
    log("qos applied")
    ch.close()
