"""Ports and process layout shared by the supervisor."""

from __future__ import annotations

MODULES = {
    "gateway": "services.gateway:app",
    "student": "services.student:app",
    "payment": "services.payment:app",
    "academic": "services.academic:app",
}

# Logical node -> port. Baseline starts node A only. FT starts A and B.
PORTS: dict[tuple[str, str], int] = {
    ("gateway", "a"): 8001,
    ("gateway", "b"): 8002,
    ("student", "a"): 8101,
    ("student", "b"): 8102,
    ("payment", "a"): 8201,
    ("payment", "b"): 8202,
    ("academic", "a"): 8301,
    ("academic", "b"): 8302,
}

CONTROL_PORT = 9000
SERVICES = ("gateway", "student", "payment", "academic")
BACKENDS = ("student", "payment", "academic")
