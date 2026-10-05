# Fault-tolerant university information system

Python services for a campus platform, run in two modes from the same code:

- `baseline` — one process per service, no retries, no mirror, no automatic restart
- `ft` — two logical nodes, health checks, retries, timeouts, a circuit breaker, idempotent payments, checkpoints, and a mirrored database

## Setup

Python 3.11 or newer, plus Docker Desktop. This machine uses a virtualenv because Homebrew Python blocks system-wide pip.

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
chmod +x campus
```

Docker Compose is the measured runtime. Each service replica is its own container. Baseline starts node A only, with no automatic restart. Fault-tolerant mode starts node A and node B, and Docker restarts a container that exits.

The process supervisor in `runner/supervisor.py` is an older orchestrator. The experiments do not use it.

## Run

Start the fault-tolerant system and open http://127.0.0.1:8001

```sh
./campus up ft
./campus down
```

Baseline stays in the foreground and stops when you interrupt it:

```sh
./campus up baseline
```

Run every failure scenario in both modes. This takes a few minutes and writes `results/experiment_results.json`.

```sh
./campus
```

One scenario:

```sh
./campus --modes ft --scenarios application_crash --session-s 0
```

## What the experiments inject

| Scenario | What is broken | What you should see |
| --- | --- | --- |
| application_crash | `student-a` is killed | Baseline registrations fail until an operator restart. FT keeps serving on `student-b`. |
| database_failure | The student database file is overwritten | Baseline reads fail until a backup is copied back. FT rebuilds the file from its mirror. |
| network_timeout | `payment-a` sleeps 2.5s, then payment is forced down | Baseline hits the 2s timeout. FT uses `payment-b`, and if both are down the registration is accepted with tuition deferred. |
| node_failure | Every process on logical node A is killed | Baseline is the whole campus. FT keeps node B. |
| interrupted_transaction | Payment process dies after moving money | Baseline retry charges twice. FT completes the checkpoint once. |
| high_load | 30 overlapping requests | Baseline rejects the overflow. FT spreads it across two replicas. |
| reliability_session | A replica is killed every 5s for 16s | Probe availability, MTTF, MTTR, and MTBF. The planning calculation in the same JSON file uses assumed rates and is not this session. |

The Docker restart policy is the orchestrator. It is not itself one of the campus nodes.
