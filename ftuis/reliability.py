"""Planning estimates for the report, kept separate from measured experiment data.

The numbers use assumed component failure rates. They are not measurements.
The experiment runner records a different availability: the fraction of probes
that succeeded while we were deliberately killing processes every few seconds.
Those two availabilities must not be presented as the same quantity.
"""

from __future__ import annotations

COMPONENT_LAMBDA_PER_HOUR = {
    "gateway": 0.002,
    "student": 0.010,
    "payment": 0.015,
    "academic": 0.010,
    "database": 0.008,
    "node": 0.005,
}

# Baseline repair is an operator restart. FT repair is an automatic restart.
BASELINE_MTTR_H = 0.5
FT_MTTR_H = 1 / 60

DESIGN = {
    "hardware": [
        "Two logical nodes, each service replica running in its own Docker container",
        "Gateway load-balances across the replicas that health checks still mark up",
        "RAID-1 style mirror: every committed write is backed up to a second database file",
        "Software Hamming SEC code as a model of memory ECC, separate from the hardware mechanism",
    ],
    "software": [
        "Retry with exponential backoff",
        "Timeouts",
        "Circuit breaker",
        "Idempotent payment processing",
        "Duplicate-request detection",
        "Checkpointing",
        "Rollback and roll-forward",
        "Health checks",
        "Graceful degradation",
        "Service replication",
    ],
    "limits": [
        "Both logical nodes are containers on one laptop, not two physical servers",
        "The database mirror is a second SQLite file, not a hardware RAID controller",
        "The Hamming code is a software model of ECC. It does not use the memory controller",
        "Docker's restart policy is the orchestrator and sits outside the service failure domain",
        "Kubernetes is intentionally not used",
    ],
}


def _series(lmbda: float, mttr_h: float) -> dict[str, float]:
    mttf = 1.0 / lmbda
    return {
        "lambda_per_hour": lmbda,
        "mttf_h": mttf,
        "mttr_h": mttr_h,
        "mtbf_h": mttf + mttr_h,
        "availability": mttf / (mttf + mttr_h),
    }


def _parallel_pair(lmbda: float, mttr_h: float) -> dict[str, float]:
    """1-out-of-2 active pair with independent repair.

    Steady-state availability is 1 - (1 - A)^2.
    MTTF is the Markov mean time from both-up until both are down:
    (mu + 3*lambda) / (2*lambda^2). Once both are down, two repair crews
    bring the pair back at 2*mu, so MTTR is 1/(2*mu).
    """
    mu = 1.0 / mttr_h
    one = _series(lmbda, mttr_h)
    mttf = (mu + 3.0 * lmbda) / (2.0 * lmbda * lmbda)
    mttr = 1.0 / (2.0 * mu)
    return {
        "lambda_per_hour": lmbda,
        "component_availability": one["availability"],
        "mttf_h": mttf,
        "mttr_h": mttr,
        "mtbf_h": mttf + mttr,
        "availability": 1.0 - (1.0 - one["availability"]) ** 2,
    }


def _combine_series(parts: dict[str, dict[str, float]]) -> dict[str, float]:
    availability = 1.0
    failure_rate = 0.0
    weighted_repair = 0.0
    for part in parts.values():
        availability *= part["availability"]
        # Use the subsystem failure frequency 1/MTTF.
        rate = 1.0 / part["mttf_h"]
        failure_rate += rate
        weighted_repair += rate * part["mttr_h"]
    mttf = 1.0 / failure_rate
    mttr = weighted_repair / failure_rate
    return {
        "mttf_h": mttf,
        "mttr_h": mttr,
        "mtbf_h": mttf + mttr,
        "availability": availability,
        "availability_from_mtbf": mttf / (mttf + mttr),
        "failure_rate_per_hour": failure_rate,
    }


def theoretical_model() -> dict[str, object]:
    baseline_parts = {
        name: _series(lmbda, BASELINE_MTTR_H)
        for name, lmbda in COMPONENT_LAMBDA_PER_HOUR.items()
    }
    ft_parts = {
        name: _parallel_pair(lmbda, FT_MTTR_H)
        for name, lmbda in COMPONENT_LAMBDA_PER_HOUR.items()
    }
    baseline = _combine_series(baseline_parts)
    ft = _combine_series(ft_parts)
    return {
        "assumptions": {
            "component_lambda_per_hour": COMPONENT_LAMBDA_PER_HOUR,
            "baseline_mttr_h": BASELINE_MTTR_H,
            "ft_mttr_h": FT_MTTR_H,
            "baseline_structure": "single instance of each component, series system",
            "ft_structure": "active-active pair for each component, then series of those pairs",
            "note": (
                "These rates are planning assumptions for the reliability section. "
                "The experiment injects crashes on a timer, so its failure rate is the "
                "injection schedule, not lambda above."
            ),
        },
        "baseline": {"components": baseline_parts, "system": baseline},
        "fault_tolerant": {"components": ft_parts, "system": ft},
        "design": DESIGN,
    }


def session_metrics(samples: list[tuple[float, bool]]) -> dict[str, object]:
    """Derive MTTF, MTTR, MTBF, and availability from probe samples.

    A sample is (timestamp, system_was_up). Completed up-intervals that end in
    a failure are the time-to-failure observations. Completed down-intervals are
    the repair observations. An interval still open at the end of the session
    is censored and is not averaged in.
    """
    if len(samples) < 2:
        return {"error": "not enough samples"}

    state = samples[0][1]
    span_start = samples[0][0]
    up_time = 0.0
    down_time = 0.0
    up_spans: list[float] = []
    down_spans: list[float] = []
    failures = 0

    for ts, up in samples[1:]:
        if up == state:
            continue
        span = ts - span_start
        if state:
            up_time += span
            up_spans.append(span)
            failures += 1
        else:
            down_time += span
            down_spans.append(span)
        state = up
        span_start = ts

    tail = samples[-1][0] - span_start
    censored = "up" if state else "down"
    if state:
        up_time += tail
    else:
        down_time += tail

    total = up_time + down_time
    mttf = sum(up_spans) / len(up_spans) if up_spans else None
    mttr = sum(down_spans) / len(down_spans) if down_spans else (0.0 if failures == 0 else None)
    mtbf = (mttf + mttr) if mttf is not None and mttr is not None else None
    return {
        "duration_s": total,
        "up_time_s": up_time,
        "down_time_s": down_time,
        "user_visible_failures": failures,
        "availability": (up_time / total) if total else None,
        "mttf_s": mttf,
        "mttr_s": mttr,
        "mtbf_s": mtbf,
        "failure_rate_per_hour": (failures / (total / 3600.0)) if total else None,
        "censored_tail": censored,
        "censored_tail_s": tail,
        "mttf_note": (
            "No completed time-to-failure interval. MTTF is longer than this session."
            if mttf is None
            else "MTTF is the mean length of up periods that ended in a user-visible failure."
        ),
    }
