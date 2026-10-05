"""Run baseline and fault-tolerant experiments and write the result file."""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback

from ftuis.config import EVENTS_PATH, RESULTS, ensure_dirs
from ftuis.ecc import demonstrate
from ftuis.reliability import theoretical_model
from experiments.harness import CampusClient, World, reset_world, wait_ready, wipe
from runner.stack import DockerControl, docker, start_stack, stop_stack
from experiments.scenarios import (
    application_crash,
    database_failure,
    high_load,
    interrupted_transaction,
    network_timeout,
    node_failure,
    reliability_session,
)

SCENARIOS = {
    "application_crash": application_crash,
    "database_failure": database_failure,
    "network_timeout": network_timeout,
    "node_failure": node_failure,
    "interrupted_transaction": interrupted_transaction,
    "high_load": high_load,
}


def _container_ecc() -> dict[str, object]:
    result = docker(["exec", "campus-student-a", "python", "-m", "ftuis.ecc"], check=False)
    if result.returncode != 0:
        return {"ok": False, "error": (result.stderr or result.stdout).strip()}
    try:
        body = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"ok": False, "error": result.stdout.strip()}
    body["ran_in_container"] = "campus-student-a"
    return body


def run_mode(mode: str, names: list[str], session_s: float) -> tuple[list[dict], dict[str, object] | None]:
    # Host-side events (kills, holds) must record the mode under test.
    # A leftover FT_MODE in the shell would otherwise label them wrong.
    os.environ["FT_MODE"] = mode
    wipe()
    rows: list[dict] = []
    container_demo = None
    try:
        start_stack(mode)
        control = DockerControl(mode)
        wait_ready(control, timeout=90)
        if mode == "ft":
            container_demo = _container_ecc()
        world = World(mode=mode, control=control, client=CampusClient())
        for name in names:
            reset_world(control)
            try:
                rows.append(SCENARIOS[name](world))
            except Exception as exc:
                rows.append(
                    {
                        "experiment": name,
                        "mode": mode,
                        "failure": "scenario crashed",
                        "detection_time_s": None,
                        "recovery_time_s": None,
                        "failed_requests": 0,
                        "successful_requests": 0,
                        "recovered_requests": 0,
                        "consistency": "fail",
                        "notes": f"{exc}\n{traceback.format_exc()}",
                    }
                )
                try:
                    reset_world(control)
                except Exception:
                    traceback.print_exc()
        if session_s > 0:
            reset_world(control)
            try:
                rows.append(reliability_session(world, session_s))
            except Exception as exc:
                rows.append(
                    {
                        "experiment": "reliability_session",
                        "mode": mode,
                        "notes": f"{exc}\n{traceback.format_exc()}",
                        "consistency": "fail",
                    }
                )
        if EVENTS_PATH.exists():
            target = RESULTS / f"events_{mode}.jsonl"
            target.write_text(EVENTS_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        return rows, container_demo
    finally:
        stop_stack()


def print_table(rows: list[dict]) -> None:
    print()
    print(f"{'mode':<10} {'experiment':<26} {'detect':>8} {'recover':>8} {'ok':>5} {'fail':>5} {'consist':<8}")
    for row in rows:
        detect = _num(row.get("detection_time_s"))
        recover = _num(row.get("recovery_time_s"))
        print(
            f"{row.get('mode', ''):<10} {row.get('experiment', ''):<26} "
            f"{detect:>8} {recover:>8} {row.get('successful_requests', 0):>5} "
            f"{row.get('failed_requests', 0):>5} {str(row.get('consistency', '')):<8}"
        )
        note = row.get("notes")
        if note:
            print(f"           {note}")
        issues = row.get("consistency_issues") or []
        if issues:
            print(f"           issues: {'; '.join(issues)}")


def _num(value) -> str:
    if value is None:
        return "-"
    return f"{value:.3f}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run campus fault-injection experiments")
    parser.add_argument("--modes", default="baseline,ft")
    parser.add_argument("--scenarios", default="all")
    parser.add_argument("--session-s", type=float, default=16.0)
    args = parser.parse_args()
    try:
        import fastapi  # noqa: F401
    except ImportError:
        sys.exit("Use the project virtualenv: .venv/bin/python -m experiments.run")

    names = list(SCENARIOS) if args.scenarios == "all" else [item.strip() for item in args.scenarios.split(",") if item.strip()]
    unknown = [name for name in names if name not in SCENARIOS]
    if unknown:
        sys.exit(f"Unknown scenarios: {', '.join(unknown)}. Choices: {', '.join(SCENARIOS)}")
    modes = [item.strip() for item in args.modes.split(",") if item.strip()]
    ensure_dirs()
    report = {
        "runtime": "docker-compose",
        "ecc_model_host": demonstrate(),
        "ecc_model_container": None,
        "theory": theoretical_model(),
        "runs": [],
    }
    for mode in modes:
        if mode not in {"baseline", "ft"}:
            sys.exit("modes must be baseline or ft")
        print(f"\n=== {mode} ===", flush=True)
        rows, container_demo = run_mode(mode, names, args.session_s)
        report["runs"].extend(rows)
        if container_demo is not None:
            report["ecc_model_container"] = container_demo
    destination = RESULTS / "experiment_results.json"
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print_table(report["runs"])
    theory = report["theory"]
    base = theory["baseline"]["system"]
    ft = theory["fault_tolerant"]["system"]
    print()
    print(
        "Planning estimate (assumed rates, not the injection test): "
        f"baseline availability {base['availability']:.6f}, "
        f"fault-tolerant availability {ft['availability']:.8f}"
    )
    print(f"Wrote {destination}")


if __name__ == "__main__":
    main()
