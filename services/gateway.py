"""API gateway: timeouts, retries, circuit breaking, and load balancing."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from ftuis.config import REGISTRY_PATH, TUITION_AMOUNT, load_settings
from ftuis.events import emit
from ftuis.resilience import CircuitBreaker, CircuitOpen, backoff_s
from ftuis.topology import BACKENDS

settings = load_settings()
DASHBOARD = Path(__file__).with_name("dashboard.html")


class UpstreamFailed(Exception):
    def __init__(self, service: str, detail: str) -> None:
        super().__init__(detail)
        self.service = service
        self.detail = detail


class Directory:
    def __init__(self) -> None:
        self.status: dict[str, str] = {}
        self.cursor: dict[str, int] = {}
        self.breakers = {name: CircuitBreaker(name) for name in BACKENDS}

    def all_instances(self) -> list[dict[str, str]]:
        if not REGISTRY_PATH.exists():
            return []
        try:
            payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        return list(payload.get("instances", []))

    def roster(self) -> list[dict[str, str]]:
        return [row for row in self.all_instances() if row["service"] in BACKENDS]

    def pick(self, service: str, exclude: set[str], prefer: str | None) -> dict[str, str] | None:
        eligible = [
            row
            for row in self.roster()
            if row["service"] == service
            and row["id"] not in exclude
            and self.status.get(row["id"]) != "down"
        ]
        if prefer:
            for row in eligible:
                if row["id"] == prefer:
                    return row
        if not eligible:
            return None
        index = self.cursor.get(service, 0)
        self.cursor[service] = index + 1
        return eligible[index % len(eligible)]

    def mark(self, instance_id: str, status: str) -> None:
        previous = self.status.get(instance_id)
        self.status[instance_id] = status
        if previous != status:
            emit("health_changed", target=instance_id, status=status, previous=previous or "unknown")

    def system_up(self) -> bool:
        by_service: dict[str, bool] = {name: False for name in BACKENDS}
        seen = set()
        for row in self.roster():
            seen.add(row["service"])
            if self.status.get(row["id"]) == "up":
                by_service[row["service"]] = True
        return seen == set(BACKENDS) and all(by_service.values())


directory = Directory()
client: httpx.AsyncClient | None = None


async def health_loop() -> None:
    assert client is not None
    while True:
        directory.mark(settings.instance_id, "up")
        for row in directory.all_instances():
            if row["id"] == settings.instance_id:
                continue
            try:
                response = await client.get(row["url"] + "/health", timeout=0.4)
                directory.mark(row["id"], "up" if response.status_code == 200 else "down")
            except httpx.HTTPError:
                directory.mark(row["id"], "down")
        await asyncio.sleep(0.3)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global client
    client = httpx.AsyncClient()
    task = asyncio.create_task(health_loop())
    yield
    task.cancel()
    await client.aclose()


app = FastAPI(title="Campus gateway", lifespan=lifespan)


class RegisterIn(BaseModel):
    name: str = Field(min_length=1)
    program: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class PaymentIn(BaseModel):
    student_id: str
    amount: float = Field(gt=0)
    idempotency_key: str


class GradeIn(BaseModel):
    student_id: str
    course: str
    grade: str


async def invoke(service: str, method: str, path: str, body: dict | None = None, prefer: str | None = None) -> dict:
    assert client is not None
    excluded: set[str] = set()
    last_error = "no healthy instance"
    saw_failure = False
    for attempt in range(settings.upstream_attempts):
        if settings.ft and not directory.breakers[service].allow():
            emit("circuit_open", service=service)
            raise CircuitOpen(service)
        instance = directory.pick(service, excluded, prefer if attempt == 0 else None)
        if instance is None:
            break
        try:
            response = await client.request(
                method,
                instance["url"] + path,
                json=body,
                timeout=settings.upstream_timeout_s,
            )
        except httpx.TimeoutException:
            saw_failure = True
            last_error = f"timeout calling {instance['id']}"
            emit("upstream_timeout", target=instance["id"], service=service, attempt=attempt)
            if settings.ft:
                directory.breakers[service].failure()
            excluded.add(instance["id"])
            if attempt + 1 < settings.upstream_attempts:
                await asyncio.sleep(backoff_s(attempt))
            continue
        except httpx.HTTPError as exc:
            saw_failure = True
            last_error = f"{type(exc).__name__} calling {instance['id']}"
            emit("upstream_error", target=instance["id"], service=service, error=type(exc).__name__)
            if isinstance(exc, httpx.ConnectError):
                directory.mark(instance["id"], "down")
            if settings.ft:
                directory.breakers[service].failure()
            excluded.add(instance["id"])
            if attempt + 1 < settings.upstream_attempts:
                await asyncio.sleep(backoff_s(attempt))
            continue
        if response.status_code >= 500:
            saw_failure = True
            last_error = response.text
            emit("upstream_status", target=instance["id"], service=service, status=response.status_code)
            if settings.ft:
                directory.breakers[service].failure()
            excluded.add(instance["id"])
            if attempt + 1 < settings.upstream_attempts:
                await asyncio.sleep(backoff_s(attempt))
            continue
        if settings.ft:
            directory.breakers[service].success()
        if saw_failure:
            emit("request_recovered", service=service, attempts=attempt + 1, target=instance["id"])
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()
    raise UpstreamFailed(service, last_error)


def _prefer(request: Request, service: str) -> str | None:
    value = request.headers.get("x-prefer-instance")
    if value and value.startswith(service):
        return value
    return None


@app.get("/health")
def process_health() -> dict[str, str]:
    return {
        "status": "ok",
        "instance": settings.instance_id,
        "node": settings.node_id,
        "service": "gateway",
        "mode": settings.mode,
    }


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return DASHBOARD.read_text(encoding="utf-8")


@app.get("/api/health")
def health() -> dict[str, object]:
    roster = []
    for row in directory.all_instances():
        roster.append({**row, "status": directory.status.get(row["id"], "unknown")})
    return {
        "mode": settings.mode,
        "gateway": settings.instance_id,
        "system_up": directory.system_up(),
        "roster": roster,
        "instances": {row["id"]: row["status"] for row in roster if row["service"] != "gateway"},
        "circuits": {name: breaker.snapshot() for name, breaker in directory.breakers.items()},
    }


@app.post("/admin/reset-breakers")
def reset_breakers() -> dict[str, bool]:
    for breaker in directory.breakers.values():
        breaker.reset()
    return {"ok": True}


@app.post("/api/register")
async def register(body: RegisterIn, request: Request) -> dict[str, object]:
    student = await invoke(
        "student",
        "POST",
        "/students",
        body.model_dump(),
        prefer=_prefer(request, "student"),
    )
    student_id = str(student["id"])
    try:
        await invoke(
            "academic",
            "POST",
            "/transcripts/init",
            {"student_id": student_id, "idempotency_key": body.idempotency_key},
        )
    except (UpstreamFailed, CircuitOpen) as exc:
        raise HTTPException(status_code=502, detail=f"academic service unavailable: {exc}") from exc
    try:
        payment = await invoke(
            "payment",
            "POST",
            "/payments",
            {
                "student_id": student_id,
                "amount": TUITION_AMOUNT,
                "idempotency_key": body.idempotency_key,
            },
            prefer=_prefer(request, "payment"),
        )
    except (UpstreamFailed, CircuitOpen) as exc:
        if not settings.ft:
            raise HTTPException(
                status_code=502,
                detail="payment unavailable; registration left incomplete",
            ) from exc
        updated = await invoke(
            "student",
            "POST",
            f"/students/{student_id}/tuition",
            {"status": "deferred", "registration_status": "deferred"},
        )
        emit("graceful_degradation", student_id=student_id, reason=str(exc))
        return {"degraded": True, "student_id": student_id, "tuition": "deferred", "student": updated}
    if payment.get("step") != "committed":
        raise HTTPException(status_code=502, detail=f"payment not committed: {payment.get('step')}")
    updated = await invoke(
        "student",
        "POST",
        f"/students/{student_id}/tuition",
        {"status": "paid", "registration_status": "active"},
    )
    return {"degraded": False, "student_id": student_id, "tuition": "paid", "payment": payment, "student": updated}


@app.post("/api/payments")
async def pay(body: PaymentIn, request: Request) -> dict[str, object]:
    try:
        return await invoke(
            "payment",
            "POST",
            "/payments",
            body.model_dump(),
            prefer=_prefer(request, "payment"),
        )
    except CircuitOpen as exc:
        raise HTTPException(status_code=503, detail="payment circuit open") from exc
    except UpstreamFailed as exc:
        raise HTTPException(status_code=504, detail=exc.detail) from exc


@app.get("/api/ping/{service}")
async def ping(service: str) -> dict:
    if service not in BACKENDS:
        raise HTTPException(status_code=404, detail="unknown service")
    try:
        return await invoke(service, "GET", "/ping")
    except CircuitOpen as exc:
        raise HTTPException(status_code=503, detail="circuit open") from exc
    except UpstreamFailed as exc:
        raise HTTPException(status_code=504, detail=exc.detail) from exc


@app.post("/api/work")
async def work() -> dict:
    try:
        return await invoke("student", "POST", "/work")
    except CircuitOpen as exc:
        raise HTTPException(status_code=503, detail="circuit open") from exc
    except UpstreamFailed as exc:
        raise HTTPException(status_code=503, detail=exc.detail) from exc


@app.get("/api/students")
async def students() -> dict:
    try:
        return await invoke("student", "GET", "/students")
    except (CircuitOpen, UpstreamFailed) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/students/{student_id}")
async def student(student_id: str) -> dict:
    return await invoke("student", "GET", f"/students/{student_id}")


@app.get("/api/transcripts/{student_id}")
async def transcript(student_id: str) -> dict:
    return await invoke("academic", "GET", f"/transcripts/{student_id}")


@app.post("/api/grades")
async def grades(body: GradeIn) -> dict:
    return await invoke("academic", "POST", "/records", body.model_dump())


@app.exception_handler(UpstreamFailed)
async def upstream_failed(_request, exc: UpstreamFailed) -> JSONResponse:
    return JSONResponse({"error": exc.detail, "service": exc.service}, status_code=504)
