Copyright 2026 Ismail Canga

# Heptamini

Heptamini is a runtime-agnostic ingress governor for constrained compute systems.

It sits in front of any execution backend (GPU worker, model server, local runtime) and enforces bounded concurrency with explicit backpressure under load.

Heptamini v1 is intentionally small, stable, and operationally predictable.

---

## Why This Exists

Inference and compute backends degrade under burst.

Without strict admission control:

- Requests accumulate internally.
- Latency grows with queue depth.
- Retries amplify pressure.
- Memory usage expands unpredictably.
- Overload becomes opaque.

Most systems rate-limit arrival. Few systems strictly govern execution.

Heptamini exists to enforce a hard concurrency boundary and make overload explicit.

When capacity is full, it returns `yield` immediately instead of buffering work.

This keeps the backend bounded and turns overload into a clear client-side decision.

---

---

## What It Does

- Deterministic framed TCP (one JSON object per line)
- Upload integrity (manifest + chunk + sha256 verification)
- Hard cap on concurrent backend submissions (`max_inflight`)
- Immediate `yield` when saturated (no server-side queue)
- Deterministic receipt output
- Semantic `ping` reflecting real capacity state

Heptamini governs execution slots, not request rate.

---

## What It Does Not Do (v1 Scope)

- No dashboards or UI
- No distributed coordination
- No token counting or model introspection
- No backend polling
- No hidden queueing
- No policy scoring, strike systems, or cooldown logic

Heptamini v1 is ingress governance only.

---

## Binaries

- `heptaminid` — daemon
- `heptaminic` — CLI client

---

## Quick Start

### Build

```bash
go test ./...
go build ./...
```

### Run Daemon

```bash
./heptaminid \
  --listen 0.0.0.0:7777 \
  --max_inflight 4 \
  --idle_timeout 60s \
  --max_line_bytes 262144 \
  --max_jobs_per_conn 64
```

### Health Check

```bash
./heptaminic --addr 127.0.0.1:7777 --ping
echo $?
```

Exit codes:
- `0` — ready
- `2` — degraded (capacity saturated)
- `1` — error

### Upload

```bash
./heptaminic --addr 127.0.0.1:7777 --file ./payload.bin
```

If capacity is saturated, the server returns:

- `status = yield`
- `retry_hint_ms`

Clients retry using the provided hint.

---

## Backend

Heptamini v1 includes a minimal stub backend for validation and testing.

Backend adapters (e.g., HTTP-based) can be layered behind the governor without changing ingress behavior.

---

## Design Principle

Under load, the server remains bounded and responsive.

Heptamini converts overload from a server-side queueing problem into a client-side retry decision.

No hidden work. No silent buffering. Deterministic admission. 

Behavior under burst (max_inflight=1, stub_ms=10000, 20 clients)

State transitions (sampled):
  t0   inflight=0  executed_total=0  yielded_total=0
  t=44  inflight=1  executed_total=1  yielded_total=19
  t=79  inflight=0  executed_total=1  yielded_total=19

  inflight:  ..........████████████████..........

Interpretation:
- inflight never exceeds 1 (bounded concurrency)
- 1 request executed, 19 yielded immediately (explicit backpressure)
- no queue growth inside the server

Heptamini is intentionally narrow. Feature requests outside ingress governance will be closed.


# License

Apache License 2.0. See LICENSE.
