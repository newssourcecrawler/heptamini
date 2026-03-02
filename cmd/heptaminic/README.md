# heptaconnc

Minimal v0 client for Heptaconn (TCP JSON-lines).
It turns a model server into a governed compute service with deterministic admission and bounded concurrency.

You can put Nginx in front of llama-server, but it only governs HTTP transport. It doesn’t understand inference as a bounded job with explicit admission, per-connection strikes, deterministic receipts, or backend concurrency as a first-class constraint.

Nginx can rate-limit requests; Heptaconnd governs expensive compute.

Use Nginx for routing and TLS. Use Heptaconnd when you need deterministic governance over inference compute.

## Run

```bash
go run ./cmd/heptaconnc \
  --addr 127.0.0.1:7777 \
  --file ./payload.bin \
  --chunk 8192 \
  --ttl_ms 60000

  Exit codes:
	•	0: received confirm + done
	•	2: received confirm + abort
	•	1: error / timeout / unexpected protocol

    ---

## Quick demo

Terminal 1:
```bash
go run ./cmd/heptaconnd --listen 0.0.0.0:7777

Terminal 2:
echo -n "hi" > /tmp/p.bin
go run ./cmd/heptaconnc --addr 127.0.0.1:7777 --file /tmp/p.bin --chunk 8

