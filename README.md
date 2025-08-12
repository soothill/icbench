# icbench

A simple, fast CLI to benchmark S3 and S3‑compatible object storage. It measures latency, throughput, and RPS for common operations with controlled concurrency and retries.

Supported benchmarks
- prepare: create a dataset of objects for read/head/retention tests
- get-object: download existing objects to measure latency and MB/s
- head-object: metadata lookups to measure latency only
- put-object: upload new objects to measure write latency and MB/s
- put-object-retention: set object lock retention on existing objects
- get-object-retention: fetch object lock retention on existing objects
- cleanup: delete test objects by prefix
- delete-all: delete every object in a bucket (destructive)

Requirements
- Go 1.22+
- AWS credentials via flags or standard AWS SDK chain (env vars, shared config, etc.)
- Network access to your S3 or S3‑compatible endpoint

Install
- From source: go install ./...
  - In this repo directory: `go install`
  - The `icbench` binary will be placed in `$(go env GOPATH)/bin`.

Quick start
1) Create a small dataset:
   `icbench prepare -b my-bucket -k bench/ic -n 200 -c 20 -s 64k --endpoint-url http://localhost:9000 --region us-east-1 --access-key minio --secret-key minio123`
2) Run a read benchmark:
   `icbench get-object -b my-bucket -k bench/ic -n 200 -c 40 -s 64k --endpoint-url http://localhost:9000`
3) Clean up:
   `icbench cleanup -b my-bucket -k bench/ic --endpoint-url http://localhost:9000`

Global flags
- `--endpoint-url`: Custom S3 endpoint (e.g., http://localhost:9000)
- `--access-key`, `--secret-key`: Static credentials (optional; otherwise uses AWS default chain)
- `-r, --region`: AWS region (default: us-east-1)
- `-b, --bucket`: Bucket name
- `-k, --key`: Object key base (prefix) used as `<prefix>-<index>`
- `-n, --requests`: Number of requests/objects (default: 100)
- `-c, --concurrency`: Number of concurrent requests (default: 10)
- `--json`: Emit JSON metrics instead of human-readable output
- `-d, --debug`: Verbose logs and AWS request logging
- `--use-virtual-hosted-style`: Use DNS/virtual hosted addressing instead of path-style

Size flag format
- Accepts raw bytes or suffix k/m (lower/upper case OK): `1024`, `64k`, `2m`

Output
- Human-readable: totals, RPS, latency distribution (avg/min/max/p50/p90/p95/p99) and bandwidth (for get/put)
- JSON (with `--json`): machine-parseable stats with `latency` and `bandwidth` objects

Command reference and examples

prepare — create dataset
- Purpose: Upload N random objects of given size using `<prefix>-<i>` key pattern
- Key flags: `-b, -k, -n, -c, -s`
- Example:
  `icbench prepare -b my-bucket -k bench/ic -n 1000 -c 50 -s 256k`
- JSON output:
  `icbench prepare ... --json`

get-object — read existing objects
- Purpose: Measure latency and MB/s when reading objects created by prepare
- Key flags: `-b, -k, -n, -c, -s`
- Note: `-s` is used to compute bandwidth (expected size)
- Example (human):
  `icbench get-object -b my-bucket -k bench/ic -n 2000 -c 200 -s 256k`
- Example (json):
  `icbench get-object ... --json`

head-object — metadata lookups
- Purpose: Measure HEAD latency on existing objects (no bandwidth)
- Key flags: `-b, -k, -n, -c`
- Example:
  `icbench head-object -b my-bucket -k bench/ic -n 5000 -c 300`

put-object — create new objects
- Purpose: Upload N random objects; measures latency and MB/s
- Key flags: `-b, -k, -n, -c, -s`
- Example:
  `icbench put-object -b my-bucket -k bench/put -n 1000 -c 100 -s 1m`

put-object-retention — set object lock
- Purpose: Call PutObjectRetention on existing objects
- Key flags: `-b, -k, -n, -c, --mode, --retain-until-date`
- `--mode`: `COMPLIANCE` or `GOVERNANCE` (default: COMPLIANCE)
- `--retain-until-date`: RFC3339 (e.g., 2025-12-31T15:04:05Z)
- Example:
  `icbench put-object-retention -b my-bucket -k bench/ic -n 500 -c 50 --mode COMPLIANCE --retain-until-date 2025-12-31T00:00:00Z`

get-object-retention — fetch object lock
- Purpose: Call GetObjectRetention on existing objects
- Key flags: `-b, -k, -n, -c`
- Example:
  `icbench get-object-retention -b my-bucket -k bench/ic -n 500 -c 50`

cleanup — delete by prefix
- Purpose: Delete all objects that match `<prefix>-*`
- Key flags: `-b, -k, -n, -c` (concurrency controls deletion batches)
- Example:
  `icbench cleanup -b my-bucket -k bench/ic`
- JSON output:
  `icbench cleanup ... --json`

delete-all — delete everything (dangerous)
- Purpose: Delete every object in a bucket; irreversible
- Must pass `--force`
- Example:
  `icbench delete-all -b my-bucket --force`

Behavior and implementation notes
- Connection reuse: a single S3 client is created once per run and reused to maximize socket reuse.
- Retries: transient errors (e.g., SlowDown/Throttling/network) are retried with exponential backoff + jitter, up to 5 attempts.
- Concurrency: all operations use a bounded worker pattern controlled by `--concurrency`.
- Cancellation: Ctrl+C stops new work and lets in‑flight requests finish; long listings/deletes also respect cancellation.
- Addressing style: path-style by default; use `--use-virtual-hosted-style` to switch to DNS style.

Credential/config resolution
- If `--access-key/--secret-key` are provided, static credentials are used.
- Otherwise, the AWS SDK default chain applies (env vars, shared config, instance/web identity, etc.).

Troubleshooting
- 403/Signature errors: check region, credentials, endpoint URL, and addressing style.
- Connection refused/DNS issues: verify `--endpoint-url` and network access.
- SlowDown/Throttling: reduce `--concurrency` or scale the backend; the tool already retries with backoff.
- JSON parsing: make sure to use `--json` if piping to tools like `jq`.

