# StreamBridge Performance Benchmarks

This document records the performance characteristics of the StreamBridge event broker under synthetic load. Tests were performed locally using `k6`, stressing both the HTTP publishing ingest and the WebSocket fan-out delivery.

## Local Benchmarking Environment
- **Host**: Windows (Docker Desktop for Postgres/Redis, Go Server running natively)
- **CPU/Memory**: Standard laptop constraints (shared cores between Load Tester and Server)
- **Tooling**: `k6` load testing tool, `docker compose`

## Test Configuration
- **Publishers**: 100 concurrent HTTP clients firing events nonstop.
- **Subscribers**: 500 concurrent WebSocket connections.
- **Duration**: 30 seconds of sustained load.
- **Topology**: Single tenant, single channel (maximum lock contention and fan-out pressure).

---

## Final Results (Local Run)

The system processed over **1.45 million WebSocket message deliveries** with **0.00% HTTP request failures** during the benchmark.

### 1. Ingest (Publishing)
- **Total Published Events**: 31,509 events
- **Average Publish Throughput**: ~1,000 events / sec
- **Publish Latency**:
  - **Median**: 18.8 ms (sub-20 ms)
  - **P(95)**: 100.2 ms
- **Error Rate**: 0.00% (No `429 Too Many Requests` or `500 Internal Server Error`)

### 2. Fan-out (WebSocket Delivery)
- **Connected Clients**: 500 / 500 (100% Success)
- **Total Messages Delivered**: 1,453,884
- **WebSocket Fan-out**: ~40.7K message deliveries / sec
- **Dropped Connections**: 0 (Buffer tuning and slow-consumer eviction prevented OOM crashes)

### 3. Stability
- Sustained 500 concurrent WebSocket clients and continuous HTTP publishing throughout the entire benchmark without dropping a single TCP connection.

## Conclusion
On a local development machine, StreamBridge sustained 500 concurrent WebSocket subscribers, 100 concurrent HTTP publishers, ~1,000 published events/sec, and ~40.7K WebSocket message deliveries/sec with 0.00% HTTP request failures.

The architecture is designed for horizontal scaling by deploying multiple stateless application instances behind a load balancer, backed by PostgreSQL and Redis.