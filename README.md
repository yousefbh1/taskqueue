TaskQueue

Built using: Go, gRPC, RabbitMQ, Redis, Docker, GitHub Actions (CI/CD)

A distributed task queue system I developed, demonstrating enterprise-grade messaging and worker orchestration. Jobs are submitted via a gRPC API, which validates requests, enforces idempotency, writes metadata into Redis, and publishes tasks to RabbitMQ with priority scheduling.

Workers consume jobs with fair dispatch (QoS=1), update Redis status (queued → running → succeeded/failed), and handle failures with exponential backoff retries (5s, 30s, 120s). Jobs that exceed max attempts are routed into a dead-letter queue (DLQ).

The entire system runs via Docker Compose, and container images are built/tested/published to GitHub Container Registry using a full CI/CD pipeline.

A demonstration video is available (Demo Video.md) showing:

Building and running the system with Docker

Submitting jobs and tracking their status

Observing retries, DLQ handling, and Redis metadata

Scaling workers horizontally for higher throughput
