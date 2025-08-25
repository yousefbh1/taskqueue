#!/usr/bin/env bash
set -euo pipefail
docker exec -i infra-rabbitmq-1 rabbitmqadmin get queue=jobs.dlq requeue=false count=${1:-5} \
| awk '/payload:/ {print substr($0, index($0,$3))}' \
| while read -r body; do
  docker exec -i infra-rabbitmq-1 rabbitmqadmin publish exchange=jobs.direct routing_key=jobs.submit payload="$body"
done
