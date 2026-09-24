#!/usr/bin/env bash
# Generate transfers to trip the fraud rules. Logs in as alice, fires N rapid
# transfers alice(7)->bob(8) to trigger the velocity rule (>5 in 60s).
# Usage: ./loadgen.sh [count] [amount]
set -euo pipefail
BASE=${BASE:-http://localhost:8080/api/v1}
COUNT=${1:-8}
AMOUNT=${2:-100}

TOK=$(curl -s -X POST "$BASE/auth/login" -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"secret123"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

echo "firing $COUNT transfers of $AMOUNT ..."
for i in $(seq 1 "$COUNT"); do
  KEY=$(python3 -c 'import uuid;print(uuid.uuid4())')
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/transfers" \
    -H "Authorization: Bearer $TOK" -H "Idempotency-Key: $KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"from_account_id\":7,\"to_account_id\":8,\"amount\":$AMOUNT,\"currency\":\"USD\"}")
  echo "  tx $i -> HTTP $code"
done
