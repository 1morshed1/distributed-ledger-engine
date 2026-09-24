"""Shared helpers for the ledger DAGs: DB connection + MinIO (S3) client.

Config comes from env (set in docker-compose airflow-env). Keeping this here means
the DAGs stay declarative and the connection details live in one place.
"""
import io
import json
import os

import boto3
import psycopg2
from botocore.config import Config
from psycopg2.extras import RealDictCursor


def ledger_conn():
    return psycopg2.connect(
        host=os.environ["LEDGER_DB_HOST"],
        port=os.environ.get("LEDGER_DB_PORT", "5432"),
        dbname=os.environ["LEDGER_DB_NAME"],
        user=os.environ["LEDGER_DB_USER"],
        password=os.environ["LEDGER_DB_PASSWORD"],
        cursor_factory=RealDictCursor,
    )


def _drop_expect_header(request, **kwargs):
    # boto3 sends `Expect: 100-continue` on PUTs; s3mock mishandles the
    # continue handshake and drops the connection. Removing the header makes
    # boto3 send the body directly.
    request.headers.pop("Expect", None)


def s3_client():
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        # Path-style is required by s3mock (and most S3-compatible dev servers).
        config=Config(s3={"addressing_style": "path"}),
    )
    client.meta.events.register("before-send.s3", _drop_expect_header)
    return client


def ensure_bucket(s3=None):
    """Create the reports bucket if missing (idempotent). LocalStack starts empty."""
    s3 = s3 or s3_client()
    bucket = os.environ["S3_BUCKET"]
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
    return bucket


def upload_bytes(key: str, data: bytes, content_type: str):
    s3 = s3_client()
    bucket = ensure_bucket(s3)
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    return f"s3://{bucket}/{key}"


def upload_json(key: str, obj) -> str:
    return upload_bytes(key, json.dumps(obj, indent=2, default=str).encode(), "application/json")


def upload_csv(key: str, header, rows) -> str:
    import csv
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return upload_bytes(key, buf.getvalue().encode(), "text/csv")
