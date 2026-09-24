"""Central config for the fraud detector. Env-overridable for later containerization."""
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "transaction-events")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "fraud-alerts")

# Postgres (host mapping: 5433 -> container 5432)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "ledger")
PG_USER = os.getenv("PG_USER", "ledger")
PG_PASSWORD = os.getenv("PG_PASSWORD", "secret")

# Rule thresholds
VELOCITY_MAX_TX = int(os.getenv("VELOCITY_MAX_TX", "5"))        # > N tx per window
VELOCITY_WINDOW = os.getenv("VELOCITY_WINDOW", "60 seconds")
VELOCITY_SLIDE = os.getenv("VELOCITY_SLIDE", "10 seconds")
WATERMARK = os.getenv("WATERMARK", "30 seconds")

ANOMALY_STD_MULT = float(os.getenv("ANOMALY_STD_MULT", "3.0"))  # amount > mean + k*std
ANOMALY_MIN_COUNT = int(os.getenv("ANOMALY_MIN_COUNT", "5"))    # need history first

DRAIN_WINDOW = os.getenv("DRAIN_WINDOW", "5 minutes")
DRAIN_THRESHOLD = int(os.getenv("DRAIN_THRESHOLD", "500000"))   # cents outflow / window

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/ledger-fraud-checkpoints")
