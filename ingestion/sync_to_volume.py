# ingestion/sync_to_volume.py
import io
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
from databricks.sdk import WorkspaceClient

# timeout espliciti + retry: se la rete cade, la chiamata fallisce e riprova invece di restare appesa
S3_CONFIG = Config(connect_timeout=10, read_timeout=60,
                   retries={"max_attempts": 10, "mode": "adaptive"})

BUCKET = "btp-lake-patrizio"
VOLUME = "/Volumes/workspace/landing/raw"
PREFIX = "raw/trip_updates/"
WORKERS = 8  # thread paralleli; 5-10 va bene


def list_pending(s3):
    """Elenca prima tutti i file, poi li sposta (non cancella mentre pagina)."""
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    return keys


def move_one(s3, w, key):
    """S3 raw/ -> Volume, poi raw/ -> archive/. Idempotente: se si interrompe, rifarlo è innocuo."""
    relative = key.removeprefix("raw/")
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    w.files.upload(f"{VOLUME}/{relative}", io.BytesIO(body), overwrite=True)
    s3.copy_object(Bucket=BUCKET, Key=f"archive/{relative}",
                   CopySource={"Bucket": BUCKET, "Key": key})
    s3.delete_object(Bucket=BUCKET, Key=key)
    return key


def sync_trip_updates(s3, w):
    keys = list_pending(s3)
    total = len(keys)
    print(f"File da spostare: {total}", flush=True)
    if total == 0:
        return 0

    moved, failed = 0, []
    start = time.time()
    pool = ThreadPoolExecutor(max_workers=WORKERS)
    try:
        futures = {pool.submit(move_one, s3, w, k): k for k in keys}
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                fut.result()
                moved += 1
            except Exception as e:
                failed.append(key)
                print(f"ERRORE {key}: {e}", file=sys.stderr, flush=True)
            done = moved + len(failed)
            if done % 50 == 0 or done == total:
                rate = done / (time.time() - start)
                eta_min = (total - done) / rate / 60 if rate else 0
                print(f"{done}/{total}  ({rate:.1f} file/s, ~{eta_min:.0f} min rimanenti)", flush=True)
    except KeyboardInterrupt:
        print("\nInterrotto: annullo i file in coda, attendo quelli in corso...", flush=True)
        pool.shutdown(wait=True, cancel_futures=True)
        raise
    pool.shutdown(wait=True)

    if failed:
        print(f"{len(failed)} file falliti (restano in raw/, verranno ripresi al prossimo giro)", file=sys.stderr)
    return moved


def upload_static(w, folder="data/gtfs_static"):
    for name in ["routes.txt", "stops.txt", "trips.txt"]:
        with open(f"{folder}/{name}", "rb") as f:
            w.files.upload(f"{VOLUME}/gtfs_static/{name}", f, overwrite=True)
    print("GTFS static caricati", flush=True)


if __name__ == "__main__":
    w = WorkspaceClient()  # legge DATABRICKS_HOST e DATABRICKS_TOKEN
    if "--static" in sys.argv:
        upload_static(w)
    print(f"File spostati: {sync_trip_updates(boto3.client('s3', config=S3_CONFIG), w)}")