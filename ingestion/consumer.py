import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

BUCKET = os.environ["S3_BUCKET"]
FLUSH_SECONDS = 600

##### da Kafka a S3 in formato Parquet

def to_row(msg):
    # estrae colonne utili
    # trip_id, 
    # route_id, 
    # feed_timestamp), 
    # il JSON completo in payload 
    # la provenienza (kafka_partition, kafka_offset), 
    # e accumula tutto in buffer in memoria;
    value = json.loads(msg.value())
    trip = value.get("trip", {})
    return {
        "trip_id": trip.get("trip_id"),
        "route_id": trip.get("route_id"),
        "feed_timestamp": int(value.get("feed_timestamp", 0)),
        "payload": msg.value().decode("utf-8"),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
    }


def main():
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id": "s3-writer", # Kafka ricorda fin dove ha letto questo gruppo (gli offset). 
        #Se aggiungessi un secondo consumer con lo stesso group.id, le 3 partizioni verrebbero divise tra i due.
        "auto.offset.reset": "earliest", # la prima volta, quando non c’è ancora un offset salvato, parte dall’inizio del topic
        "enable.auto.commit": False, # l’offset lo salva lui, a mano, e solo quando decide.
    })
    consumer.subscribe(["trip_updates"])
    s3 = boto3.client("s3")
    buffer, last_flush = [], time.time()

    while True:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            buffer.append(to_row(msg))
        # ogni 600 secondi, se il buffer non è vuoto, scrive un Parquet in memoria
        if time.time() - last_flush >= FLUSH_SECONDS:
            if buffer:
                now = datetime.now(timezone.utc)
                # (BytesIO, senza file su disco)
                sink = io.BytesIO()
                # carica su raw/trip_updates/date=YYYY-MM-DD/HHMMSS.parquet
                # la cartella date=YYYY-MM-DD serve a partitionare i dati in S3, così da poter fare query più veloci con Athena/Presto/Trino
                # ed essere riconosciuti da Databricks.
                pq.write_table(pa.Table.from_pylist(buffer), sink)
                key = f"raw/trip_updates/date={now:%Y-%m-%d}/{now:%H%M%S}.parquet"
                s3.put_object(Bucket=BUCKET, Key=key, Body=sink.getvalue())
                # solo dopo l’upload riuscito chiama consumer.commit().
                consumer.commit(asynchronous=False)
                print(f"Scritti {len(buffer)} record in {key}", flush=True)
            buffer, last_flush = [], time.time()

# Perché il commit va dopo l’upload: 
# se il container cade prima della scrittura su S3, 
# l’offset non è stato salvato. 
# Al riavvio rilegge gli stessi messaggi e non perde niente. 
# Il prezzo è che a volte un messaggio viene scritto due volte (at-least-once).
# I duplicati li toglierai nella silver, ed è anche per questo che salvi kafka_partition e kafka_offset.

if __name__ == "__main__":
    main()