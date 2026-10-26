
import json
import os
import time

import requests
from confluent_kafka import Producer
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

URL = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates"
TOPIC = "trip_updates"
POLL_SECONDS = 30

##### Dal feed a Kafka

def to_message(entity, feed_timestamp):
    # to_message() converte il protobuf in un dizionario (MessageToDict),
    # aggiunge feed_timestamp (il momento in cui Translink ha generato il feed) 
    # e restituisce (trip_id, json).
    """Trasforma un FeedEntity in (chiave, valore JSON)."""
    payload = MessageToDict(entity.trip_update, preserving_proto_field_name=True)
    payload["feed_timestamp"] = int(feed_timestamp)
    # la chiave è trip_id: 
    # Kafka sceglie la partizione facendo un hash della chiave. 
    # Tutti i messaggi di una stessa corsa vanno quindi sempre nella stessa partizione,
    #  e dentro una partizione l’ordine è garantito.
    return entity.trip_update.trip.trip_id, json.dumps(payload)


def poll_once(producer):
    # scarica il feed con requests.get. Il contenuto è protobuf (binario), non JSON;
    # ParseFromString lo decodifica con le classi di gtfs-realtime-bindings;
    # per ogni entity del feed tiene solo quelle che contengono un trip_update (una corsa con i ritardi alle fermate);
    # producer.produce(...) mette il messaggio in un buffer locale. L’invio è asincrono, non immediato;
    # producer.poll(0) gestisce le conferme di consegna già arrivate, senza aspettare;
    # producer.flush() a fine giro attende che tutti i messaggi siano stati consegnati a Kafka.
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(requests.get(URL, timeout=30).content)
    sent = 0
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        key, value = to_message(entity, feed.header.timestamp)
        producer.produce(TOPIC, key=key, value=value)
        producer.poll(0)
        sent += 1
    producer.flush()
    return sent


def main():
    # crea il Producer e gira in un ciclo infinito: 
    # chiama poll_once, poi dorme 30 secondi. 
    # Il try/except serve a non far morire il processo quando il feed Translink non risponde: 
    # stampa l’errore e riprova al giro successivo.
    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")})
    while True:
        try:
            print(f"Inviati {poll_once(producer)} messaggi", flush=True)
        except Exception as exc:  # il feed a volte non risponde: si riprova al giro dopo
            print(f"Errore: {exc}", flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()