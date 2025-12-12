import json
import time
import random
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

TOPIC = os.environ.get("KAFKA_TOPIC", "events")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
FILES = [
    "cart_events.json",
    "click_events.json",
    "navigation_events.json",
    "purchase_events.json"
]

def load_events(files):
    events = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as infile:
                first = infile.readline().strip()
                infile.seek(0)
                if not first:
                    continue
                # si le fichier est un tableau JSON
                if first.startswith("["):
                    infile.seek(0)
                    events += json.load(infile)
                else:
                    # ligne-par-ligne JSON (one JSON per line)
                    for line in infile:
                        line = line.strip()
                        if line:
                            events.append(json.loads(line))
        except FileNotFoundError:
            print(f"[producer] fichier non trouvé: {f}, skipped")
        except Exception as e:
            print(f"[producer] erreur lecture {f}: {e}")
    return events

def make_producer():
    backoff = 1
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                # on laisse la détection automatique de version
                api_version_auto_timeout_ms=60000,  # 60s si nécessaire
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            # test simple pour s'assurer que le broker répond
            p.bootstrap_connected()  # True/False
            print(f"[producer] connecté à Kafka {BOOTSTRAP}")
            return p
        except NoBrokersAvailable as e:
            print(f"[producer] Kafka non disponible, retry dans {backoff}s... ({e})")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as e:
            print(f"[producer] erreur création producer: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

def main():
    events = load_events(FILES)
    if not events:
        print("[producer] aucun événement chargé — vérifiez les fichiers.")
    producer = make_producer()
    sent = 0
    try:
        while True:
            if events:
                event = random.choice(events)
            else:
                # fallback: évènement minimal pour tester connectivity
                event = {"ts": time.time(), "type": "heartbeat"}
            try:
                future = producer.send(TOPIC, event)
                # optionnel: attendre le résultat (bloquant)
                result = future.get(timeout=10)
                sent += 1
                if sent % 10 == 0:
                    producer.flush()
                print("[producer] envoyé:", event)
            except KafkaError as ke:
                print(f"[producer] erreur envoi kafka: {ke}")
                # essayer de re-créer le producer si nécessaire
                producer = make_producer()
            time.sleep(1)
    except KeyboardInterrupt:
        print("[producer] interruption par user, flush et exit")
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
