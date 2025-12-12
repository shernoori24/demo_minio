import json
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC = os.environ.get("KAFKA_TOPIC", "events")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

def make_consumer():
    backoff = 1
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            print(f"[consumer] connecté à Kafka {BOOTSTRAP}, topic={TOPIC}")
            return consumer
        except NoBrokersAvailable as e:
            print(f"[consumer] Kafka non disponible, retry dans {backoff}s... ({e})")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as e:
            print(f"[consumer] erreur création consumer: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

def main():
    consumer = make_consumer()
    try:
        for msg in consumer:
            print(msg.value)
    except KeyboardInterrupt:
        print("[consumer] interruption par user")
    finally:
        try:
            consumer.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
