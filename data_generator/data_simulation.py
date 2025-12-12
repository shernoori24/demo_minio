"""
    script de simulation de données pour une plateforme e-commerce. Génère des événements de clics, d'ajouts au panier,
    d'achats et de navigation, puis les enregistre dans des fichiers JSON.
    
    - click_events.json
    - cart_events.json
    - purchase_events.json
    - navigation_events.json
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

# paramètres
NB_USERS = 5000
NB_PRODUCTS = 300
NB_EVENTS = 50000

users = [fake.random_int(min=1, max=NB_USERS) for _ in range(NB_USERS)]
products = [f"P_{i}" for i in range(1000, 1000 + NB_PRODUCTS)]
campaigns = ["CAMP_GREENWEEK", "CAMP_BIOFEST", "CAMP_WINTERSALE", "CAMP_NEWCUSTOMERS"]

start_date = datetime(2025, 10, 20)
end_date = datetime(2025, 10, 28)

def random_timestamp():
    return (start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))).isoformat()

def write_json_file(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        for d in data:
            f.write(json.dumps(d) + "\n")

# Click events
click_events = [{
    "event_id": f"click_{uuid.uuid4().hex[:8]}",
    "user_id": random.choice(users),
    "product_id": random.choice(products),
    "campaign_id": random.choice(campaigns),
    "timestamp": random_timestamp(),
    "source": random.choice(["homepage_banner", "search", "email", "social_media"])
} for _ in range(NB_EVENTS)]

write_json_file("./data_generator/data/raw/click_events.json", click_events)

# Cart events
cart_events = [{
    "event_id": f"cart_{uuid.uuid4().hex[:8]}",
    "user_id": random.choice(users),
    "product_id": random.choice(products),
    "quantity": random.randint(1, 5),
    "timestamp": random_timestamp()
} for _ in range(int(NB_EVENTS * 0.6))]  # 60% des clics deviennent des ajouts panier

write_json_file("./data_generator/data/raw/cart_events.json", cart_events)

# Purchase events
purchase_events = [{
    "event_id": f"purchase_{uuid.uuid4().hex[:8]}",
    "user_id": random.choice(users),
    "order_id": f"ORD_{uuid.uuid4().hex[:6]}",
    "product_id": random.choice(products),
    "quantity": random.randint(1, 3),
    "price": round(random.uniform(5, 60), 2),
    "total_amount": 0,
    "timestamp": random_timestamp(),
    "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "bank_transfer"])
} for _ in range(int(NB_EVENTS * 0.3))]  # 30% d’achats

for e in purchase_events:
    e["total_amount"] = round(e["price"] * e["quantity"], 2)

write_json_file("./data_generator/data/raw/purchase_events.json", purchase_events)

# Navigation events
navigation_events = [{
    "event_id": f"nav_{uuid.uuid4().hex[:8]}",
    "user_id": random.choice(users),
    "page": random.choice(["home", "category", "product_detail", "checkout"]),
    "product_id": random.choice(products),
    "device": random.choice(["mobile", "desktop", "tablet"]),
    "timestamp": random_timestamp(),
    "session_id": f"S_{uuid.uuid4().hex[:6]}"
} for _ in range(NB_EVENTS)]

write_json_file("./data_generator/data/raw/navigation_events.json", navigation_events)

print("Fichiers JSON générés avec succès !")
