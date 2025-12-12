FROM python:3.11

WORKDIR /app
COPY docker-kafka/producer/requirements.txt .
COPY docker-kafka/producer/producer.py .
COPY data_generator/data/raw/cart_events.json .
COPY data_generator/data/raw/click_events.json .
COPY data_generator/data/raw/navigation_events.json .
COPY data_generator/data/raw/purchase_events.json .

RUN pip install -r requirements.txt

CMD ["python", "producer.py"]