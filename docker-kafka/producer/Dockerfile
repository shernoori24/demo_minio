FROM python:3.11

WORKDIR /app
COPY requirements.txt .
COPY producer.py .
COPY cart_events.json .
COPY click_events.json .
COPY navigation_events.json .
COPY purchase_events.json .

RUN pip install -r requirements.txt

CMD ["python", "producer.py"]