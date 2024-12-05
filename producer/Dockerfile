FROM python:3.10-slim

RUN pip install confluent-kafka

COPY producer.py /app/producer.py

WORKDIR /app

ENTRYPOINT ["python", "producer.py"]
