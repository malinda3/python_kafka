from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092', 
    'security.protocol': 'PLAINTEXT'
}

producer = Producer(config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

topic = 'hello_world'

message = f"my first message"
producer.produce(topic, message.encode('utf-8'), callback=delivery_report)

producer.flush()
