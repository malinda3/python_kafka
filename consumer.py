from confluent_kafka import Consumer, KafkaException

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group', 
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)

topic = 'hello_world'

consumer.subscribe([topic])

print(f"Listening to topic '{topic}'...")

try:
    while True:
       
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue 

        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                print(f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                print(f"Error: {msg.error()}")
        else:
            print(f"Received message: {msg.value().decode('utf-8')} from {msg.topic()} [{msg.partition()}]")
except KeyboardInterrupt:
    print("\nConsumer stopped manually.")
finally:
    consumer.close()
