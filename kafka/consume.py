from confluent_kafka import Consumer, KafkaException, KafkaError

# Configure the Consumer
c = Consumer({
    'bootstrap.servers': 'localhost:19092',  # Assuming you're running this on the same machine as the compose
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
})

# Subscribe to the topic
c.subscribe(['test_topic'])

# Process messages
try:
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            print('Received message: {}'.format(msg.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    c.close()
