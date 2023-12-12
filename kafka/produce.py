from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Configure the Producer
p = Producer({
    'bootstrap.servers': 'localhost:19092',  # Assuming you're running this on the same machine as the compose
    'client.id': 'python-producer'
})

# The topic you want to publish to
topic = 'test_topic'

# Produce a message
try:
    p.produce(topic, key='key', value='value', callback=delivery_report)
except Exception as e:
    print(str(e))

# Wait for any outstanding messages to be delivered
p.flush()
