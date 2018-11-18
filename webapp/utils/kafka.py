from pykafka import KafkaClient, exceptions, Topic


class Client:
    def __init__(self):
        self.client = KafkaClient(hosts="168.62.162.78:6667")  # fail loudly if unable to connect to a host

    def send(self, topic, payload):
        topic: Topic = self.client.topics.get(topic)
        producer = topic.get_sync_producer()  # synchronously push to Kafka
        producer.produce(payload)  # fail loudly if unable to push to Topic
