from pykafka import KafkaClient, Topic


class Client:  # do not instantiate this client here so as to prevent SocketDisconnectedError
    def __init__(self):
        self.client = KafkaClient(hosts="sandbox-hdp.hortonworks.com:6667")  # fail loudly if unable to connect to a host

    def send(self, topic, msg):
        """
        :param topic: Kafka topic
        :type  topic: bytes
        :param msg: data to be sent to the kafka topic
        :type  msg: bytes
        """
        topic: Topic = self.client.topics[topic]
        producer = topic.get_sync_producer(pending_timeout_ms=20*1000)
        producer.produce(msg)  # fail loudly if unable to push to Topic

    def send_async(self, topic, msg):
        """
        An asynchronous producer to Kafka, use it if pushing messages in bulk
        :param topic:
        :param msg:
        :return:
        """
        topic: Topic = self.client.topics[topic]
        producer = topic.get_producer(linger_ms=0, block_on_queue_full=False, max_queued_messages=10)  # keep linger_ms
        # small else it expects more messages to be produced by the current thread.
        producer.produce(msg)
        print('sent')
