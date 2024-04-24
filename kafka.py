import requests
from confluent_kafka import Consumer, KafkaError, Producer
from hdfs import InsecureClient
import json

# Configuration Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'topic1'

# Configuration HDFS
hdfs_namenode = 'http://localhost:9870'
hdfs_path = '/user/hadoop/data/'

# Configuration du client HDFS
hdfs_client = InsecureClient(hdfs_namenode, user='hadoop')

# Configuration Kafka producer
conf_p ={
    'bootstrap.servers': kafka_bootstrap_servers
}

# Configuration du client Kafka Consumer
conf_c = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
producer = Producer(conf_p)
consumer = Consumer(conf_c)
consumer.subscribe([kafka_topic])

def produce_message(data):
    producer.produce(kafka_topic,json.dumps(data).encode('utf-8'))
    producer.flush()

def consume_message():

    # Consommation des messages Kafka et écriture dans HDFS
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        hdfs_file_path = hdfs_path + 'data.txt'
        with hdfs_client.write(hdfs_file_path, overwrite=True) as writer:
            writer.write(msg.value() + b'\n')

def probe_request():
    container_ip="172.24.0.7"
    res= requests.get(f"http://{container_ip}:5000/lastscan")
    produce_message(res)

if __name__ == "__main__":
    probe_request()
    producer.close()
    consumer.close()