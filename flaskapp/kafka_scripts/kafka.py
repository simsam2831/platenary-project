import requests
from confluent_kafka import Consumer, KafkaError, Producer
from hdfs import InsecureClient
import json
import time

# Configuration Kafka
kafka_bootstrap_servers = '172.24.0.12:9092'
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
    print('Producing message to Kafka')
    producer.produce(kafka_topic,json.dumps(data).encode('utf-8'))
    producer.flush()

def consume_message():
    print('Consuming messages from Kafka and writing to HDFS')
    i=0
    while True :
        msg = consumer.poll(timeout=1.0)
        print (f"boucle n°{i}")
        if msg :
            print('Received message')
            hdfs_file_path = hdfs_path + 'data.txt'
            with hdfs_client.write(hdfs_file_path, overwrite=True) as writer:
                writer.write(msg.value() + b'\n')
            break
        i+=1
    print('Message written to HDFS')

def probe_request():
    container_ip="127.0.0.1"
    res= requests.get(f"http://{container_ip}:5000/lastscan")
    produce_message(res.json())

if __name__ == "__main__":
    try:
        while True:
            probe_request()
            consume_message()
            time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()
        consumer.close()