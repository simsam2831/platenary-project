from confluent_kafka import Consumer, KafkaError
from hdfs import InsecureClient

# Configuration Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'topic1'

# Configuration HDFS
hdfs_namenode = 'http://localhost:9870'
hdfs_path = '/user/hadoop/data/'

# Configuration du client Kafka Consumer
conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([kafka_topic])

# Configuration du client HDFS
hdfs_client = InsecureClient(hdfs_namenode, user='hadoop')

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

consumer.close()