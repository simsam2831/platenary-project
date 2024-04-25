import requests
from hdfs import InsecureClient
import json
import traceback

# Configuration HDFS
hdfs_namenode = 'http://localhost:9870'
hdfs_path = '/user/hadoop/data/'

# Configuration du client HDFS
hdfs_client = InsecureClient(hdfs_namenode, user='hadoop')


def probe_request():
    container_ip="172.24.0.7"
    res= requests.get(f"http://{container_ip}:5000/lastscan")
    hdfs_file_path = hdfs_path + 'data.txt'
    msg=json.dumps(res.json()).encode("utf-8")
    with hdfs_client.write(hdfs_file_path, overwrite=True) as writer:
        writer.write(msg.value() + b'\n')
        

if __name__ == "__main__":
    try:
        x=0
        while x<20:
            probe_request()
            x+=1
    except Exception:
        print(traceback.format_exc())