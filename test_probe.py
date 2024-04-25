import requests
from hdfs import InsecureClient
import json
import traceback
from planets import use_planets
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

# Configuration HDFS
hdfs_namenode = 'http://localhost:9870'
hdfs_path = '/user/hadoop/data/'

# Configuration du client HDFS
hdfs_client = InsecureClient(hdfs_namenode, user='hadoop')
hdfs_file_path = hdfs_path + 'data.txt'


def probe_request():
    container_ip="172.24.0.7"
    res= requests.get(f"http://{container_ip}:5000/lastscan")
    return res.json()  
 
def prediction_model(input_data):

    model_path = "bestmodel/data"
    model = PipelineModel.load(model_path)
    
    preprocessed_data = use_planets(input_data)

    predictions = model.predict(preprocessed_data)

    predictions.show()

def save_hdfs(data):
    msg=json.dumps(data).encode("utf-8")
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