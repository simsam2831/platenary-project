import requests
from hdfs import InsecureClient
import json
import traceback
from planets import use_planets
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName('planets').getOrCreate()

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

# Configuration HDFS
hdfs_namenode = 'http://localhost:9870'
hdfs_path = '/user/hadoop/data/'

# Configuration du client HDFS
hdfs_client = InsecureClient(hdfs_namenode, user='hadoop')
hdfs_file_path = hdfs_path + 'data.txt'


def probe_request():
    container_ip="172.19.0.8"
    res= requests.get(f"http://{container_ip}:5000/lastscan")
    return res.json()  
 
def prediction_model(input_data):

    model_path = "bestmodel/"
    model = DecisionTreeClassificationModel.load(model_path)
        
    preprocessed_data = use_planets(input_data)
    all_columns = [col for col in preprocessed_data.columns]
    assembler = VectorAssembler(inputCols=all_columns, outputCol="selectedFeatures")
    assembled_data = assembler.transform(preprocessed_data)
    predictions = model.transform(assembled_data)
    predictions["probability","prediction"].show()

    return predictions

def save_hdfs(data):
    msg=json.dumps(data).encode("utf-8")
    with hdfs_client.write(hdfs_file_path, overwrite=True) as writer:
        writer.write(msg.value() + b'\n')

if __name__ == "__main__":
    try:
        x=0
        while x<20:
            res=probe_request()
            pred=prediction_model(res)
            x+=1
    except Exception:
        print(traceback.format_exc())