import random
from flask import Flask
import pandas as pd

app=Flask("Space_Probe")

@app.get("/")
def home():
    return {"message":"Hello World"}

@app.get("/lastscan")
def last_scan():
    data=pd.read_csv("data/hwc.csv")
    x=random.randrange(0,data.index[-1])
    res={}
    for col in data.columns:
        res[col]=data[col][x]
    return {x:res}
    

if __name__ == "__main__":
    app.run(port=5000)