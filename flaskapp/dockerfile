FROM python:3.10

WORKDIR /app

COPY /flaskapp . 

RUN pip install -r requirements.txt
RUN pip install confluent-kafka

EXPOSE 5000

CMD ["python", "probe.py"]
