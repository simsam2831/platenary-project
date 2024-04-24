FROM python:3.10-alpine

WORKDIR /app

COPY /flaskapp . 

RUN pip install flask
RUN pip install pandas

EXPOSE 5000

CMD ["python", "probe.py"]
