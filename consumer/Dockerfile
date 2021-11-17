FROM ubuntu:latest



RUN apt-get update && apt-get install -y --no-install-recommends \

python3.5 \

python3-pip \

&& \

apt-get clean && \

rm -rf /var/lib/apt/lists/*



RUN pip3 install confluent-kafka mysql-connector-python
COPY ./consumer.py /consumer.py
COPY ./db.py /db.py


ENTRYPOINT [ "python3" , "/consumer.py"]
#
# FROM python:3.8.2
# CMD pip install confluent-kafka mysql-connector-python
# COPY ./consumer.py /consumer.py
# ENTRYPOINT ["python3", "/consumer.py"]