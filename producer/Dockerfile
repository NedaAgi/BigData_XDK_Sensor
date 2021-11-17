FROM ubuntu:latest



RUN apt-get update && apt-get install -y --no-install-recommends \

python3.5 \

python3-pip \

&& \

apt-get clean && \

rm -rf /var/lib/apt/lists/*



RUN pip3 install pyserial confluent-kafka
COPY ./main.py /main.py
ENTRYPOINT [ "python3" , "/main.py"]

# FROM python:3.8.2
# # CMD pip install serial confluent-kafka
# RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir serial
# COPY ./main.py /main.py
# ENTRYPOINT ["python3", "/main.py"]