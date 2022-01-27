from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
from db import *
import time
import pandas as pd

time.sleep(10.0)

db, cursor = createConnection()
# createSensorDataTableIfNotExists(cursor)
createAudioTableIfNotExists(cursor)
# createLightTableIfNotExists(cursor)

conf = {'bootstrap.servers': "localhost:9092", 'group.id': "foo", 'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# average over n data sample
average_n = 20

running = True


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        count = 0
        temperature_values = []
        humidity_values = []
        average_timestamp = 0
        average_label = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                message = msg.value().decode("UTF-8")
                if message.startswith("Sound") or message.startswith("Humidity") or message.startswith("Temperature") \
                        or message.startswith("Light"):
                    print(message)
                    sensor_data = str(message).split(":")
                    record = sensor_data[1].split(" ")[1:4]
                    record = [float(i) for i in record]
                    print(record)
                    if sensor_data[0] == "Sound":
                        insertIntoAudioTable(db, cursor, record)
                    elif sensor_data[0] == "Light":
                        insertIntoLightTable(db, cursor, record)
                    else:
                        if count <= average_n:
                            if sensor_data[0] == "Temperature":
                                temperature_values.append(record[0])
                            else:
                                humidity_values.append(record[0])
                            average_timestamp += record[1]
                            average_label += record[2]
                            count += 1
                        else:
                            average_timestamp = round(average_timestamp / average_n)
                            average_label = round(average_label/average_n)

                            average_temperature = sum(temperature_values) / len(temperature_values)
                            average_humidity = sum(humidity_values) / len(humidity_values)

                            insertIntoSensorDataTable(db, cursor, [average_temperature, average_humidity,
                                                                   average_timestamp, average_label])
                            count = 0
                            temperature_values = []
                            humidity_values = []
                            average_timestamp = 0
                            average_label = 0

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


print('waiting\n')
basic_consume_loop(consumer, ["audio"])

closeConnection(db, cursor)
