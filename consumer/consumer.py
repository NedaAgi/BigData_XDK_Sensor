from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
from db import *
import time

time.sleep(10.0)

db, cursor = createConnection()
createAudioTableIfNotExists(db, cursor)
# createTableIfNotExists(db)

conf = {'bootstrap.servers': "localhost:9092", 'group.id': "foo", 'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

running = True


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

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
                if (msg.value().decode("UTF-8")).startswith("Sound"):
                    record = str(msg.value().decode("UTF-8")).split(":")[1].split(" ")[1:3]
                    record = [float(i) for i in record]
                    print(record)
                    insertIntoAudioTable(db, cursor, record)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


print('waiting\n')
basic_consume_loop(consumer, ["audio"])

closeConnection(db, cursor)
