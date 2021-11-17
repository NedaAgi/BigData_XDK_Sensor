import serial
from confluent_kafka import Producer
import socket
import time

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

print()

ser = serial.Serial(
    port='/dev/tty.usbmodem1442401',
    baudrate=9600,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeout=0)

labeled = True
# LABEL: 0 => Unknown, 1 => Fire, 2 => Not fire
if labeled:
    label = 1
else:
    label = 0

print("connected to: " + ser.portstr)
count = 1

while True:
    if ser.read():
        line = ser.readline()
        timestamp = int(round(time.time() * 1000))
        ts = str(line.decode("UTF-8").strip()) + " " + str(timestamp)
        producer.produce("audio", key="db", value=ts)
        print(str(count) + str(': ') + str(ts))
        count = count+1

ser.close()
