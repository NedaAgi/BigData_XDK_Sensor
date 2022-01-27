import mysql.connector
from mysql.connector import errorcode
import time


def createConnection():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pass1234ReallyStrong",
        database='XDK'
    )

    print(db)
    cursor = db.cursor()

    return db, cursor


def createSensorDataTableIfNotExists(cursor):
    table_query = "CREATE TABLE IF NOT EXISTS SensorData (\
                  temperature DOUBLE,\
                  humidity DOUBLE,\
                  label INT,\
                  timestamp BIGINT(20),\
                  PRIMARY KEY ( timestamp )\
                  );"

    try:
        print("Creating table SensorData if not exists: ")
        cursor.execute(table_query)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")


def createAudioTableIfNotExists(cursor):
    table_query = "CREATE TABLE IF NOT EXISTS Audio (\
                  db DOUBLE,\
                  timestamp BIGINT(20),\
                  label INT\
                  );"

    try:
        print("Creating table Audio if not exists: ")
        cursor.execute(table_query)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")


def createLightTableIfNotExists(cursor):
    table_query = "CREATE TABLE IF NOT EXISTS Light (\
                  light DOUBLE,\
                  label INT,\
                  timestamp BIGINT(20),\
                  PRIMARY KEY ( timestamp )\
                  );"

    try:
        print("Creating table Light if not exists: ")
        cursor.execute(table_query)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")


def insertIntoSensorDataTable(db, cursor, record):

    insert_query = "INSERT INTO SensorData (temperature, humidity, timestamp, label)" \
                   " VALUES (%s, %s, %s, %s)"
    cursor.execute(insert_query, record)

    db.commit()


def insertIntoAudioTable(db, cursor, record):

    insert_query = "INSERT INTO Audio (db, timestamp, label)" \
                   " VALUES (%s, %s, %s)"

    try:
        print("Insert into table Audio")
        cursor.execute(insert_query, record)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")

    db.commit()


def insertIntoLightTable(db, cursor, record):
    insert_query = "INSERT INTO Light (light, timestamp, label)" \
                   " VALUES (%s, %s, %s)"

    try:
        print("Insert into table Light")
        cursor.execute(insert_query, record)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")

    db.commit()


def closeConnection(db, cursor):
    cursor.close()
    db.close()
