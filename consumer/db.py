import mysql.connector
from mysql.connector import errorcode
import time


def createConnection():
    db = mysql.connector.connect(
        host="localhost",
        user="agi",
        password="AgiPassword",
        database='XDK'
    )

    print(db)
    cursor = db.cursor()

    return db, cursor


def createAudioTableIfNotExists(db, cursor):
    table_query = "CREATE TABLE IF NOT EXISTS Audio (\
                  db DOUBLE,\
                  timestamp BIGINT(20)\
                  );"

    try:
        print("Creating table Audio if not exists: ")
        cursor.execute(table_query)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")


def createSensorDataTableIfNotExists(db, cursor):
    table_query = "CREATE TABLE IF NOT EXISTS SensorData (\
                  data_id INT NOT NULL AUTO_INCREMENT,\
                  temperature DOUBLE,\
                  humidity DOUBLE,\
                  luminosity DOUBLE,\
                  label INT,\
                  submission_date DATE,\
                  PRIMARY KEY ( data_id )\
                  );"

    try:
        print("Creating table SensorData if not exists: ")
        cursor.execute(table_query)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")


def insertIntoSensorDataTable(db, cursor, record):

    insert_query = "INSERT INTO SensorData (temperature, humidity, luminosity, label, submission_date)" \
                   " VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(insert_query, record)

    db.commit()


def insertIntoAudioTable(db, cursor, record):

    insert_query = "INSERT INTO Audio (db, timestamp)" \
                   " VALUES (%s, %s)"

    try:
        print("Insert into table Audio")
        cursor.execute(insert_query, record)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")

    db.commit()


def closeConnection(db, cursor):
    cursor.close()
    db.close()
