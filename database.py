import psycopg2
import os
from dotenv import load_dotenv
from sql.covid_table import ( CREATE_COVID_DATA_TABLE )
from sql.upload_covid_data import ( UPLOAD )

load_dotenv()

url = os.getenv("DATABASE_URL")

connection = psycopg2.connect(
    host = os.getenv("DB_HOST"),
    database = os.getenv("DB_NAME"),
    port = os.getenv("DB_PORT"),
    user = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
)

def connect():
    # Connect to PostgreSQL
    postgresql_connection = connection
    try:
        if postgresql_connection:
            # create cursor
            cursor = postgresql_connection.cursor()
            # exectute test statement
            cursor.execute('SELECT version()')
            
            
            # display version and connection
            return {
                "PostgreSQL": cursor.fetchone(),
                "Connection": "Connected to PostgreSQL DB..."
            }, cursor.close() # close communication with PostgreSQL
            

    except (Exception, psycopg2.DatabaseError) as e:
        return {
            "Error Message": str(e)
        }

def create_covid_data_table():
    postgresql_connection = connection
    try:
        if postgresql_connection:
            cursor = postgresql_connection.cursor()
            cursor.execute(CREATE_COVID_DATA_TABLE)
            
            return {
                "PostgreSQL": cursor.statusmessage(),
            }, cursor.close() # close communication with PostgreSQL
    except (Exception, psycopg2.DatabaseError) as e:
        return {
            "Error Message": str(e)
        }

def upload_data():
    postgresql_connection = connection
    try:
        if postgresql_connection:
            cursor = postgresql_connection.cursor()
            cursor.execute(UPLOAD)
            
            return {
                "PostgreSQL": cursor.fetchall(),
                "Connection": cursor.statusmessage
            }, cursor.close()
    except (Exception, psycopg2.DatabaseError) as e:
        return {
            "Error Message": str(e)
        }

