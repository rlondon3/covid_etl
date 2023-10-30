from transformations.extract_data import extract
from database import ( connect, create_covid_data_table, upload_data )
import csv

extract()

def connect_db():
    connected = connect()
    print(connected)
    if connected:
        create_covid_data_table()
        upload_data()

connect_db()