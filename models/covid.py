import psycopg2
import os
from dotenv import load_dotenv
from sql.covid_table import ( CREATE_COVID_DATA_TABLE, TABLE_EXISTS, UPLOAD, GET_COVID_TABLE, GET_COVID_DATA_BY_ID, DROP_TABLE )
from psycopg2 import sql
from database import connection

# Load environment variables from a .env file
load_dotenv()

# instantiate a class that holds a schema for the covid data as methods

class Covid_Store:
    # Get Table
    def index(self):
        postgresql_connection = connection
        try:
            cursor = postgresql_connection.cursor()
            # Execute SQL statement to get all covid data from the 'covid_data' table
            cursor.execute(GET_COVID_TABLE)
            # Get the column names from the query result description
            column_names = [desc[0] for desc in cursor.description]
            covid_data = cursor.fetchall()

            if covid_data:
                cursor.close()
                # Create a list of dictionaries, where each dictionary represents a row of data.
                # The zip function combines the column names with the row data, and the dict() function
                # create a dictionary where the column names are keys and the row data is values.
                result = [dict(zip(column_names, row)) for row in covid_data]
                return result
            else:
                cursor.close()
                return {'Error': "No data found!"}
        except Exception as e:
            return {"Error" : str(e)}
    
    # Get data by id
    def show(self, id):
        postgresql_connection = connection
        try:
            cursor = postgresql_connection.cursor()
            # Execute SQL statement to get data by id from the 'covid_data' table
            cursor.execute(GET_COVID_DATA_BY_ID, (id))
            # Get the column names from the query result description
            column_names = [desc[0] for desc in cursor.description]
            data = cursor.fetchone()

            if data:
                cursor.close()
                # create a dictionary where the column names are keys and the row data is values.
                result = dict(zip(column_names, data))
                return result
            else:
                cursor.close()
                return {"Error": "Could not fetch data by id."}
        except Exception as e:
            return {"Error": str(e)}
    # Create covid data table
    def create(self):
        postgresql_connection = connection
        try:
            cursor = postgresql_connection.cursor()

            # Execute the SQL statement to create the 'covid_data' table
            cursor.execute(CREATE_COVID_DATA_TABLE)
            postgresql_connection.commit()

            # Check if the 'covid_data' table exists in the database
            cursor.execute(TABLE_EXISTS)
            postgresql_connection.commit()
            table_exists = cursor.fetchone()[0]

            if table_exists:
                print("Table exists:", table_exists)
                cursor.close()
            return {
                "PostgreSQL": cursor.statusmessage,
            }

        except (Exception, psycopg2.DatabaseError) as e:
            return {
                "Error Message": str(e)
            }
    # Upload data from csv file- data is updated by John Hopkins and NY Times    
    def update(self):
         postgresql_connection = connection
         folder_path = "./csv"  # actual folder path

         # Create a CSV file path
         csv_file_path = f"{folder_path}/us_covid_data.csv"
         try:
            if postgresql_connection:
                cursor = postgresql_connection.cursor()
                if os.path.exists(csv_file_path):
                    # Use copy_expert to upload data from a local file
                    with open(csv_file_path, 'r') as file:
                        cursor.copy_expert(sql.SQL(UPLOAD), file)

                    postgresql_connection.commit()
                cursor.close()
                return {
                    "Connection": cursor.statusmessage
                }
         except (Exception, psycopg2.DatabaseError) as e:
            print(str(e))
            return {
                "Error Message": str(e)
            }
    # Delete covid data table from database
    def delete(self):
        postgresql_connection = connection
        try:
            cursor = postgresql_connection.cursor()
            cursor.execute(DROP_TABLE)
            postgresql_connection.commit()
            cursor.close()
        except(Exception, psycopg2.DatabaseError) as e:
            return {
                "Error Message": str(e)
            }