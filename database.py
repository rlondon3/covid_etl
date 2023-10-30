import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
from sql.covid_table import ( CREATE_COVID_DATA_TABLE, TABLE_EXISTS )
from sql.upload_covid_data import UPLOAD

# Load environment variables from a .env file
load_dotenv()

# Connect to the PostgreSQL database using environment variables
connection = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

# Function to establish a connection and check version
def connect():
    postgresql_connection = connection
    try:
        if postgresql_connection:
            # Create a cursor for database communication
            cursor = postgresql_connection.cursor()
            # Execute a test statement to check the version
            cursor.execute('SELECT version()')

            # Display the version and a connection message
            result = {
                "PostgreSQL": cursor.fetchone(),
                "Connection": "Connected to PostgreSQL DB..."
            }
            cursor.close()  # Close the cursor
            return result  # Return the result

    except (Exception, psycopg2.DatabaseError) as e:
        # Handle any exceptions and return an error message
        return {
            "Error Message": str(e)
        }

# Function to create the 'covid_data' table
def create_covid_data_table():
    postgresql_connection = connection
    try:
        if postgresql_connection:
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

# Function to upload data from a CSV file to the 'covid_data' table
def upload_data():
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

# Call the functions to perform their respective tasks
# For example, you can call connect() to check the version and connection,
# and then call create_covid_data_table() to create the table, and so on.
