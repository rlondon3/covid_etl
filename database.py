import psycopg2
import os
from dotenv import load_dotenv

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