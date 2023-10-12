import requests
import csv
import io

# Function to fetch and parse CSV data from a URL
def fetch_and_parse_csv(url):
    response = requests.get(url)

    if response.status_code == 200:
        # Extract the CSV data from the response
        csv_data = response.text
        # Create a text stream (StringIO) to work with the CSV data
        csv_file = io.StringIO(csv_data)
        # Create a CSV reader that interprets the data as a dictionary
        csv_reader = csv.DictReader(csv_file)
        # Convert the CSV data into a list of dictionaries (each row becomes a dictionary)
        data_list = list(csv_reader)
        return data_list
    else:
         # If the response status code is not 200, return error message
        error_message = f"Failed to fetch data from {url}. HTTP status code: {response.status_code}"
        return error_message