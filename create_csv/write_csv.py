import os
import csv

def write_csv(dataset):
    # Check if dataset is not empty before creating the CSV
    if dataset:
        # Define the folder path where you want to save the CSV file
        folder_path = "./csv"  # Replace with your actual folder path

        # Create a CSV file path
        csv_file_path = f"{folder_path}/merged_covid_data.csv"
        # Check if the file already exists and delete it. Data will be updated by NY Times and JHU daily
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)

        # Convert the dataset into a CSV file
        with open(csv_file_path, mode='w', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=dataset[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(dataset)
        return csv_file_path