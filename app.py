from transformations.transform_data import filter_data
from create_csv.write_csv import write_csv
from create_csv.fetch_and_parse_csv import fetch_and_parse_csv

def extract():
    # Fetch NYT data
    nyt_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    nyt_data = fetch_and_parse_csv(nyt_url)
    # Fetch Johns Hopkins data
    jhu_url = "https://raw.githubusercontent.com/milanowicz/COVID-19-Dataset/master/data/jhu/time_series_covid19_grouped_day_country.csv"
    jhu_data = fetch_and_parse_csv(jhu_url)

    # If data has beened fetched and parsed, combine the datasets
    if jhu_data and nyt_data: 
        merge_data = filter_data(jhu_data, nyt_data)
        # Write csv file with merged data
        transformed_data = write_csv(merge_data)
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/csv',
                'Content-Disposition': f'attachment; filename="merged_covid_data.csv"'
            },
            'body': f"CSV file saved in {transformed_data}"
        }
    else:
        return {
            'statusCode': 204,  # No content
            'body': "No data found for merging."
        }

extract()
