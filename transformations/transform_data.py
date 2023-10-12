from datetime import datetime


def filter_data(data_set_1, data_set_2):
    # Filter data_set_1 data to keep only the recovery data
    data_set_1 = [{k: v for k, v in entry.items() if k == "Date" or k == "Recovered" or k == "Country"} for entry in data_set_1]
    # Identify common dates between the two datasets
    common_dates = set(entry["date"] for entry in data_set_2) & set(entry["Date"] for entry in data_set_1)
    
    # Filter both datasets to only include data for the United States
    data_set_1 = [entry for entry in data_set_1 if entry.get("Country") == "US"]
    # Identify common dates between the two datasets
    common_dates = set(entry["date"] for entry in data_set_2) & set(entry["Date"] for entry in data_set_1)


    # Create a new merged dataset using the common dates and the recovery data from Johns Hopkins
    merged_data = []
    for date in common_dates:
        data_entry_2 = next((entry for entry in data_set_2 if entry["date"] == date), None)
        data_entry_1 = next((entry for entry in data_set_1 if entry["Date"] == date), None)
        if data_entry_2 and data_entry_1:
            merged_entry = {
                "date": datetime.strptime(date, '%Y-%m-%d').date(),
                "recovered": int(data_entry_1["Recovered"]),
                "confirmed cases": int(data_entry_2["cases"]),
                "deaths": int(data_entry_2["deaths"]),
                "country": data_entry_1['Country']
            }
            merged_data.append(merged_entry)
    # Sort merged_data by Desc dates
    sort_merged_by_date = sorted(
        merged_data,
        key=lambda x: x['date'], reverse=False
    )
    
    # Check for duplicate dates and remove from list
    sort_merged_by_date = remove_duplicate_dates(sort_merged_by_date, "date")

    # Insert ids into dictionaries
    covid_data = []
    for index, entry in enumerate(sort_merged_by_date, start=1):
        if entry:
            new_data = {'id': index}
            new_data.update(entry)
            covid_data.append(new_data)

    return covid_data


def remove_duplicate_dates(dict_list, date_key):
    # Create an empty set to keep track of seen dates
    seen_dates = set()
     # Create an empty list to store unique 
    unique = []
    for entry in dict_list:
        # Get the date from the dictionary using the specified date key
        date = entry.get(date_key)
        # Check if the date is not None and has not been seen before
        if date is not None and date not in seen_dates:
             # If it's a new date, add the dictionary to the unique list
            unique.append(entry)
            # Mark the date as seen by adding it to the set
            seen_dates.add(date)
    return unique
            