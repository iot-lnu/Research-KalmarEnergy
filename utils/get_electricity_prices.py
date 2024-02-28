import requests
import pandas as pd
from datetime import datetime, timedelta

# Define region codes
regionCodes = ['SE4']

# Function to fetch data
def fetchData(year, month, day, regionCode):
    response = requests.get(f"https://www.elprisetjustnu.se/api/v1/prices/{year}/{month}-{day}_{regionCode}.json")
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data for region {regionCode}")
    data = response.json()
    return { 'region': regionCode, 'data': data }

# Function to fetch data for a date range
def fetchDataForDateRange(start_date, end_date):
    # Parse start and end dates
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Initialize list to store data
    data_list = []

    while start_date <= end_date:
        # Format month and day
        month = str(start_date.month).zfill(2)
        day = str(start_date.day).zfill(2)
        year = start_date.year

        # Fetch data for each region and store it
        for region in regionCodes:
            data_list.append(fetchData(year, month, day, region))

        # Increment day
        start_date += timedelta(days=1)

    return data_list

# Fetch data
data_list = fetchDataForDateRange("2023-01-01", "2023-12-31")

# Convert data to dataframe
df = pd.DataFrame(data_list)

# Exploding 'data' column into multiple columns and then renaming the time_start to timestamp as requested
df = df.explode('data') 
df = pd.concat([df.drop(['data'], axis=1), df['data'].apply(pd.Series)], axis=1)
df = df.rename(columns={"time_start": "timestamp"})

# Drop time_end column as per the request
df = df.drop(['time_end'], axis=1)

df.to_csv('data/electricity_prices_2023.csv')