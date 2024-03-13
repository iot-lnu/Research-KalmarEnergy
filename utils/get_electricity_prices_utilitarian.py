import httpx
import json
import pandas as pd
from datetime import datetime, timedelta

# Define region codes
regionCodes = ['SE4']

# Function to fetch data
# Time is in UTC and prices are in €/MWh
def fetchData(year, month, day, regionCode):
    print(f"https://spot.utilitarian.io/electricity/{regionCode}/{year}/{str(month).zfill(2)}/{str(day).zfill(2)}")
    response = httpx.get(f"https://spot.utilitarian.io/electricity/{regionCode}/{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/",
                         follow_redirects=True)
    
    print(response.status_code)
    print(response.text)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data for region {regionCode}")
    print(response.text)
    data = json.loads(response.text)
    return pd.DataFrame(data)

# Function to fetch data for a date range
def fetchDataForDateRange(start_date, end_date):
    # Parse start and end dates
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Initialize dataframe to store data
    df_list = []

    while start_date <= end_date:
        # Fetch data for each region and store it
        for region in regionCodes:
            df_list.append(fetchData(start_date.year, start_date.month, start_date.day, region))

        # Increment day
        start_date += timedelta(days=1)

    return pd.concat(df_list, ignore_index=True)

# Fetch data
df = fetchDataForDateRange("2023-01-01", "2023-12-31")

df.to_csv('data/electricity_prices_2023.csv')