import requests
import pandas as pd
from io import StringIO
import re

# Define the parameters and years you want to process
parameters = [4, 5, 6, 7, 19, 26, 27, 39]  # Updated parameters list
years = [2020, 2021, 2022, 2023]
station_id = "66420"  # Assuming you are using the same station for all parameters and this station id means Kalmar

# Base URL for SMHI Open API
base_url = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{}/station/{}/period/corrected-archive/data.csv"

# Define the column name mappings
column_names_mapping = {
    'Datum': 'Date',
    'Tid (UTC)': 'Time',
    'Vindhastighet': 'Wind Speed',  # For parameter 4
    'Relativ Luftfuktighet': 'Relative Humidity',  # For parameter 6
    'Nederbördsmängd': 'Precipitation',  # For parameter 5 and 7
    'Lufttemperatur': 'Air Temperature',  # For parameters 19, 26, and 27
    'Kvalitet': 'Quality',
    'Från Datum Tid (UTC)': 'From Date Time (UTC)',  # For parameter 5, 19
    'Till Datum Tid (UTC)': 'To Date Time (UTC)',  # For parameter 5, 19
    'Representativt dygn': 'Representative Date',  # For parameter 19
    'Daggpunktstemperatur': 'Dew Point Temperature', # For parameter 39
    # Add more mappings for other parameters as needed
}

# Function to fetch and save the CSV file from SMHI Open API for a parameter
def fetch_and_save_csv(parameter):
    # Construct the API URL
    url = base_url.format(parameter, station_id)
    
    # Make the request to the SMHI Open API
    response = requests.get(url)
    
    if response.status_code == 200:
        # Read the content of the response with the correct encoding
        content = response.content.decode('utf-8')
        
        # Save content to a CSV file for the entire dataset
        csv_filename = f'Weather_P{parameter}_full.csv'
        with open(csv_filename, 'w', encoding='utf-8') as file:
            file.write(content)
        print(f'Saved CSV for parameter {parameter}.')
    else:
        print(f'Failed to fetch data for parameter {parameter}. Status code: {response.status_code}')

def find_header_row(file_name):
    with open(file_name, 'r', encoding='utf-8') as file:
        for i, line in enumerate(file):
            # Check if 'Datum' or 'Från Datum' is in the line
            if 'Datum' in line or 'Från Datum' in line:
                return i
    return None  # If a header row wasn't found, return None


# Function to process the CSV file for a specific year
def process_csv(parameter, year):
    csv_filename = f'Weather_P{parameter}_full.csv'
    
    # Find the header row
    header_row = find_header_row(csv_filename)
    
    # Load the dataset
    df = pd.read_csv(csv_filename, delimiter=';', encoding='utf-8', skiprows=header_row, low_memory=False)
    
    # Rename the columns as specified
    df = df.rename(columns=column_names_mapping)
    
    # Handle parameter-specific columns and conversions
    if parameter == 5:
        # Define the regex pattern for the time part
        time_pattern = re.compile(r'(?<=\d{4}-\d{2}-\d{2}\s)\d{2}:\d{2}:\d{2}')
        # Extract the time part and assign it to a new column called 'Time'
        df['Time'] = df['From Date Time (UTC)'].astype(str).apply(lambda x: time_pattern.search(x).group(0) if time_pattern.search(x) else '')
        df['Date'] = pd.to_datetime(df['Representative Date'], errors='coerce')
        df = df[['Date', 'Time', 'Precipitation', 'Quality']]
    elif parameter == 6:
        # For parameter 6, the 'Time' column should already exist
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df[['Date', 'Time', 'Relative Humidity', 'Quality']]
    elif parameter == 7:
        # For parameter 7, the 'Time' column should already exist
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df[['Date', 'Time', 'Precipitation', 'Quality']]
    elif parameter == 19:
        # Define the regex pattern for the time part
        time_pattern = re.compile(r'(?<=\d{4}-\d{2}-\d{2}\s)\d{2}:\d{2}:\d{2}')
        # Extract the time part and assign it to a new column called 'Time'
        df['Time'] = df['To Date Time (UTC)'].astype(str).apply(lambda x: time_pattern.search(x).group(0) if time_pattern.search(x) else '')
        df['Date'] = pd.to_datetime(df['Representative Date'], errors='coerce')
        df.rename(columns={'Air Temperature': 'Air Temperature 1 Min'}, inplace=True)
        df = df[['Date', 'Time', 'Air Temperature 1 Min', 'Quality']]
    elif parameter == 26:
        df.rename(columns={'Air Temperature': 'Air Temperature 2 Min'}, inplace=True)
        df = df[['Date', 'Time', 'Air Temperature 2 Min', 'Quality']]
    elif parameter == 27:
        df.rename(columns={'Air Temperature': 'Air Temperature 2 Max'}, inplace=True)
        df = df[['Date', 'Time', 'Air Temperature 2 Max', 'Quality']]
    elif parameter == 39:
        # For parameter 39, use 'Dew Point temperature' column
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df[['Date', 'Time', 'Dew Point Temperature', 'Quality']]
    else:  # For parameter 4 and any other parameters
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Wind Speed'] = df.get('Wind Speed', None)  # Get 'Wind Speed' if it exists, else fill with None
        df = df[['Date', 'Time', 'Wind Speed', 'Quality']]

    # Ensure the 'Date' column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Now that 'Date' is a datetime object, filter the DataFrame by the specified year
    df = df[df['Date'].dt.year == year]
    
    # Save the processed DataFrame to a CSV file
    processed_file_name = f'weather_P{parameter}_{year}_processed.csv'
    df.to_csv(processed_file_name, index=False, sep=';', encoding='utf-8')
    print(f'Processed and saved {processed_file_name}')

# Main loop to fetch and process the data
for parameter in parameters:
    fetch_and_save_csv(parameter)  # Fetch the full dataset for each parameter

    for year in years:
        process_csv(parameter, year)  # Process data for each year
        
        
        
        

