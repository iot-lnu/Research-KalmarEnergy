import pandas as pd

# Define your parameters and years
parameters = [4, 6, 7, 19, 26, 27, 39]
years = [2020, 2021, 2022, 2023]

# Step 1: Combine CSVs for each parameter across all years
for parameter in parameters:
    # List to hold data for each year for the current parameter
    yearly_data = []
    
    for year in years:
        file_name = f'weather_P{parameter}_{year}_processed.csv'
        try:
            # Read the CSV file
            df = pd.read_csv(file_name, delimiter=';', encoding='utf-8')
            # Create 'DateTime' column and drop 'Date' and 'Time' columns
            df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
            df.drop(['Date', 'Time', 'Quality'], axis=1, inplace=True)  # Exclude 'Quality' column as well
            # Append to the yearly data list
            yearly_data.append(df)
        except FileNotFoundError:
            print(f"File {file_name} not found. Skipping.")
    
    # Combine yearly data for the current parameter
    parameter_data = pd.concat(yearly_data, ignore_index=True)
    # Save combined data for the current parameter to a CSV
    parameter_data.to_csv(f'combined_P{parameter}.csv', index=False, sep=';', encoding='utf-8')

# Step 2: Merge all parameters into one final CSV
# List to hold the combined data from each parameter
all_parameters_data = []

for parameter in parameters:
    try:
        # Read the combined CSV for the parameter
        df = pd.read_csv(f'combined_P{parameter}.csv', delimiter=';', encoding='utf-8')
        # Rename all columns except for 'DateTime' to include the parameter number
        df.rename(columns={col: f"{col}_P{parameter}" for col in df.columns if col != 'DateTime'}, inplace=True)
        # Append to the all parameters data list
        all_parameters_data.append(df)
    except FileNotFoundError:
        print(f"Combined CSV for parameter {parameter} not found. Skipping.")

# Combine all parameter data into one DataFrame
final_data = pd.concat(all_parameters_data, ignore_index=True)

# Pivot to get one unique 'DateTime' per row
final_data_pivot = final_data.pivot_table(index='DateTime', aggfunc='first').reset_index()

# Sort by 'DateTime'
final_data_pivot.sort_values(by='DateTime', inplace=True)

# Save the final pivoted data to a CSV
final_data_pivot.to_csv('final_combined_weather_data.csv', index=False, sep=';', encoding='utf-8')

print("Finished combining all parameter data.")
