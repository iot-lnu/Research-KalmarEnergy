import pandas as pd

# Function to correct the 'Stensö' variations
def correct_stenso(area_name):
    return 'Stensö' if area_name.startswith('Stens') else area_name

# Combine all the data files, correct 2023 columns
def consolidate_data(filenames, fixed_encoding=True):
    encoding = 'ISO-8859-1' if fixed_encoding else None
  
    # Step 1: Read all CSV files into DataFrames with optional ISO-8859-1 encoding
    dfs = [pd.read_csv(filename, encoding=encoding) for filename in filenames]

    # Step 2: Rename columns only for the 2023 dataset
    dfs[-1].columns = dfs[-1].columns.str.replace('VALUE_', 'HOUR_')
    dfs[-1].rename(columns={'ID_FROM_DATE': 'DATE'}, inplace=True)
    
    # Step 3: Add a 'YEAR' column
    for df, filename in zip(dfs, filenames):
        df['YEAR'] = filename[-8:-4]  # extract year from filenames

    # Concatenate all DataFrames into one and reset the index
    combined_df = pd.concat(dfs, ignore_index=True)

    # Apply the function to the 'AREA' column
    combined_df['AREA'] = combined_df['AREA'].apply(correct_stenso)

    # Convert 'DATE' to datetime format
    combined_df['DATE'] = pd.to_datetime(combined_df['DATE'])

    return combined_df
