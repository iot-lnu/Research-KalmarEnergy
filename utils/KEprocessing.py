import pandas as pd

# Function to correct the 'Stensö' variations
def correct_stenso(area_name):
    return 'Stensö' if area_name.startswith('Stens') else area_name

def consolidate_data(filenames, dataset_type, fixed_encoding=True):
    assert dataset_type in ['power', 'price'], "dataset_type must be either 'power' or 'price'"
    
    encoding = 'ISO-8859-1' if fixed_encoding else None
    dfs = [pd.read_csv(filename, encoding=encoding) for filename in filenames]
    
    if dataset_type == 'power':
        # Specific steps for the power dataset
        dfs[-1].columns = dfs[-1].columns.str.replace('VALUE_', 'HOUR_')
        dfs[-1].rename(columns={'ID_FROM_DATE': 'DATE'}, inplace=True)
        
        for df, filename in zip(dfs, filenames):
            df['YEAR'] = filename[-8:-4]  # Extract year from filenames
            
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df['AREA'] = combined_df['AREA'].apply(correct_stenso)
        combined_df['DATE'] = pd.to_datetime(combined_df['DATE'])
        
        return combined_df
    
    elif dataset_type == 'price':
        # Specific steps for the price dataset
        combined_prices_df = pd.concat(dfs, ignore_index=True)
        combined_prices_df.drop(columns=['index'], inplace=True, errors='ignore')
        combined_prices_df['timestamp_utc'] = pd.to_datetime(combined_prices_df['timestamp_utc']).dt.tz_localize(None)
        combined_prices_df.rename(columns={'timestamp_utc': 'DateTime', 'EUR_per_MWh': 'Price'}, inplace=True)
        combined_prices_df_sorted = combined_prices_df.sort_values('DateTime')
        combined_prices_df_sorted = combined_prices_df_sorted.reset_index(drop=True)
        
        return combined_prices_df_sorted


def replace_invalid_with_row_mean(df, residential_threshold, commercial_threshold, min_non_nan=3):
    # Operate only on the hour columns
    hour_columns = [f'HOUR_{i}' for i in range(24)]
    
    # Filter out rows that don't meet the threshold for non-NaN values
    df_filtered = df.dropna(thresh=min_non_nan, subset=hour_columns)
    
    # Determine thresholds based on customer type
    thresholds = df_filtered['ISPRIVATEPERSON'].map(lambda x: residential_threshold if x == 'Ja' else commercial_threshold)
    
    # Mask the values that are either above their respective thresholds or negative
    df_filtered[hour_columns] = df_filtered[hour_columns].mask((df_filtered[hour_columns] < 0) | (df_filtered[hour_columns].gt(thresholds, axis=0)))
    
    # Calculate the mean for each row excluding NaNs
    row_means = df_filtered[hour_columns].mean(axis=1)
    
    # Replace NaNs with the row's mean
    df_filtered[hour_columns] = df_filtered[hour_columns].apply(lambda x: x.fillna(row_means[x.name]), axis=1)
    
    return df_filtered


def prepare_final_df():
    filenames = ['data/lnu_2020.csv', 'data/lnu_2021.csv', 'data/lnu_2022.csv', 'data/lnu_2023.csv']
    combined_df = consolidate_data(filenames, 'power')

    # Define the hourly columns
    hourly_columns = [f'HOUR_{i}' for i in range(24)]

    # Calculate the sum of the hourly consumption for each record and create a new column 'One_Day_Power'
    combined_df['One_Day_Power'] = combined_df[hourly_columns].sum(axis=1)
    # Calculate the sum of the hourly consumption being NaN and save to a new column
    combined_df['One_Day_Power_NaN'] = combined_df[hourly_columns].isna().sum(axis=1)
    
    # Define your thresholds
    residential_threshold = 0.03  # 0.03 MWh for residential
    commercial_threshold = 3      # 3 MWh for commercial

    # Define the minimum number of non-NaN hourly values required to keep a row
    min_non_nan_values = 3

    combined_df_noNA = replace_invalid_with_row_mean(combined_df, residential_threshold, commercial_threshold, min_non_nan_values)
    combined_df_noNA.to_csv('data/combined_df_noNA.csv')
    
    return(combined_df_noNA)
    