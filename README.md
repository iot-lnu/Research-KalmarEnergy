# Research-KalmarEnergy

## Initial data cleaning and pre-processing 

All scripts for data cleaning and pre-processing can be found in utils/KEKEprocessing.py script 

To get the final version of the dataframe in your notebook use the foloowing code. 
This assumes that you have the initial data files in your data folder. 

```
import os
import pandas as pd
import utils.KEprocessing

# Define path to the CSV file
csv_path = 'data/combined_df_noNA.csv'

# Check if the file exists
if os.path.exists(csv_path):
    # If the file exists, read the DataFrame from the CSV
    combined_df = pd.read_csv(csv_path)
else:
    # If the file does not exist, call the function to prepare the DataFrame. 
    combined_df = utils.KEprocessing.prepare_final_df()
```    

## Electricity Price data 

### Year 2023 

https://www.elprisetjustnu.se/elpris-api
