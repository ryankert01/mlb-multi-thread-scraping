import pandas as pd
import os

# Base directory containing the year folders
base_path = './mlb/'

# Define the types of files expected
file_types = ['player_hitting', 'player_pitching', 'team_hitting', 'team_pitching']

# Initialize dictionaries to hold dataframes for each type
dataframes = {file_type: [] for file_type in file_types}

# Iterate over each year in the directory
for year in os.listdir(base_path):
    year_path = os.path.join(base_path, year)
    if os.path.isdir(year_path):  # Ensure it's a directory
        # Iterate over each file type
        for file_type in file_types:
            # Build the path to the CSV file
            file_path = os.path.join(year_path, f"{file_type}.csv")
            if os.path.exists(file_path):  # Check if the file exists
                # Read the CSV file
                df = pd.read_csv(file_path)
                # Add a 'year' column
                df['year'] = year
                # Append the dataframe to the correct list in the dictionary
                dataframes[file_type].append(df)

# Concatenate all dataframes for each type and save to a CSV file
for file_type, dfs in dataframes.items():
    if dfs:  # Check if there are any dataframes to concatenate
        final_df = pd.concat(dfs, ignore_index=True)
        # Save the combined dataframe to a CSV file
        final_df.to_csv(f'{file_type.replace(" ", "_")}_combined.csv', index=False)

        
