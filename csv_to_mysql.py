import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('your_database.db')
terms = ("player_hitting", "player_pitching", "team_hitting", "team_pitching")

# Read the CSV files and insert the data into the database
csv_files = [f'./result/{term}_combined.csv' for term in terms]
for i in range(len(csv_files)):
    df = pd.read_csv(csv_files[i])
    df.to_sql(terms[i], conn, if_exists='replace', index=False)

# Close the connection
conn.close()
