import pandas as pd
import mysql.connector
from mysql.connector import Error

# Function to preprocess CSV data
def preprocess_csv(csv_file_path):
    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file_path)

    # Split 'PLAYER' column into 'player_name' and 'position'
    df[['number', 'player_name', 'position']] = df['PLAYER'].str.split(';', expand=True)

    # Select relevant columns
    df = df[['Season Year', 'player_name', 'position', 'TEAM']]

    # Rename columns for consistency with database schema
    df.columns = ['season_year', 'player_name', 'position', 'team']

    return df

# Function to load DataFrame into MySQL
def load_df_to_mysql(df, mysql_config, table_name, create_table_sql):
    try:
        # Establish MySQL connection
        connection = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )

        if connection.is_connected():
            cursor = connection.cursor()
            print("Connected to MySQL database")

            # Create table if not exists
            cursor.execute(create_table_sql)
            print(f"Table {table_name} is ready.")

            # Insert data into table
            for i, row in df.iterrows():
                print(i, tuple(row))
                insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(row))})"
                cursor.execute(insert_query, tuple(row))
                
            
            # Commit the transaction
            connection.commit()
            print(f"Data loaded successfully into {table_name}.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def main():
    # MySQL configuration
    mysql_config = {
        'host': 'localhost',
        'user': 'user',
        'password': 'psd',
        'database': 'db'
    }

    # SQL statement to create team_players table
    create_team_players_table = """
    CREATE TABLE IF NOT EXISTS team_players (
        id INT NOT NULL,
        season_year INT,
        player_name VARCHAR(255) NOT NULL,
        position VARCHAR(255) NOT NULL,
        team VARCHAR(255) NOT NULL,
        FOREIGN KEY (id) REFERENCES players(id)
    )
    """

    create_players_table = """
    CREATE TABLE IF NOT EXISTS players (
        id INT NOT NULL UNIQUE,
        player_name VARCHAR(255) NOT NULL,
        PRIMARY KEY (id)
    )
    """

    # CSV file path
    csv_file_path = ['./mlb/hitting_player_by_scrapping_2003-23.csv',
                    './mlb/pitching_player_by_scrapping_2003-23.csv']

    # Preprocess CSV data
    df1 = preprocess_csv(csv_file_path[0])
    df2 = preprocess_csv(csv_file_path[1])
    df = pd.concat([df1, df2])
    df['id'] = df.groupby('player_name').ngroup() + 1

    df_players = df[['id', 'player_name']].drop_duplicates()

    # save 
    df.to_csv('team_players.csv', index=False)
    df_players.to_csv('players.csv', index=False)

    # Load preprocessed data into MySQL
    load_df_to_mysql(df_players, mysql_config, 'players', create_players_table)
    load_df_to_mysql(df, mysql_config, 'team_players', create_team_players_table)

if __name__ == '__main__':
    main()