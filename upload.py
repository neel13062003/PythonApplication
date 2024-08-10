import pandas as pd
import mysql.connector
from mysql.connector import Error

# Step 1: Connect to the database
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='ls-b22a231d8e508b8c418ba2bb457da5988f6a952d.c2g9uvquz5lp.ap-south-1.rds.amazonaws.com',
            user='DEV_FRANCE',
            password='Egniol#271222',
            database='airtel'
        )
        if connection.is_connected():
            print('Connected to MySQL database')
            return connection
        else:
            print("Connection failed")
    except Error as e:
        print(f'Error connecting to MySQL database: {e}')
        return None

# Step 2: Load the CSV data into a pandas DataFrame
def load_csv_to_dataframe(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded {len(df)} records from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

# Step 3: Insert the data from the DataFrame into the database table in batches
def insert_dataframe_to_database(connection, df, table_name, batch_size=1000):
    try:
        cursor = connection.cursor()
        
        # Disable foreign key checks and indexes temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute(f"ALTER TABLE {table_name} DISABLE KEYS;")
        
        # Replace NaN with None
        df = df.where(pd.notnull(df), None)
        
        # Define the insert query
        insert_query = f"""
            INSERT IGNORE INTO {table_name} (`CINNumber`, `Company Name`, `COMPANY_TYPE`, `DATE_INCorporation`, `EMAILID`, `R_O_C`, `ADDRESS`, `City`, `State`, `Director_DIN`, `PANCard`, `Director_Name`, `COUNTRY`, `DIRECTOR_PINCODE`, `Mobile`, `DIRECTOR_EMAIL`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Insert data in batches
        for start in range(0, len(df), batch_size):
            end = start + batch_size
            batch_data = df.iloc[start:end].values.tolist()
            
            # Print the data being inserted in the current batch
            print(f"Inserting batch {start//batch_size + 1} with {len(batch_data)} records")
            for record in batch_data:
                print(record)
            
            cursor.executemany(insert_query, batch_data)
            connection.commit()
            print(f"Inserted batch {start//batch_size + 1}")
        
        # Re-enable foreign key checks and indexes
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        cursor.execute(f"ALTER TABLE {table_name} ENABLE KEYS;")
        
        print(f"Inserted total {len(df)} records into {table_name} table")
    except Error as e:
        print(f"Error inserting data into table: {e}")

# Main function to load CSV and insert into database
def main(file_path):
    connection = connect_to_database()
    if connection:
        df = load_csv_to_dataframe(file_path)
        if df is not None:
            insert_dataframe_to_database(connection, df, 'airtel.mca_pt')
        connection.close()

# Replace 'today.csv' with the path to your CSV file
main('today.csv')
