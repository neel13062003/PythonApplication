import pandas as pd
import mysql.connector
from mysql.connector import Error
import pymysql
from pymysql import MySQLError

# Step 1: Connect to the database
def connect_to_database():
    try:
        # connection = mysql.connector.connect(
        connection = pymysql.connect(
            host='103.180.186.207',
            user='qrt',
            password='t7%><rC)MC)8rdsYCj<E',
            database='PLATFORM'
        )
        return connection
    except Error as e:
        print(f'Error connecting to MySQL database: {e}')
        return None

# Step 2: Load the CSV data into a pandas DataFrame
def load_csv_to_dataframe(file_path):
    try:
        # Read CSV while specifying the date format for 'created_at'
        # df = pd.read_csv(file_path, parse_dates=['Date'], dayfirst=True)
        df = pd.read_csv(file_path, parse_dates=['createdAt'], dayfirst=True,infer_datetime_format=True)
        # df = pd.read_csv(file_path)
        # Convert 'created_at' column to the desired format
        # Check if the 'Date' column exists
        if 'createdAt' not in df.columns:
            raise ValueError("The CSV file does not contain a 'createdAt' column")

        # df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # print(f"Loaded {len(df)} records from {file_path}")
         # Convert 'Date' column to datetime format
        df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')  # Convert to datetime, errors='coerce' will handle invalid parsing
        if df['createdAt'].isnull().any():
            print("Warning: Some 'Date' values could not be parsed and have been set to NaT.")
        
        # Convert 'Date' column to the desired string format
        df['createdAt'] = df['createdAt'].dt.strftime('%Y-%m-%d %H:%M:%S')

        print(f"Loaded {len(df)} records from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

# Step 3: Insert the data from the DataFrame into the database table
def insert_dataframe_to_database(connection, df, table_name):
    try:
        cursor = connection.cursor()
        # Using executemany for bulk insert
        insert_query = f"""
            INSERT Ignore INTO PLATFORM.egniol_form (Name,Email,Phone,Subject,Date)
            VALUES (%s, %s,%s,%s,%s)
        """
        # print(df,"df")
        data = df.values.tolist()
        # print(insert_query,"insert_query")
        cursor.executemany(insert_query, data)
        connection.commit()
        print(f"Inserted {len(df)} records into  table")
    except Error as e:
        print(f"Error inserting data into table: {e}")

# Main function to load CSV and insert into database
def main(file_path):
    connection = connect_to_database()
    if connection:
        df = load_csv_to_dataframe(file_path)
        if df is not None:
            insert_dataframe_to_database(connection, df, 'egniol_form')
        connection.close()

# Replace 'your_file_path.csv' with the path to your CSV file
main('weblead.csv')

