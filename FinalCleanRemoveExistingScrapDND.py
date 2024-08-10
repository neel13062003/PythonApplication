import csv
import re
import pandas as pd
import mysql.connector
from mysql.connector import Error
import pymysql

def clean_phone_number(phone_number):
    # Remove leading/trailing spaces and non-numeric characters
    # cleaned_number = re.sub(r"^\s*|\s*|[\D]", "", phone_number)
    cleaned_number = phone_number
    # If the cleaned number has less than 10 digits, or starts with 1-5, ignore it
    if len(cleaned_number) < 10:
        return None

    # if cleaned_number[0] in "012345":
    #     return None

    # If the cleaned number has more than 10 digits, get the last 10 digits
    # if len(cleaned_number) > 10:
    #     if cleaned_number[-10:][0] in "012345":
    #         return None
    #     return cleaned_number[-10:]

    # Check for any remaining non-numeric characters
    # if any(char not in "0123456789" for char in cleaned_number):
    #     return None

    if len(cleaned_number) > 10:
        cleaned_number = cleaned_number[-10:]
    # If the cleaned number has exactly 10 digits, return it
    return cleaned_number


def process_csv(input_file, phone_number_column):
    not_clean_numbers = 0
    cleaned_rows = []

    with open(input_file, "r", encoding="utf-8") as infile:
        reader = csv.reader(infile)

        # Read the header row
        header = next(reader)
        if not header:
            raise ValueError("Input CSV file is empty")

        # Check if the phone number column exists
        if phone_number_column not in header:
            raise ValueError(
                f"Column '{phone_number_column}' not found in the CSV file"
            )

        # Find the index of the phone number column
        phone_number_index = header.index(phone_number_column)

        cleaned_rows.append(header)

        # Process each row
        for row in reader:
            if phone_number_index < len(row):
                phone_number = row[phone_number_index]
                cleaned_number = clean_phone_number(phone_number)
                if cleaned_number is not None:
                    # Create a new row with the cleaned phone number
                    cleaned_row = row[:]
                    cleaned_row[phone_number_index] = cleaned_number
                    cleaned_rows.append(cleaned_row)
                else:
                    not_clean_numbers += 1
            else:
                not_clean_numbers += 1

    cleaned_df = pd.DataFrame(cleaned_rows[1:], columns=cleaned_rows[0])
    return cleaned_df, not_clean_numbers


def connect_to_database():
    try:
        # connection = mysql.connector.connect(
        connection = pymysql.connect(
            host="103.180.186.207",
            user="qrt",
            password="t7%><rC)MC)8rdsYCj<E",
            database="mainSource",
        )
        # if connection.is_connected():
        print("Loading... Please Wait Sometime")
        return connection
        # else:
        #     print("Connection failed")
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None


def connect_to_database_prod():
    try:
        # connection = mysql.connector.connect(
        connection = pymysql.connect(
            host="103.180.186.207",
            user="qrt",
            password="t7%><rC)MC)8rdsYCj<E",
            database="CRM",
        )
        # if connection.is_connected():
        #     print("Loading... Please Wait Sometime")
        #     return connection
        # else:
        #     print("Connection failed")
        print("Loading... Please Wait Sometime")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None


def fetch_mobile_numbers(query, connection):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        db_mobile_numbers = {str(row[0]) for row in result}
        return db_mobile_numbers
    except Error as e:
        print(f"Error fetching data from MySQL database: {e}")
        return set()


def main():
    try:
        # Get user input
        input_file = input("Enter the name of the CSV file: ")
        if not input_file.endswith(".csv"):
            input_file += ".csv"

        # Verify if the file is actually a CSV file
        if not input_file.lower().endswith(".csv"):
            raise ValueError("Invalid file format. Please provide a CSV file.")

        phone_number_column = input("Enter the name of the phone number column: ")

        # Clean the numbers in the CSV file
        cleaned_df, not_clean_numbers = process_csv(input_file, phone_number_column)

        # Connect to the database
        connection = connect_to_database()
        if connection is None:
            return

        # Fetch mobile numbers from the databases
        db_mobile_numbers_company = fetch_mobile_numbers(
            "SELECT STARTUPCONTACT FROM CRM.company_data", connection
        )
        db_mobile_numbers_dnd = fetch_mobile_numbers(
            "SELECT mobileNo FROM dnd", connection
        )
        db_mobile_numbers_scrap = fetch_mobile_numbers(
            "SELECT mobileNo FROM scrap", connection
        )

        # Check which mobile numbers from the cleaned CSV are not in any of the databases
        def is_number_in_databases(number):
            if number in db_mobile_numbers_dnd:
                return "dnd"
            if number in db_mobile_numbers_scrap:
                return "scrap"
            if number in db_mobile_numbers_company:
                return "company"
            return "not_found"

        # Filter the DataFrame and count removed numbers
        cleaned_df[phone_number_column] = cleaned_df[phone_number_column].astype(str)
        removed_dnd_numbers = 0
        removed_scrap_numbers = 0
        existing_numbers = 0

        df_cleaned = cleaned_df[
            cleaned_df[phone_number_column].apply(
                lambda x: is_number_in_databases(x) == "not_found"
            )
        ]
        for number in cleaned_df[phone_number_column]:
            status = is_number_in_databases(number)
            if status == "dnd":
                removed_dnd_numbers += 1
            elif status == "scrap":
                removed_scrap_numbers += 1
            elif status == "company":
                existing_numbers += 1

        # Calculate counts for messages
        total_numbers = cleaned_df.shape[0]

        # Print counts
        print(f"Total numbers processed: {total_numbers}")
        print(f"Count of not clean numbers: {not_clean_numbers}")
        print(f"Count of DND numbers removed: {removed_dnd_numbers}")
        print(f"Count of scrap numbers removed: {removed_scrap_numbers}")
        print(f"Count of existing numbers removed: {existing_numbers}")

        # Save the cleaned DataFrame to a new CSV file
        final_output_file = input_file + "_cleaned.csv"
        df_cleaned_removed_duplicate = df_cleaned.drop_duplicates()
        df_cleaned_removed_duplicate.to_csv(final_output_file, index=False)
        print(f"Cleaned CSV saved to '{final_output_file}'")

        # Close the database connection
        connection.close()

    except ValueError as e:
        print(f"Error: {e}")

    except Error as e:
        print(f"Database Error: {e}")

    except Exception as e:
        print(f"Unexpected Error: {e}")


if __name__ == "__main__":
    main()
