import pandas as pd
import mysql.connector
import pymysql

# Function to get unmatched numbers


def get_unmatched_numbers(df, column_name):
    return df[df[column_name].isna()]['MobileNumber'].tolist()

# Function to trim and clean mobile numbers


def clean_mobile_number(mobile_number):
    return str(mobile_number).strip()


# Database connection (replace with your database credentials)
db_config = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    # 'database': '',
}

# Connect to the database using MySQL Connector
# connection = mysql.connector.connect(**db_config)
connection = pymysql.connect(**db_config)
# cursor = connection.cursor(dictionary=True)
cursor = connection.cursor(pymysql.cursors.DictCursor)

# Load the CSV file
csv_file_path = 'inputmatch.csv'
df = pd.read_csv(csv_file_path)

# Trim and clean mobile numbers
df['MobileNumber'] = df['MobileNumber'].apply(clean_mobile_number)

# Initialize columns for CompanyName and Email
df['CompanyName'] = None
df['Email'] = None

# Define table priorities and columns to select
tables = [
    ('pt.mca_35', 'mobileNumber', 'nameOfTheCompany', 'emailAddress'),
    ('gt.laliyo_qrtv2', 'CONTACT', 'COMPANY_NAME', 'EMAIL'),
    ('udyam_scrap.udyam_data', 'CONTACT', 'COMPANY_NAME', 'EMAIL'),
    # ('udy.hmng_udy_che', 'CONTACT', 'COMPANY_NAME', 'EMAIL')
]

# Perform lookups in priority order
for table, contact_column, name_column, email_column in tables:
    mobile_numbers = get_unmatched_numbers(df, 'CompanyName')

    if not mobile_numbers:
        break

    query = f"""
        SELECT {contact_column} AS MobileNumber, {name_column} AS CompanyName, {email_column} AS Email
        FROM {table}
        WHERE {contact_column} IN ({','.join(['%s'] * len(mobile_numbers))})
    """
    cursor.execute(query, mobile_numbers)
    results = cursor.fetchall()
    print(table, " | ", len(results), " | Results Matched")
    for result in results:
        df.loc[df['MobileNumber'] == result['MobileNumber'], [
            'CompanyName', 'Email']] = [result['CompanyName'], result['Email']]

# Save the results to a new CSV file
results_file_path = 'inputoutput.csv'
df.to_csv(results_file_path, index=False)

# Close the database connection
cursor.close()
connection.close()
