import pymysql
import pandas as pd

# Database connection details
host = 'localhost'
username = 'root'
password = 'root'

# Database connection function
def get_connection(db_name):
    return pymysql.connect(
        host=host,
        user=username,
        password=password,
        db=db_name,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# Queries
queries = {
    'gt_laliyo_qrtv2': """
        SELECT * FROM laliyo_qrtv2
        WHERE COMPANY_NAME LIKE '%Academy%'
           OR COMPANY_NAME LIKE '%Campus%'
           OR COMPANY_NAME LIKE '%Classes%'
           OR COMPANY_NAME LIKE '%Coaching%'
           OR COMPANY_NAME LIKE '%College%'
           OR COMPANY_NAME LIKE '%Curriculum%'
           OR COMPANY_NAME LIKE '%EdTech%'
           OR COMPANY_NAME LIKE '%Education%'
           OR COMPANY_NAME LIKE '%Eduservices%'
           OR COMPANY_NAME LIKE '%Edventures%'
           OR COMPANY_NAME LIKE '%eLearning%'
           OR COMPANY_NAME LIKE '%Institute%'
           OR COMPANY_NAME LIKE '%Learning%'
           OR COMPANY_NAME LIKE '%Learning Solutions%'
           OR COMPANY_NAME LIKE '%Pathways%'
           OR COMPANY_NAME LIKE '%Scholar Solutions%'
           OR COMPANY_NAME LIKE '%School%'
           OR COMPANY_NAME LIKE '%Schoolhouse%'
           OR COMPANY_NAME LIKE '%Studies%'
           OR COMPANY_NAME LIKE '%Study%'
           OR COMPANY_NAME LIKE '%Study Experts%'
           OR COMPANY_NAME LIKE '%Study Solutions%'
           OR COMPANY_NAME LIKE '%Teach%'
           OR COMPANY_NAME LIKE '%University%'
           OR COMPANY_NAME LIKE '%Scholarship%'
           OR COMPANY_NAME LIKE '%Tutors%'
           OR COMPANY_NAME LIKE '%Educators%';
    """,
    'pt_mca_35': """
        SELECT * FROM mca_35 
        WHERE nameOfTheCompany LIKE '%Academy%'
           OR nameOfTheCompany LIKE '%Campus%'
           OR nameOfTheCompany LIKE '%Classes%'
           OR nameOfTheCompany LIKE '%Coaching%'
           OR nameOfTheCompany LIKE '%College%'
           OR nameOfTheCompany LIKE '%Curriculum%'
           OR nameOfTheCompany LIKE '%EdTech%'
           OR nameOfTheCompany LIKE '%Education%'
           OR nameOfTheCompany LIKE '%Eduservices%'
           OR nameOfTheCompany LIKE '%Edventures%'
           OR nameOfTheCompany LIKE '%eLearning%'
           OR nameOfTheCompany LIKE '%Institute%'
           OR nameOfTheCompany LIKE '%Learning%'
           OR nameOfTheCompany LIKE '%Learning Solutions%'
           OR nameOfTheCompany LIKE '%Pathways%'
           OR nameOfTheCompany LIKE '%Scholar Solutions%'
           OR nameOfTheCompany LIKE '%School%'
           OR nameOfTheCompany LIKE '%Schoolhouse%'
           OR nameOfTheCompany LIKE '%Studies%'
           OR nameOfTheCompany LIKE '%Study%'
           OR nameOfTheCompany LIKE '%Study Experts%'
           OR nameOfTheCompany LIKE '%Study Solutions%'
           OR nameOfTheCompany LIKE '%Teach%'
           OR nameOfTheCompany LIKE '%University%'
           OR nameOfTheCompany LIKE '%Scholarship%'
           OR nameOfTheCompany LIKE '%Tutors%'
           OR nameOfTheCompany LIKE '%Educators%';
    """,
    'udyam_scrape': """
        SELECT * FROM udyam_data 
        WHERE COMPANY_NAME LIKE '%Academy%'
           OR COMPANY_NAME LIKE '%Campus%'
           OR COMPANY_NAME LIKE '%Classes%'
           OR COMPANY_NAME LIKE '%Coaching%'
           OR COMPANY_NAME LIKE '%College%'
           OR COMPANY_NAME LIKE '%Curriculum%'
           OR COMPANY_NAME LIKE '%EdTech%'
           OR COMPANY_NAME LIKE '%Education%'
           OR COMPANY_NAME LIKE '%Eduservices%'
           OR COMPANY_NAME LIKE '%Edventures%'
           OR COMPANY_NAME LIKE '%eLearning%'
           OR COMPANY_NAME LIKE '%Institute%'
           OR COMPANY_NAME LIKE '%Learning%'
           OR COMPANY_NAME LIKE '%Learning Solutions%'
           OR COMPANY_NAME LIKE '%Pathways%'
           OR COMPANY_NAME LIKE '%Scholar Solutions%'
           OR COMPANY_NAME LIKE '%School%'
           OR COMPANY_NAME LIKE '%Schoolhouse%'
           OR COMPANY_NAME LIKE '%Studies%'
           OR COMPANY_NAME LIKE '%Study%'
           OR COMPANY_NAME LIKE '%Study Experts%'
           OR COMPANY_NAME LIKE '%Study Solutions%'
           OR COMPANY_NAME LIKE '%Teach%'
           OR COMPANY_NAME LIKE '%University%'
           OR COMPANY_NAME LIKE '%Scholarship%'
           OR COMPANY_NAME LIKE '%Tutors%'
           OR COMPANY_NAME LIKE '%Educators%';
    """,
    'egniol_mca_new_neel_final3': """
        SELECT * FROM mca_new_neel_final3
        WHERE companyname LIKE '%Academy%'
           OR companyname LIKE '%Campus%'
           OR companyname LIKE '%Classes%'
           OR companyname LIKE '%Coaching%'
           OR companyname LIKE '%College%'
           OR companyname LIKE '%Curriculum%'
           OR companyname LIKE '%EdTech%'
           OR companyname LIKE '%Education%'
           OR companyname LIKE '%Eduservices%'
           OR companyname LIKE '%Edventures%'
           OR companyname LIKE '%eLearning%'
           OR companyname LIKE '%Institute%'
           OR companyname LIKE '%Learning%'
           OR companyname LIKE '%Learning Solutions%'
           OR companyname LIKE '%Pathways%'
           OR companyname LIKE '%Scholar Solutions%'
           OR companyname LIKE '%School%'
           OR companyname LIKE '%Schoolhouse%'
           OR companyname LIKE '%Studies%'
           OR companyname LIKE '%Study%'
           OR companyname LIKE '%Study Experts%'
           OR companyname LIKE '%Study Solutions%'
           OR companyname LIKE '%Teach%'
           OR companyname LIKE '%University%'
           OR companyname LIKE '%Scholarship%'
           OR companyname LIKE '%Tutors%'
           OR companyname LIKE '%Educators%';
    """
}

# Fetch data from database and save to CSV
def fetch_data_and_save_to_csv(db_name, query, csv_filename):
    connection = get_connection(db_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            df.to_csv(csv_filename, index=False)
    finally:
        connection.close()

# Fetch data and save to respective CSV files
fetch_data_and_save_to_csv('gt', queries['gt_laliyo_qrtv2'], 'gt_laliyo_qrtv2.csv')
fetch_data_and_save_to_csv('pt', queries['pt_mca_35'], 'pt_mca_35.csv')
fetch_data_and_save_to_csv('udyam_scrape', queries['udyam_scrape'], 'udyam_scrape.csv')
fetch_data_and_save_to_csv('egniol', queries['egniol_mca_new_neel_final3'], 'egniol_mca_new_neel_final3.csv')

print("Data fetched and saved to CSV files successfully.")
