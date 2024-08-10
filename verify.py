import streamlit as st
import pandas as pd
import mysql.connector
import dns.resolver
import smtplib
from datetime import datetime
from io import StringIO

DB_HOST = "192.168.0.224"
DB_USER = "py"
DB_PASSWORD = "Python@2023"
DB_NAME = "email"

# Cache for DNS lookups
dns_cache = {}

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def is_email_valid(email):
    if '@' not in email:
        return False

    domain = email.split('@')[1].lower()
    if not domain:
        return False

    if domain in dns_cache:
        records = dns_cache[domain]
    else:
        try:
            records = dns.resolver.resolve(domain, 'MX')
            dns_cache[domain] = records
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.DNSException):
            return False

    if not records:
        return False

    mx_host = str(records[0].exchange)
    
    try:
        server = smtplib.SMTP(mx_host, 25)
        server.set_debuglevel(0)
        server.helo()
        server.mail('verify@yashvik.co.in')
        code, _ = server.rcpt(email)
        server.quit()
        return code == 250
    except Exception:
        return False

def process_emails(df):
    df['Status'] = df['Email'].apply(lambda email: 'Verified' if is_email_valid(email) else 'Not Verified')
    return df

def main():
    st.title("Email Verification App")

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(uploaded_file)

        if 'Email' not in df.columns:
            st.error("The CSV file must contain an 'Email' column.")
            return

        # Process emails
        result_df = process_emails(df)

        # Display the resulting DataFrame
        st.write("Processed Data:")
        st.dataframe(result_df)

        # Optionally, save to CSV
        csv = result_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name='processed_emails.csv',
            mime='text/csv'
        )

if __name__ == "__main__":
    main()
