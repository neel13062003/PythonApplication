import mysql.connector
import dns.resolver
import smtplib
from multiprocessing import Pool
from datetime import datetime

DB_HOST = "192.168.0.224"
DB_USER = "py"
DB_PASSWORD = "Python@2023"
DB_NAME = "email"
BATCH_SIZE = 100000

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

def update_email_status(email, status):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get the current date and time
    now = datetime.now()

    # Format the current date and time for MySQL
    created_at = now.strftime('%Y-%m-%d %H:%M:%S')
    # print(created_at)
    cursor.execute(
        "INSERT IGNORE INTO email_verification (email, isVerified, email_from, createdAt) VALUES (%s, %s, 'udy_system', %s);",
        (email, status, created_at)
    )
    conn.commit()
    cursor.close()
    conn.close()

def process_email(email):
    if is_email_valid(email):
        print(f"Verified Mail {email} !!!!!")
        update_email_status(email, 1)
    else:
        print(f"Not Verified Mail {email} !!!!!")
        update_email_status(email, 0)

def read_processed_emails_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # Fetch emails that are in raw_emails but not in email_verification
    cursor.execute("""
        SELECT lower(email) as email FROM email_verification
    """)
    emails = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return emails

def read_all_emails_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # Fetch emails that are in raw_emails but not in email_verification
    cursor.execute("""
        SELECT lower(EMAIL) as email FROM projectdb.udyam_data
    """)
    emails = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return emails

def main():
    all_emails = read_all_emails_from_db()
    processed_emails = read_processed_emails_from_db()
    emails_to_process = list(set(all_emails) - set(processed_emails))  # Convert to list
    print(f"Total emails to process: {len(emails_to_process)}")
    batches = [emails_to_process[i:i + BATCH_SIZE] for i in range(0, len(emails_to_process), BATCH_SIZE)]
    print(f"Total batches: {len(batches)}")
    with Pool(processes=100) as pool:
        for i, batch in enumerate(batches):  # Process only the first three batches
            print(f"Processing batch {i+1}/{len(batches)}...")
            pool.map(process_email, batch)
            print(f"Batch {i+1} processed.")

if __name__ == "__main__":
    main()