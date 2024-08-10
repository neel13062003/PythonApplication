import pymysql

# Database connection details
db_config = {
    'host': '103.180.186.207',
    'user': 'qrt',
    'password': 't7%><rC)MC)8rdsYCj<E',
    'database': 'egnioldialer',
}

def batch_update_lead_ids(batch_size=1000):
    # Connect to the database
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            # Query to fetch IDs to be updated
            fetch_query = """
            SELECT d.call_id, l.id as new_lead_id
            FROM dialpad1.dialpadCallDetails d
            JOIN dialpad1.testing_leads l
            ON RIGHT(l.contact, 7) = RIGHT(d.clientNumber, 7)
            WHERE d.leadId IS NULL
            AND d.total_duration != 0
            LIMIT %s;
            """
            
            # Update query
            update_query = """
            UPDATE dialpad1.dialpadCallDetails d
            JOIN dialpad1.testing_leads l
            ON RIGHT(l.contact, 7) = RIGHT(d.clientNumber, 7)
            SET d.leadId = l.id
            WHERE d.call_id = %s;
            """
            
            while True:
                # Fetch a batch of records to update
                cursor.execute(fetch_query, (batch_size,))
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                # Update records in batch
                for row in rows:
                    try:
                        # Print query and parameters for debugging
                        print(f"Executing query: {update_query} with parameters: {row[1]}, {row[0]}")
                        cursor.execute(update_query, (row[0],))  # Note: Order of parameters is crucial
                    except pymysql.MySQLError as e:
                        print(f"Error updating record with call_id {row[0]}: {e}")
                        continue
                
                # Commit the transaction
                connection.commit()
                print(f"Batch update completed with {len(rows)} records.")
    
    except pymysql.MySQLError as e:
        print(f"Error occurred: {e}")
        connection.rollback()
    
    finally:
        # Close the connection
        connection.close()

if __name__ == "__main__":
    batch_update_lead_ids(batch_size=1000)
