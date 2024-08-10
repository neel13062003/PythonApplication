import pymysql

# Database connection details
db_config = {
    'host': '103.180.186.207',
    'user': 'qrt',
    'password': 't7%><rC)MC)8rdsYCj<E',
    'database': 'dialpad1',
}

def batch_update_lead_ids(batch_size=1000):
    # Connect to the database
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            while True:
                # Query to fetch IDs to be updated
                fetch_query = """
                SELECT l.id
                FROM dialpad1.testing_leads l
                WHERE l.assignedTo = 1 AND l.assignedTo != 15
                LIMIT %s;
                """
                cursor.execute(fetch_query, (batch_size,))
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                ids = [row[0] for row in rows]
                print(f"Fetched rows: {ids}")

                # Prepare update query
                update_query = """
                UPDATE dialpad1.testing_leads l
                JOIN dialpad1.leadAssignedDetails ld ON l.id = ld.leadId
                SET l.assignedTo = ld.userId
                WHERE l.id IN %s;
                """
                
                # Update records in bulk
                try:
                    # Use tuple for SQL IN clause
                    id_tuples = tuple(ids)
                    cursor.execute(update_query, (id_tuples,))
                except pymysql.MySQLError as e:
                    print(f"Error updating records: {e}")
                    connection.rollback()
                    continue
                
                # Commit the transaction
                connection.commit()
                print(f"Batch update completed with {len(ids)} records.")
    
    except pymysql.MySQLError as e:
        print(f"Error occurred: {e}")
        connection.rollback()
    
    finally:
        # Close the connection
        connection.close()

if __name__ == "__main__":
    batch_update_lead_ids(batch_size=1000)
