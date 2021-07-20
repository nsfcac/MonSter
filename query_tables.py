def query_tables(conn):
    cursor = conn.cursor()
    query = "SELECT * FROM metrics;"
    cursor.execute(query)
    for row in cursor.fetchall():
        print(row)
    cursor.close()
