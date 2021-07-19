import psycopg2

CONNECTION = "dbname=redraider user=monster password=redraider"


def query_tables(conn):
    cursor = conn.cursor()
    query = "SELECT * FROM metrics_definition;"
    cursor.execute(query)
    for row in cursor.fetchall():
        print(row)
    cursor.close()


def main():
    with psycopg2.connect(CONNECTION) as conn:
        query_tables(conn)


if __name__ == "__main__":
    main()
