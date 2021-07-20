import psycopg2
from dotenv import dotenv_values
from create_tables import create_tables

config = dotenv_values(".env")

CONNECTION = f"dbname={config['DBNAME']} user={config['USER']} password={config['PASSWORD']}"


def query_tables(conn):
    cursor = conn.cursor()
    query = "SELECT * FROM metrics;"
    cursor.execute(query)
    for row in cursor.fetchall():
        print(row)
    cursor.close()


def main():
    with psycopg2.connect(CONNECTION) as conn:
        create_tables(conn)
    # query_tables(conn)


if __name__ == "__main__":
    main()
