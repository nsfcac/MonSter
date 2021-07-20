from create_tables import create_tables
from query_tables import query_tables
from dotenv import dotenv_values
from utils.check_config import check_config
from utils.parse_config import parse_config
import psycopg2
import os
import sys
sys.path.append(os.getcwd())


tsdb_config = dotenv_values(".env")

CONNECTION = f"dbname={tsdb_config['DBNAME']} user={tsdb_config['USER']} password={tsdb_config['PASSWORD']}"

path = os.getcwd()


def main():
    config_path = path + '/config.yml'
    config = parse_config(config_path)

    if not check_config(config):
        return

    try:
        idrac_config = config['idrac']

        conn = psycopg2.connect(CONNECTION)

        create_tables(conn)
        query_tables(conn)

    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()
