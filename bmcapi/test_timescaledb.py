import psycopg2
from pgcopy import CopyManager

#user monster
#pwd redraider
#host localhost
#port 5432
#db test_tsdb

CONNECTION="postgres://monster:redraider@localhost:5432/test_tsdb"

# CONNECTION = "dbname =test_tsdb user=monster password=redraider host=localhost port=5432 sslmode=require"


def main():
    with psycopg2.connect(CONNECTION) as conn:
        fast_insert(conn)

def insert(conn):
    cur = conn.cursor()

    ### This should be done by superuser
    # enable_timscale_extension = "CREATE EXTENSION IF NOT EXISTS timescaledb;"
    # cur.execute(enable_timscale_extension)   

    ### Create tables and enable hypertable
    # query_create_sensors_table = "CREATE TABLE sensors (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50));"
    
    # query_create_sensordata_table = """CREATE TABLE sensor_data (
    #                                    time TIMESTAMPTZ NOT NULL,
    #                                    sensor_id INTEGER,
    #                                    temperature DOUBLE PRECISION,
    #                                    cpu DOUBLE PRECISION,
    #                                    FOREIGN KEY (sensor_id) REFERENCES sensors (id)
    #                                    );"""

    # query_create_sensordata_hypertable = "SELECT create_hypertable('sensor_data', 'time');"
    
    # cur.execute(query_create_sensors_table)
    # cur.execute(query_create_sensordata_table) 
    # cur.execute(query_create_sensordata_hypertable)
    
    ### Test inserting rows
    sensors = [('a','floor'),('a', 'ceiling'), ('b','floor'), ('b', 'ceiling')]

    SQL = "INSERT INTO sensors (type, location) VALUES (%s, %s);"
    for sensor in sensors:
        try:
            data = (sensor[0], sensor[1])
            cur.execute(SQL, data)
        except (Exception, psycopg2.Error) as error:
            print(error.pgerror)

    conn.commit()
    cur.close()
    print("Success")



def fast_insert(conn):
   cur = conn.cursor()

   #for sensors with ids 1-4
   for id in range(1,4,1):
       data = (id, )
       #create random data
       simulate_query = """SELECT  generate_series(now() - interval '24 hour', now(), interval '5 minute') AS time,
       %s as sensor_id,
       random()*100 AS temperature,
       random() AS cpu
       """
       cur.execute(simulate_query, data)
       values = cur.fetchall()

       print(values)

       #define columns names of the table you're inserting into
       cols = ('time', 'sensor_id', 'temperature', 'cpu')

       #create copy manager with the target table and insert!
       mgr = CopyManager(conn, 'sensor_data', cols)
       mgr.copy(values)

   #commit after all sensor data is inserted
   #could also commit after each sensor insert is done
   conn.commit()

   #check if it worked
   cur.execute("SELECT * FROM sensor_data LIMIT 5;")
#    print(cur.fetchall())
   cur.close()


if __name__ == '__main__':
    main()