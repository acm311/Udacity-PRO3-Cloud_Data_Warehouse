import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import datetime

""" take info from S3 buckets and load the data into 
    Redshift (staging tables)
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

""" execute INSERT queries from staging tables to 
the dimension and fact tables
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)        
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    now = datetime.datetime.now()
    print('Starting ETL process at: ' + now.strftime("%Y-%m-%d %H:%M:%S"))
    print('Connecting...')    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Loading data from S3 buckets to staging tables...')    
    load_staging_tables(cur, conn)
    print('Loading data from staging tables to data warehouse...')
    insert_tables(cur, conn)

    conn.close()
    print('Connection closed.')
    now = datetime.datetime.now()
    print('Finishing ETL process at: ' + now.strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    main()