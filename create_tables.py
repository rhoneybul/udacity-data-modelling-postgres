'''
This module is required to create the tables for the sparkify etl pipeline
It drops the given tables if they exists, and subsequently creates the neccessary
tables.
'''

import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    ''' This creates the PSQL database required for the ETL pipeline '''
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    '''This drops the created tables

    Parameters:
    cur: database cursor
    conn: database connection

    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''This creates the relevant tables

    Parameters
    cur: database cursor
    conn: database connection
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ''' creates the database, drops tables if they
    exists, and creats the tables neccesary
    '''
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
