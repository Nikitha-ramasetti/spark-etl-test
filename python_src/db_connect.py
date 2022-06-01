import psycopg2
from sqlalchemy import *


def getConnection(username, password, host, port):
    conn_str = f"postgresql://{username}:{password}@{host}:{port}/postgres"
    engine = create_engine(conn_str)
    conn = engine.connect()
    return conn, conn_str


def execute_sql_ddl_cmds(conn):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Users (
            user_id INTEGER PRIMARY KEY,
            createdAt TIMESTAMP,
            updatedAt TIMESTAMP,
            city TEXT,
            country TEXT,
            profile_gender CHAR(8),
            profile_is_smoking Boolean,
            profile_profession CHAR(30),
            profile_income REAL,
            birth_year Integer,
            domain CHAR(30)
        )
        """,

        """ 
        CREATE TABLE IF NOT EXISTS Subscriptions (
            subscription_id Integer,
            user_id Integer,
            createdAt TIMESTAMP,
            startDate TIMESTAMP,
            endDate TIMESTAMP,
            status CHAR(20),
            amount REAL
        )
        """,

        """
        CREATE TABLE IF NOT EXISTS Messages (
            message_id INTEGER PRIMARY KEY,
            createdAt TIMESTAMP,
            receiverId Integer,
            senderId Integer
        )
        """)

    try:

        print("Connecting to the PostgreSQL database")

        # preparing query to create database
        # cur.execute("""CREATE database spark1;""")
        # "print("Database created successfully")

        # create table one by one
        for command in commands:
            conn.execute(command)
        print("Database schema created")

        # close communication with the PostgreSQL database server
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("PostgreSQL connection is closed")