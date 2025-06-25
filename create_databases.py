from pyhive import hive
from pymongo import MongoClient
import mysql.connector

def create_hive_database(database_name, host="localhost", port=10000):
    """Create a Hive database if it does not exist."""
    try:
        conn = hive.Connection(host=host, port=port, username='hadoop') 
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Hive database '{database_name}' created successfully.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to create Hive database: {e}")

def create_mongodb_database(database_name, host="localhost", port=27017):
    """Create a MongoDB database by inserting into a dummy collection."""
    try:
        client = MongoClient(host, port)
        db = client[database_name]
        print(f"MongoDB database '{database_name}' created successfully.")
        client.close()
    except Exception as e:
        print(f"Failed to create MongoDB database: {e}")

def create_mysql_database(database_name, host="localhost", user="root", password="Manish@123"):
    """Create a MySQL database if it does not exist."""
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"MySQL database '{database_name}' created successfully.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to create MySQL database: {e}")

create_hive_database("db")
create_mongodb_database("sample")
create_mysql_database("sample", password="Manish@123") 