import mysql.connector
from mysql.connector import Error
import pandas as pd
from json import load
# pip install mysql-connector-python

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

def get_table_column(connection,table,column):
     cursor = connection.cursor()
     try:
         cursor.execute(f"SELECT {column} FROM {table}")
         results=cursor.fetchall()
         cursor.close()
         return results
     except Error as err:
         print(f"Error: '{err}'")


create_games_table = """
CREATE TABLE games (
  game_id INT PRIMARY KEY,
  away_team VARCHAR(40) NOT NULL,
  home_team VARCHAR(40) NOT NULL,
  away_ops FLOAT NOT NULL,
  home_ops FLOAT NOT NULL,
  away_fp FLOAT NOT NULL,
  home_fp FLOAT NOT NULL,
  away_era FLOAT NOT NULL,
  home_era FLOAT NOT NULL,
  homeoraway int NOT NULL,
  date DATE NOT NULL
);
 """
def insert_game(table,connection,*args):
    entries=','.join(f"'{arg}'" if isinstance(arg,str) else str(arg) for arg in args)
    query=f"""INSERT
    INTO
    {table}
    VALUES ({entries});"""
    execute_query(connection,query)

if __name__=='__main__':
    CREDENTIALS_JSON = r'sql.json'
    with open(CREDENTIALS_JSON, 'rb') as jfile:
        logon_dict: dict = load(jfile)
    sql_connect=create_db_connection(*logon_dict.values(),'mlb')
    create_database(sql_connect,create_games_table)
