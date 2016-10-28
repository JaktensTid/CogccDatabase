import psycopg2

def get_connection():
    return psycopg2.connect(database="data",user="postgres",host="localhost",port=5432,password='')