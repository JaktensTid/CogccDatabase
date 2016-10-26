import psycopg2

def get_connection():
    return psycopg2.connect(database="apdatabase",user="postgres",host="localhost",port=5432)