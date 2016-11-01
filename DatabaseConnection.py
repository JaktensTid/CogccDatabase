import psycopg2


def get_connection():
    return psycopg2.connect(database="data",user="postgres",host="138.68.81.207",port=5432,password="2v2ZbQPw")


