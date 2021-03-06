import psycopg2
import os

def get_connection():
    return psycopg2.connect(database="data",user="postgres",host="localhost",port=5432,password='')

def write_checked_api_in_database(year):
    with get_connection() as connection:
        cursor = connection.cursor()
        counter = 0
        with open('checked_api_%s' % year, 'r') as fh:
            str = fh.read()
            query = """INSERT INTO checked_api_%s(api_county_code,
                                api_seq_num,
                                sidetrack_num)""" % year
            splited = str.split(':')
            for s in splited:
                if counter != 0:
                    county = s[0:3]
                    seq = s[3:8]
                    sidetrack = s[8:10]
                    if county != '':
                        query += ",('%s','%s','%s')" % (county, seq,sidetrack)
                else:
                    county = s[0:3]
                    seq = s[3:8]
                    sidetrack = s[8:10]
                    query += " VALUES('%s','%s','%s')" % (county, seq,sidetrack)
                counter += 1
                if counter % 10000 == 0:
                    cursor.execute(query)
                    connection.commit()
                    query = """INSERT INTO checked_api_%s(api_county_code,
                                api_seq_num,
                                sidetrack_num)""" % year
                    counter = 0
            query += ' ON CONFLICT DO NOTHING;'
            cursor.execute(query)
            connection.commit()
            print('Over')

if __name__ == '__main__':
    write_checked_api_in_database('2013')



