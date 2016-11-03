import psycopg2


def get_connection():
    return psycopg2.connect(database="data",user="postgres",host="localhost",port=5432,password="7ACGApgx")

def write_checked_api_in_database(year):
    with get_connection() as connection:
        cursor = connection.cursor()
        with open('checkedapi', 'r') as fh:
            str = fh.read()
            query = """INSERT INTO checked_api_%s(api_county_code,
                                api_seq_num,
                                sidetrack_num,
                                year)""" % year
            splited = str.split(':')
            counter = 0
            for s in splited:
                if counter != 0:
                    county = s[0:3]
                    seq = s[3:8]
                    sidetrack = s[8:10]
                    year = s[10:]
                    if year != '':
                        query += ",('%s','%s','%s','%s')" % (county, seq,sidetrack,year)
                else:
                    county = s[0:3]
                    seq = s[3:8]
                    sidetrack = s[8:10]
                    year = s[10:]
                    query += " VALUES('%s','%s','%s','%s')" % (county, seq,sidetrack,year)
                counter += 1
            cursor.execute(query)
            connection.commit()
            print('Over')



