import Downloader
import logging
from datetime import date
from DatabaseConnection import get_connection
from Scraper import download_data_by_well_one_year

def check_and_create_table_if_not_exist():
    with get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("""SELECT EXISTS (
           SELECT 1
           FROM   information_schema.tables
           WHERE  table_schema = 'public'
           AND    table_name = 'monthly_well_production_%s'
        );""" % date.today().year)
        table_exists = True
        for row in cursor:
            table_exists = row[0]
            break
        if not table_exists:
            year = date.today().year
            create_table_query = """CREATE TABLE monthly_well_production_%s (CHECK (year = %s))
                                INHERITS (monthly_well_production);""" % (year,year)
            cursor.execute(create_table_query)
            connection.commit()
            query = """CREATE INDEX monthly_well_production_%s_index ON monthly_well_production_%s (year);
                    CREATE RULE monthly_well_production_%s AS ON INSERT TO monthly_well_production WHERE ( year = %s )
                      DO INSTEAD INSERT INTO monthly_well_production_%s VALUES (NEW.*);""" % (year,year,year,year,year)
            cursor.execute(query)
            connection.commit()
            logging.info(u"Creating table -- success")


def move_data_to_wells_apis():
    with get_connection() as connection:
        cursor = connection.cursor()
        query = """INSERT INTO wells_apis (api_county_code, api_seq_num, sidetrack_num, facility_name,facility_num, operator_name, operator_num, field_code, field_name, api_num, location)
                SELECT api_county_code, api_seq_num, sidetrack_num, facility_name,facility_num, name, operator_num, field_code, field_name, api_num, (qtrqtr || ' ' || sec || ' ' || twp || ' ' || range || ' ' || meridian) as location
                FROM well_completions
                GROUP BY api_county_code, api_seq_num, sidetrack_num, facility_name,facility_num, name, operator_num, field_code, field_name, api_num, qtrqtr, sec,  twp, range, meridian
                ON CONFLICT DO NOTHING;"""
        cursor.execute(query)
        connection.commit()
        logging.info(u"Move new data to wells apis -- success")


def clear_last_year_table():
    with get_connection() as connection:
        cursor = connection.cursor()
        query = "TRUNCATE monthly_well_production_%s CASCADE;" % date.today().year
        cursor.execute(query)
        connection.commit()
        logging.info(u"Clearing last year table -- success")

def download_and_insert_data_by_all_apis_by_year(year,fh):
    def dict_to_insert(d, cursor):
        table_names = ', '.join(list(d.keys()))
        values = ', '.join(['%s' for i in list(d.values())])
        qry = "INSERT INTO monthly_well_production (" + table_names + ") VALUES (%s);" % values
        cursor.execute(qry, list(d.values()))
    with get_connection() as connection1:
        with get_connection() as connection2:
            cursor1 = connection1.cursor()
            cursor2 = connection2.cursor()
            get_wells_query = """SELECT api_county_code, api_seq_num, sidetrack_num FROM wells_apis WHERE NOT EXISTS
                                (SELECT api_county_code, api_seq_num, sidetrack_num FROM checked_api_%s
                                WHERE wells_apis.api_county_code=checked_api_%s.api_county_code
                                AND wells_apis.api_seq_num=checked_api_%s.api_seq_num
                                AND wells_apis.sidetrack_num=checked_api_%s.sidetrack_num);"""
            cursor1.execute(get_wells_query % (year,year,year,year))
            for row in cursor1:
                data = download_data_by_well_one_year(row[0], row[1], row[2], year)
                for d in data:
                    d['year'] = year
                    d['api_county_code'] = row[0]
                    d['api_seq_num'] = row[1]
                    d['sidetrack_num'] = row[2]
                    dict_to_insert(d, cursor2)
                connection2.commit()
                fh.write(row[0]+row[1]+row[2]+str(year)+':')
                fh.flush()
    logging.info(u"Inserting -- success")


def update():
    logging.info(u"Cron job started")
    Downloader.download_and_insert_last_well_completion()
    move_data_to_wells_apis()
    check_and_create_table_if_not_exist()
    clear_last_year_table()
    with open('cron_apis_by_year','w+') as fh:
        download_and_insert_data_by_all_apis_by_year(date.today().year, fh)
    logging.info(u"Cron job over")


if __name__ == "__main__":
    update()
