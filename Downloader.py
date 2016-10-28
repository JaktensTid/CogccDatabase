import os
import csv
from os import listdir
from os.path import isfile, join
import logging
from urllib.request import urlopen
from zipfile import ZipFile
from CreateTables import column_names, tables_names
from DatabaseConnection import get_connection
import subprocess
from datetime import date
import re

insert_into_well_completions_query = "INSERT INTO {0}({1}) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(tables_names[0], ','.join(column_names[0]))
insert_into_production_reports_query = "INSERT INTO {0}({1}) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(tables_names[1], ','.join(column_names[1]))
tempfile_path = 'tempfile.zip'
logging.basicConfig(level = logging.DEBUG)
well_completions_link = "http://cogcc.state.co.us/documents/data/downloads/production/co%20{0}%20Annual%20Production%20Summary-xp.zip"
production_summaries_link = "http://cogcc.state.co.us/documents/data/downloads/production/{0}_prod_reports.csv"
production_summaries_current_year_link = "http://cogcc.state.co.us/documents/data/downloads/production/monthly_prod.csv"

def download_and_insert_all_well_completions():
    links = [(year,well_completions_link.format(year)) for year in range(1999, 2017)]
    logging.info(u'Starting processing of well complections')

    for year, link in links:
        mdb = download_well_completion(link)
        csv = mdb_to_csv(mdb)
        insert_to_database(csv, insert_into_well_completions_query, year)

    logging.info(u'Ended processing of well complections')

def download_and_insert_all_production_reports():
    links = [(year,production_summaries_link.format(year)) for year in range(1999, 2016)] + [(date.today().year, production_summaries_current_year_link)]
    logging.info(u'Starting processing of production reports')

    for year, link in links:
        csv = download_production_report(link)
        insert_to_database(csv, insert_into_production_reports_query, year)

    logging.info(u'Ended processing of production reports')

def download_well_completion(link):
    '''Returns path to .mdb file'''
    logging.info(u'Downloading: ' + link)
    zipresp = urlopen(link)
    tempzip = open("Temporary/" + tempfile_path, "wb")
    tempzip.write(zipresp.read())
    tempzip.close()
    zf = ZipFile("Temporary/" + tempfile_path)
    zf.extractall(path='Temporary')
    zf.close()
    os.remove("Temporary/" + tempfile_path)
    logging.info(u'Downloading over: ' + link)
    return "Temporary/" + [f for f in listdir("Temporary") if isfile(join("Temporary", f))][1]

def download_production_report(link):
    '''Returns path to csv'''
    logging.info(u"Production report downloading link: " + link)
    response = urlopen(link).read()
    filename = link.split('/')[-1]
    path = os.path.join("Temporary",filename)
    with open(path, 'wb') as f:
        while True:
            chunk = response.read(1024)
            if not chunk:
                break
            f.write(chunk)

    logging.info(u"Production report downloading over, link: " + link)
    return path

def mdb_to_csv(path_to_mdb):
    '''Returns path to .csv file'''
    try:
        logging.info(u'Running mdb-export on %s' % path_to_mdb)
        subprocess.call("mdb-export '%s' 'colorado well completions' > Temporary/result.csv" % path_to_mdb, shell=True)
        logging.info(u'Mdb-export successfully over')
    except Exception as e:
        logging.exception(str(e))
    finally:
        os.remove(path_to_mdb)
    return 'Temporary/result.csv'

def insert_to_database(path_to_csv, query, year, id='summaries'):
    logging.info(u"Inserting into database began")
    inserted_rows_counter = 0
    try:
        with open(path_to_csv, newline='') as csvfile:
            with get_connection() as connection:
                cursor = connection.cursor()
                reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                counter = 0
                for row in reader:
                    if counter != 0:
                        data = [year]
                        for value in row:
                            value = value.strip() if value != "" else None
                            data.append(value)
                        if id == 'summaries' and len(data) < 34:
                            data.append(None)
                        if id == 'wells' and len(data) < 41:
                            data[29:29] = [None,None,None,None]
                            data += [None]
                            data = [value if not isinstance(value,str) else re.sub("\s\s+", " ", value) for value in data]
                        cursor.execute(query, data)
                        inserted_rows_counter += 1
                    counter += 1
                connection.commit()
    finally:
        os.remove(path_to_csv)
    logging.info(u"Inserting into database ended, inserted rows count: " + str(inserted_rows_counter))

def _test_exporting():
    path_to_csv = "Temporary/colorado well completions.csv"
    insert_to_database("Dataexample/colorado well completions.csv", insert_into_well_completions_query, 2016)

if __name__ == "__main__":#    path_to_mdb = download_well_completion("http://cogcc.state.co.us/documents/data/downloads/production/co%202016%20Annual%20Production%20Summary-xp.zip")
    download_and_insert_all_well_completions()
    download_and_insert_all_production_reports()
