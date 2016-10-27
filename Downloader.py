import os
import csv
from os import listdir
from os.path import isfile, join
import logging
from urllib.request import urlopen
from zipfile import ZipFile
from datetime import date
from CreateTables import column_names, tables_names
from DatabaseConnection import get_connection
import subprocess

insert_into_well_completions_query = "INSERT INTO {0}({1}) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(tables_names[0], ','.join(column_names[0]))
tempfile_path = 'tempfile.zip'
logging.basicConfig(level = logging.DEBUG)
years = [range(1999, 2016)]
production_summaries_link = "http://cogcc.state.co.us/documents/data/downloads/production/co%20{0}%20Annual%20Production%20Summary-xp.zip"
well_completions_link = "http://cogcc.state.co.us/documents/data/downloads/production/%_prod_reports.zip"
well_completions_current_year_link = "http://cogcc.state.co.us/documents/data/downloads/production/monthly_prod.zip"

def download_and_insert_all_well_completions():
    links = [(year,production_summaries_link.format(year)) for year in years] + (date.today().year, well_completions_current_year_link)
    logging.info(u'Starting to download well complections')

    for year, link in links:
        download_well_completion(link)

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

def mdb_to_csv(path_to_mdb):
    '''Returns path to .csv file'''
    try:
        logging.info(u'Running mdb-export on %s' % path_to_mdb)
        subprocess.call("mdb-export '%s' 'colorado well completions' > Temporary/result.csv" % path_to_mdb, shell=True)
        os.remove(path_to_mdb)
        logging.info(u'Mdb-export successfully over')
    except Exception as e:
        logging.error(u'Message: ' + str(e))
    return 'Temporary/result.csv'

def insert_to_database(path_to_csv, year):
    logging.info(u"Inserting into database began")
    inserted_rows_counter = 0
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
                    cursor.execute(insert_into_well_completions_query, data)
                    inserted_rows_counter += 1
                counter += 1
            connection.commit()
            os.remove(path_to_csv)
    logging.info(u"Inserting into database ended, inserted rows count: " + str(inserted_rows_counter))

def _test_exporting():
    path_to_csv = "Temporary/colorado well completions.csv"
    insert_to_database("Dataexample/colorado well completions.csv", 2016)

if __name__ == "__main__":#    path_to_mdb = download_well_completion("http://cogcc.state.co.us/documents/data/downloads/production/co%202016%20Annual%20Production%20Summary-xp.zip")
    mdb = download_well_completion("http://cogcc.state.co.us/documents/data/downloads/production/co%202016%20Annual%20Production%20Summary-xp.zip")
    logging.info(u'Path to mdb: ' + mdb)
    csv = mdb_to_csv(mdb)
    logging.info(u'Path to csv: ' + csv)