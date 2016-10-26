import os
import logging
from urllib.request import urlopen
from zipfile import ZipFile

tempfile_path = 'tempfile.zip'
logging.basicConfig(level = logging.DEBUG)
years = [range(1999, 2017)]
production_summaries_link = "http://cogcc.state.co.us/documents/data/downloads/production/co%20{0}%20Annual%20Production%20Summary-xp.zip"
well_completions_link = "http://cogcc.state.co.us/documents/data/downloads/production/%_prod_reports.zip"
well_completions_current_year_link = "http://cogcc.state.co.us/documents/data/downloads/production/monthly_prod.zip"

def download_and_insert_all_well_completions():
    links = [well_completions_link.format(year) for year in years] + well_completions_current_year_link
    logging.debug(u'Starting to download well complections')

    for link in links:
        download_and_insert_well_complection(link)

def download_and_insert_well_complection(link):
    def download(link):
        logging.debug(u'Downloading: ' + link)
        zipresp = urlopen(link)
        tempzip = open("Temporary/" + tempfile_path, "wb")
        tempzip.write(zipresp.read())
        tempzip.close()
        zf = ZipFile("Temporary/" + tempfile_path)
        zf.extractall(path='Temporary')
        zf.close()
        os.remove("Temporary/" + tempfile_path)
        logging.debug(u'Downloading over: ' + link)

    def process():
        pass

    download(link)

if __name__ == "__main__":
    download_and_insert_well_complection("http://cogcc.state.co.us/documents/data/downloads/production/co%202016%20Annual%20Production%20Summary-xp.zip")
