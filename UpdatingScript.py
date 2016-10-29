import Downloader
import logging

def update():
    logging.info(u"Cron job started")
    Downloader.download_and_insert_last_production_report()
    Downloader.download_and_insert_last_well_completion()
    logging.info(u"Cron job over")

if __name__ == "__main__":
    update()
