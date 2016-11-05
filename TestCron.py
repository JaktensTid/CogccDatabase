import datetime
import Scraper
def test():
    try:
        fh = open("/home/Cogcc/CogccDatabase/Temporary/cronlog.txt", 'r')
        fh.close()
    except:
        # if file does not exist, create it
        fh = open("/home/Cogcc/CogccDatabase/Temporary/cronlog.txt", 'w')
        fh.close()
    with open("/home/Cogcc/CogccDatabase/Temporary/cronlog.txt", 'a') as file_handler:
        current_time = datetime.datetime.now()
        file_handler.write("Test cron job at: " + str(current_time) + '\n')


