import datetime

def test():
    try:
        fh = open("Temporary/cronlog.txt", 'r')
        fh.close()
    except:
        # if file does not exist, create it
        fh = open("Temporary/cronlog.txt", 'w')
        fh.close()
    with open("Temporary/cronlog.txt", 'a') as file_handler:
        current_time = datetime.datetime.now()
        file_handler.write("Test cron job at: " + str(current_time))

if __name__ == "__main__":
    test()

