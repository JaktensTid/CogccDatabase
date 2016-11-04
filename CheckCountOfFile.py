import subprocess
import sys
if __name__ == "__main__":
    process = subprocess.call('cat %s | sed s/%s/\\n/g | wc -w' % (sys.argv[1],sys.argv[2]), shell=True)
    print(process)
