from __future__ import print_function

import os
import sys
import threading
import multiprocessing

try:
    import Queue
except ImportError:
    import queue as Queue

# this is the fastest way to replace something in a big file
# if you use sed or any other tools it will take ages
# but with this code you can replace it so fast


REPLACEMENT_UUID_FILE = 'output/tabelname_uuid_replacements.txt'
SQL_DUMP_FILE= 'output/tablename.sql'
# this is the final file you need to import to mysql
OUTPUT_CLEAN_FILE = 'output/tablename_final.sql'

with open(REPLACEMENT_UUID_FILE, 'r') as f:
    UUIDS = f.readlines()
    f.close()

TOTAL = 0
STATE = 0

def bar(current):
    global TOTAL
    precent = current * 100 / TOTAL
    precent = int(precent)
    sys.stdout.write(('=' * precent) + ('' * (100 - precent)) + (
            "\r[ " + str(TOTAL) + " / " + str(current) + " ] [ %d" % precent + "% ] [ REPLACE UUIDS ] "))
    sys.stdout.flush()


def replace(line):
    # global STATE

    try:
        for uuids in UUIDS:
            replacements = uuids.split(':')
            line = line.replace(replacements[0], replacements[1].replace("\r", "").replace("\n", ""))

        with open(OUTPUT_CLEAN_FILE, "a") as output:
            output.write(line)

        STATE += 1
        bar(STATE)
    except Exception as e:
        print(e)


def starter():
    """
    threading workers initialize
    """
    global TOTAL

    queue = Queue.Queue()
    threads = []
    max_thread = 50

    queuelock = threading.Lock()

    p = multiprocessing.Pool(10)
    m = multiprocessing.Manager()
    event = m.Event()

    with open(SQL_DUMP_FILE) as f:
        queue_size = 0
        for line in f:
            p.apply_async(replace, (line,))
        print("Queue Size: %s" % queue_size)

    event.wait()
    sys.exit()

    try:
        time.sleep(10)

    except  (KeyboardInterrupt, SystemExit):
        print
        "Caught KeyboardInterrupt, terminating workers"
        p.terminate()
        p.join()
    except:
        pass
    else:
        p.close()
        p.join()

if __name__ == "__main__":
    starter()
