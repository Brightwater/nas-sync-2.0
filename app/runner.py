import time
import psycopg
from psycopg.rows import dict_row
import json
import signal
from datetime import datetime
from utility.task import *
from scheduler import log

# # https://crontab.guru/#45_17_*_*_*

TEMP_WITH_ORIGINAL = True
TEMP_PATH = None
conn = None


def handler(signum, frame):
    msg = "Scheduler: Ctrl-c was pressed. "
    print(msg, end="", flush=True)
    conn.close()    
    exit(1)
    
def errorOutTask(task):
    cur = conn.cursor()
    log("Erroring out task " + str(task))
    try:
        cur.execute(f"update taskqueue set status = 'Failed' where id = {task['id']}")
        conn.commit()
    except Exception as e:
        print(e)
    cur.close()
    
# process the task
# each task must handle marking itself complete
def processTask(task):
    try:
        name = task['name']
        # determine task type
        if name == 'Add sync path':
            addSyncPathTask(task, TEMP_PATH, conn)
            
        elif name == 'Sync':
            syncFilesToRemote(conn, task)
            
        elif name == 'Retrive file from remote':
            downloadFileFromRemote(conn, task)
        else:
            # probably shouldn't be a real thing
            log("Unsupported task type, erroring it out")
            errorOutTask(task)
            return
        log("Finished task " + name)
    except Exception as e:
        log(e)
        errorOutTask(task)

def readTaskQueue():
    cur = conn.cursor()
    try:
        cur.execute("select * from taskqueue where try < 4 and status <> 'Scheduled' and status <> 'Complete' order by ts asc")
        task = cur.fetchone()
        if task:
            tryNum = int(task['try']) + 1
            cur.execute(f"update taskqueue set try = {tryNum} where id = {task['id']}")
            conn.commit()
    except Exception as e:
        log(e)
        task = None
    cur.close()
    return task
        
if __name__ == "__main__":
    conn = psycopg.connect(user = "postgres",
                                password = "",
                                host = "127.0.0.1",
                                port = "5432",
                                dbname = "nassync",
                                row_factory=dict_row )
    cur = conn.cursor()
    
    # get needed properties
    cur.execute("select props from properties")
    props = cur.fetchone()['props']
    cur.close()
    TEMP_WITH_ORIGINAL = props['tempWithOriginal']
    if not TEMP_WITH_ORIGINAL:
        TEMP_PATH = props['tempPath']
    log("Setting temp path " + TEMP_PATH)
    
    log("Starting task loop")
    
    signal.signal(signal.SIGINT, handler) # catch sigint to close db conn first
    time.sleep(5)
    
    while True:
        try:
            # read task from task queue
            task = readTaskQueue()
            if task:
                log("Processing task: " + task['name'])
                processTask(task)
            else:
                time.sleep(120) # move to else later
        except Exception as e:
            print(e)
            time.sleep(120) # sleep 30s to check before checking for tasks again

