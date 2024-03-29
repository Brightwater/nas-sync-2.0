import time
import psycopg
from psycopg.rows import dict_row
import json
import signal
from datetime import datetime
from utility.task import *
import traceback
from logs.customlogger import CustomLog
from utility.psycopgUtil import getConn

# # https://crontab.guru/#45_17_*_*_*

TEMP_WITH_ORIGINAL = True
TEMP_PATH = None
conn = None
cur = None
task = None
log = CustomLog("Runner")

def handler(signum, frame):
    msg = "Scheduler: Ctrl-c was pressed. "
    print(msg, end="", flush=True)
    if cur:
        conn.rollback()
        cur.close()
    conn.close()    
    exit(1)
    
def errorOutTask(task):
    cur = conn.cursor()
    log.error("Erroring out task " + str(task))
    try:
        cur.execute(f"update taskqueue set retry_ts = NOW() + 15 * interval '1 second' where id = {task['id']}")
        conn.commit()
    except Exception as e:
        log.error("", e)
        if cur:
            cur.close()
    cur.close()
  
def test(TEMP_PATH, conn, task):
    print("TESTTTTTTTTTTT")
    time.sleep(10)
      
taskMap = {
    'Add sync path': lambda: addSyncPathGetSubTask(task, TEMP_PATH, conn),
    'Sync': lambda: syncFilesToRemote(conn, task, TEMP_PATH),
    'Retrieve file from remote': lambda: downloadFileFromRemote(conn, task),
    'Sync update': lambda: syncUpdateGetSubTask(task, TEMP_PATH, conn),
    'Delete sync files': lambda: deleteSyncFiles(conn, task)
    # 'Sync delete': lambda: triggerFileDelete(conn, task, TEMP_PATH)
}
    
# process the task
# each task must handle marking itself complete
####could use an options map here instead of globals and 
####pass options as param to the lambda like 
####taskMap[name](options) then lambda: options: (...)
def processTask(task):
    try:
        name = task['name']
        taskMap[name]()
        log.info("Finished task " + name)
    except Exception as e:
        log.error(traceback.format_exc(), e)
        errorOutTask(task)

def readTaskQueue():
    cur = conn.cursor()
    try:
        cur.execute("""select * 
                    from taskqueue 
                    where try < 4 
                    and status <> 'Scheduled' 
                    and status <> 'Complete' 
                    and status <> 'Syncing'
                    order by ts asc""")
        task = None
        tasks = cur.fetchall()
        cur.close()
        for t in tasks:
            if datetime.now() > t['retry_ts']:
                task = t
                break
        
        if task:
            cur = conn.cursor()
            tryNum = int(task['try']) + 1
            cur.execute(f"update taskqueue set try = {tryNum} where id = {task['id']}")
            conn.commit()
            cur.close()
        elif len(tasks) > 0:
            log.info(f"Next retry task will be at: {tasks[0]['retry_ts']}")
    except Exception as e:
        log(e)
        task = None
        if cur:
            cur.close()
    return task
        
if __name__ == "__main__":
    conn = getConn()
    cur = conn.cursor()
    
    # get needed properties
    cur.execute("select props from properties")
    props = cur.fetchone()['props']
    cur.close()
    
    props['startup_time'] = time.time()
    with conn.cursor() as cur:
        cur.execute(f"update properties set props = '{json.dumps(props)}'")
        conn.commit()
    
    TEMP_WITH_ORIGINAL = props['tempWithOriginal']
    if not TEMP_WITH_ORIGINAL:
        TEMP_PATH = props['tempPath']
    log.info("Setting temp path " + TEMP_PATH)
    
    log.info("Starting task loop")
    
    signal.signal(signal.SIGINT, handler) # catch sigint to close db conn first
    time.sleep(5)
    
    while True:
        try:
            # read task from task queue
            # print("TASK LOOP")
            task = readTaskQueue()
            if task:
                log.info("Processing task: " + task['name'] + " task status: " + task['status'])
                processTask(task)
            else:
                time.sleep(15) # move to else later
        except Exception as e:
            log.error("", e)
            time.sleep(15) # sleep 30s to check before checking for tasks again

