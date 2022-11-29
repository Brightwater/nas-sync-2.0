import time
import psycopg
from psycopg.rows import dict_row
import json
import signal
from datetime import datetime
from utility.task import *

conn = None

def log(msg):
    print("Scheduler - " + str(datetime.now()) + ": " + str(msg))
    
def insertScheduleTask(task):
    cur = conn.cursor()
    log("Triggering task " + str(task))
    try:
        cur.execute(f"insert into taskqueue (name, task, ts, status, try) values('{task}', null, NOW(), 'Queued', 0)")
        conn.commit()
    except Exception as e:
        print(e)
    cur.close()

# runs tasks at their scheduled time
def scheduleTasks():
    currentHour = datetime.now().strftime("%H")
    currentMin = datetime.now().strftime("%M")
    
    # master sync task
    # should run at start of sync interval
    if int(currentHour) == 1 and int(currentMin) == 15:
        insertScheduleTask("Sync")
        
    # run at end of sync interval ish
    #if currentHour == 10 and currentMin == 0:
    #    syncRanToday = False

def handler(signum, frame):
    msg = "Scheduler: Ctrl-c was pressed. "
    print(msg, end="", flush=True)
    conn.close()    
    exit(1)
        
if __name__ == "__main__":
    conn = psycopg.connect(user = "postgres",
                                    password = "",
                                    host = "127.0.0.1",
                                    port = "5432",
                                    dbname = "nassync",
                                    row_factory=dict_row)
    cur = conn.cursor()
    
    # get needed properties
    # cur.execute("select props from properties")
    # props = cur.fetchone()['props']
    # cur.close()
   
    
    log("Starting scheduler loop")
    
    signal.signal(signal.SIGINT, handler) # catch sigint to close db conn first
    
    while True:
        try:
            # check if its time for a scheduled task
            scheduleTasks()
            
            time.sleep(60)
        except Exception as e:
            print(e)
            time.sleep(60) # sleep 30s to check before checking for tasks again