import time
import psycopg
from psycopg.rows import dict_row
import json
import signal
from datetime import datetime, timezone
from utility.task import *
from logs.customlogger import CustomLog
from utility.psycopgUtil import getConn

conn = None
log = CustomLog("Scheduler")
    
def insertScheduleTask(task, nickname):
    cur = conn.cursor()
    log.info("Triggering task " + str(task) + " " + nickname)
    try:
        d = {}
        d['nickname'] = nickname
        cur.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('{task}', '{json.dumps(d)}', NOW(), 'Queued', 0, NOW())")
        conn.commit()
    except Exception as e:
        log.error("", e)
    cur.close()
    
def scheduleSyncsAllRemotes():
    cur = conn.cursor()
    cur.execute(f"select * from my_remotes")
    remotes = cur.fetchall()
    cur.close()
    for r in remotes:
        insertScheduleTask("Sync", r['nickname'])

def deleteOldLogsAndQueueTasks():
    with conn.cursor() as cur:
        cur.execute(f"""delete from logs where ts < NOW() - INTERVAL '2 weeks'""")
        cur.execute(f"""delete from taskqueue where ts < NOW() - INTERVAL '2 weeks'""")
        conn.commit()
        
def deleteExpiredRefreshTokens():
    print("in here")
    tokenObjects = []
    with conn.cursor() as cur:
        cur.execute(f"""select refresh_token_data, username from authenticated_user""")
        tokenObjects = cur.fetchall()

    for tokenObject in tokenObjects:
        changed = False
        actualTokenObj = tokenObject['refresh_token_data']
        for t in actualTokenObj:
            if round(t['refresh_token_expiration']) <= round(datetime.now(timezone.utc).timestamp()):
                log.info(f"Cleaning refresh token {t} for {tokenObject['username']}" )
                actualTokenObj.remove(t)
                changed = True
        if changed:
            tokenObject['refresh_token_data'] = actualTokenObj
            with conn.cursor() as cur:
                cur.execute(f"""update authenticated_user 
                                set refresh_token_data = '{json.dumps(actualTokenObj)}'
                                where username = '{tokenObject['username']}'""")
                conn.commit()
                log.info(f"Cleaned tokens for user {tokenObject['username']} done")
    
    
# runs tasks at their scheduled time
def scheduleTasks():
    currentHour = datetime.now().strftime("%H")
    currentMin = datetime.now().strftime("%M")
    
    # master sync task
    # should run at start of sync interval
    if int(currentHour) == 22 and int(currentMin) == 1:
        scheduleSyncsAllRemotes()
        
    if int(currentMin) == 0 or int(currentMin) == 30:
        log.info("Logging any errored out tasks")
        with conn.cursor() as cur:
            cur.execute(f"update taskqueue set status = 'Failed' where try >= 4 and status <> 'Failed' and status <> 'Complete'")
            conn.commit()
    
    if int(currentHour) == 2 and int(currentMin) == 5:
        deleteOldLogsAndQueueTasks()
    
    if int(currentHour) == 14 and int(currentMin) == 14:
        deleteExpiredRefreshTokens()

def handler(signum, frame):
    msg = "Scheduler: Ctrl-c was pressed. "
    print(msg, end="", flush=True)
    conn.close()    
    exit(1)
        
if __name__ == "__main__":
    conn = getConn()
    cur = conn.cursor()
    
    # get needed properties
    # cur.execute("select props from properties")
    # props = cur.fetchone()['props']
    # cur.close()
    
    log.info("Starting scheduler loop")
    
    signal.signal(signal.SIGINT, handler) # catch sigint to close db conn first
    
    while True:
        try:
            # check if its time for a scheduled task
            scheduleTasks()
            
            time.sleep(60)
        except Exception as e:
            log.error("", e)
            time.sleep(60) # sleep 30s to check before checking for tasks again