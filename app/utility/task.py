from scheduler import log
from utility.file import openSslEncryptFile
import json
import pathlib
import os

def updateTaskStatus(conn, status, task):
    cur = conn.cursor()
    cur.execute(f"update taskqueue set status = '{status}', try = 0 where id = {task['id']}")
    conn.commit()
    cur.close()
    
# recursive tree read
def encryptFullTree(tree, basePath, tempPlace, token):
    for item in tree['contents']:
        if item['type'] == "directory" and item['contents'] != None:
            encryptFullTree(item, basePath+"/"+item['name'], tempPlace, token) 
        else:
            # encrypt the file here...
            if tempPlace is None:
                tempPlace = basePath
            log('encrypting file Name: ' + item['name'] + " Fake name: " + item['nameFake'])
            err = openSslEncryptFile(token, basePath+"/"+item['name'], tempPlace+"/"+item['nameFake'])
            if err:
                print("err got here")
                print(err)
                raise(Exception(err))

def addSyncPathTask(task, tempPlace, conn):
    cur = conn.cursor()
    
    cur.execute(f"select file_encryption_key from authenticated_user where 'admin' = any(scopes)")
    token = cur.fetchone()
    if token is None:
        raise Exception('No admin key found')
    token = token['file_encryption_key']
    
    cur.close()
    parent = ""
    # determine if base path is a file or dir
    if task['task']['type'] == 'directory':
        #directory
        log("Base path " + task['task']['name'])
        encryptFullTree(task['task'], task['task']['name'], tempPlace, token)
    else:
        # encrypt the file here...
        if tempPlace is None:
            tempPlace = task['task']['name']
        log('encrypting file Name: ' + task['task']['name'] + " Fake name: " + task['task']['nameFake'])
        parent = str(pathlib.Path(tempPlace).parent)
        err = openSslEncryptFile(token, task['task']['name'], parent+"/"+task['task']['nameFake'])
        if err:
            raise(Exception(err))
    
    if tempPlace is None:
        tempPlace = task['task']['name']
    
    if parent != "":
        inp = parent+"/"+"metadata"
    else:
        inp = tempPlace+"/"+"metadata"
    log("creating metadata json file")
    # create metadata file
    with open(inp+".json", "w") as metadata:
        # Writing data to a file
        data = json.dumps(task['task'])
        metadata.write(data)
    # encrypt the metadata file
    
    err = openSslEncryptFile(token, inp+".json", inp+".enc")
    if err:
        raise(Exception(err))
    os.remove(inp+".json")
    
    updateTaskStatus(conn, 'Scheduled', task)
    
def syncFilesToRemote(conn, task):
    
    # make sure sync task didn't already run
    cur = conn.cursor()
    cur.execute(f"select id from taskqueue where id <> {task['id']} and name = 'Sync' and ts between now() - interval '3 HOURS' and now()")
    old = cur.fetchone()
    cur.close()
    if not old:
        log("Starting sync task")
        # insert new task
        cur = conn.cursor()
        cur.execute(f"update taskqueue set status = '{status}', try = 0 where id = {task['id']}")
        conn.commit()
        cur.close()
        # get remotes
        
        # get new sync dirs
        
        #sync
    
    
    
    
    # complete task
    updateTaskStatus(conn, 'Complete', task)