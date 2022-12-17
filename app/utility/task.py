from math import ceil
from scheduler import log
from utility.file import openSslEncryptFile, nameFile
import json
import pathlib
import os
import requests
import shutil

def determineAddress(address, port):
    if port == None or port == "":
        return address
    else:
        return "http://" + address + ":" + str(port)

def updateTaskStatus(conn, status, task):
    cur = conn.cursor()
    cur.execute(f"update taskqueue set status = '{status}' where id = {task['id']}")
    conn.commit()
    cur.close()
    
# recursive tree read
def encryptFullTree(tree, basePath, tempPlace, token, individualFilesWithHashes, individualFIlesWithPaths):
    for item in tree['contents']:
        if item['type'] == "directory" and item['contents'] != None:
            encryptFullTree(item, basePath+"/"+item['name'], tempPlace, token, individualFilesWithHashes, individualFIlesWithPaths) 
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
            h = {}
            h['nameFake'] = item['nameFake']
            h['hash'] = item['hash']
            individualFilesWithHashes.append(h)
            f = {}
            f['nameFake'] = item['nameFake']
            f['fullPath'] = basePath+"/"+item['name']
            individualFIlesWithPaths.append(f)

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
    individualFilesWithHashes = []
    individualFIlesWithPaths = []
    if task['task']['type'] == 'directory':
        #directory
        log("Base path " + task['task']['name'])
        encryptFullTree(task['task'], task['task']['name'], tempPlace, token, individualFilesWithHashes, individualFIlesWithPaths)
    else:
        # encrypt the file here...
        if tempPlace is None:
            tempPlace = task['task']['name']
        log('encrypting file Name: ' + task['task']['name'] + " Fake name: " + task['task']['nameFake'])
        parent = str(pathlib.Path(tempPlace).parent)
        err = openSslEncryptFile(token, task['task']['name'], parent+"/"+task['task']['nameFake'])
        if err:
            raise(Exception(err))
        h = {}
        h['nameFake'] = task['task']['nameFake']
        h['hash'] = task['task']['hash']
        individualFilesWithHashes.append(h)
        f = {}
        f['nameFake'] = task['task']['nameFake']
        f['fullPath'] = task['task']['name']
        individualFIlesWithPaths.append(f)
    
    if tempPlace is None:
        tempPlace = task['task']['name']
        
    task['task']['individualFilesWithHashes'] = individualFilesWithHashes
    task['task']['individualFilesWithPaths'] = individualFIlesWithPaths
    
    dummyMetadataName = nameFile()
    if parent != "":
        inp = parent+"/"+"metadata."+dummyMetadataName
    else:
        inp = tempPlace+"/"+"metadata."+dummyMetadataName
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
    
    task['task']['metadataFileName'] = "metadata."+dummyMetadataName+".enc"
    task['task']['metadataFilePath'] = inp+".enc"
    task['task']['syncFakeName'] = dummyMetadataName
    
    cur = conn.cursor()
    cur.execute(f"update taskqueue set status = 'Scheduled', task = '{json.dumps(task['task'])}', try = 0 where id = {task['id']}")
    conn.commit()
    cur.close()
    
def syncFilesToRemote(conn, task):
    
    # make sure sync task didn't already run
    cur = conn.cursor()
    cur.execute(f"select id from taskqueue where id <> {task['id']} and name = 'Sync' and ts between now() - interval '3 HOURS' and now()")
    old = cur.fetchone()
    cur.close()
    # first do the new syncs
    if not old:
        log("Starting sync task")
        # insert new task
        
        # get new sync dirs
        cur = conn.cursor()
        # cur.execute(f"select * from taskqueue where name = 'Add sync path'")
        cur.execute(f"""select r.address, r.token, r.port, s.*
                        from my_remotes r
                        inner join lateral 
                        (
                            select task::json ->> 'remote' as nickname, 
                            t2.* 
                            from taskqueue t2
                            where name = 'Add sync path'
                        ) as s on s.nickname = r.nickname
                        where s.try < 4""")
        newSyncs = cur.fetchall()
        cur.close()
        for sync in newSyncs:
            addr = determineAddress(sync['address'], sync['port'])
            # ping the remote to make sure it's ready
            params = {'name': sync['nickname'], 'token': sync['token']}
            try:
                req = requests.get(f'{addr}/file/checkIfInInterval', params=params)
                if req.status_code != 200:
                    raise Exception
            except:
                log(f"Remote {sync['nickname']} {sync['address']} not up or not accepting syncs currently. Will try again later.")
                continue
            
            log(f"Triggering sync for {sync['nickname']}")
            
            #build object to send the remote
            sendObj = {}
            sendObj['syncSize'] = sync['task']['syncSize']
            sendObj['name'] = sync['task']['syncFakeName']
            sendObj['metadataFileName'] = sync['task']['metadataFileName']
            sendObj['individualFilesWithHashes'] = sync['task']['individualFilesWithHashes']
            
            # trigger the sync on the remote
            # and post the files it will need to download
            
            
            params = {'name': sync['nickname'], 'token': sync['token']}
            try:
                req = requests.post(f'{addr}/file/syncFromRemote', params=params, json=sendObj)
                if req.status_code != 200:
                    raise Exception
            except:
                log(f"Remote {sync['nickname']} not up or not accepting syncs currently. 222 Will try again later.")
                continue
            #update the syncs task
            cur = conn.cursor()
            cur.execute(f"update taskqueue set status = 'Syncing' where id = {sync['id']}")
            conn.commit()
            cur.close()
            #insert the sync in db?? no not yet
            
    # next do the existing syncs
    
    # complete task
    updateTaskStatus(conn, 'Complete', task)
    
def downloadFileFromRemote(conn, task):
    log(f"Starting file download for fake name {task['task']['nameFake']}")
    
    # get remaining hosted remote space
    cur = conn.cursor()
    cur.execute(f"select remaining_space_in_mb from hosted_remotes where nickname = '{task['task']['remote']}'")
    remainingSpace = cur.fetchone()
    cur.close()
    remainingSpace = remainingSpace['remaining_space_in_mb']
    
    # open the file (stream it)
    # download the file from the remote api
    
    # first get the size of the file and make sure you have enough room for it
    # ping the remote to make sure it's ready
    params = {'name': task['task']['remote'], 'token': task['task']['token'], 'fakeName': task['task']['nameFake']}
    addr = determineAddress(task['task']['address'], task['task']['port'])
    # name: str, token: str, fakeName
    req = requests.get(f"{addr}/file/getFileSize", params=params)
    if req.status_code != 200:
        log(f"Remote {task['task']['remote']} not up or not accepting syncs currently. Will try again later.")
        raise Exception
    
    sizeNeeded = int(ceil(float(req.text)))

    # lets not let the disk go below 5% empty space
    diskStat = shutil.disk_usage(task['task']['filePath'])
    diskTotal = diskStat.total*0.000001
    diskFree = diskStat.free*0.000001
    diskRemainingAfterFile = diskFree - sizeNeeded # change to file size
    remainingSpaceAfterFile = remainingSpace - sizeNeeded
    
    if diskRemainingAfterFile / diskTotal <= 0.05 or remainingSpaceAfterFile <= 0:  
        raise Exception(f"Not enough space on disk for the file")  
    
    try:
        # Create a request object with the URL
        r = requests.get(f"{addr}/file/downloadForSync", stream=True, params=params)

        # Check the HTTP status code of the response
        if r.status_code == 200:
            # Open a local file to save the downloaded data
            with open(f"{task['task']['filePath']}/{task['task']['nameFake']}", "wb") as f:
                # Set a chunk size for the download
                chunk_size = 1024*1024 # 1mb

                # Iterate over the response data and write it to the file in chunks
                for chunk in r.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
        else:
            # If the HTTP status code is not 200, raise an error
            log(f"Remote {task['task']['remote']} not up or not accepting syncs currently. Will try again later.")
            raise ValueError(f"HTTP status code {r.status_code}")
    except Exception as e:
        # If there was an error making the request, raise an error
        raise Exception(f"Request error: {e}")
    
    # double check the file to see if it took the stated amount of space. if it didn't then delete it
    # and raise error
    actualSize = os.path.getsize(f"{task['task']['filePath']}/{task['task']['nameFake']}") * 0.000001
    if actualSize > sizeNeeded:
        log(f"File size not as reported by remote server. Deleting the file. ACTUAL SIZE: {actualSize}, STATED SIZE: {sizeNeeded}")
        if os.path.exists(f"{task['task']['filePath']}/{task['task']['nameFake']}"):
            os.remove(f"{task['task']['filePath']}/{task['task']['nameFake']}")
        raise Exception("Incorrectly sized file from remote")
    
    if task['task']['index'] == task['task']['numInSync']:
        log("last file in sync reached")
    
    # increase db space taken and reduce available
    cur = conn.cursor()
    cur.execute(f"""update hosted_remotes 
                    set used_space_in_mb = used_space_in_mb + '{sizeNeeded}', 
                    remaining_space_in_mb = remaining_space_in_mb - '{sizeNeeded}' 
                    where nickname = '{task['task']['remote']}'""")
    cur.close()
    conn.commit()
    
    updateTaskStatus(conn, 'Complete', task)