from math import ceil
from scheduler import log
from utility.file import openSslEncryptFile, nameFile, getDirTreeDict, addHashesToTree, getDirSize
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
def fullTreeGetFiles(tree, basePath, individualFilesWithHashes, individualFIlesWithPaths):
    for item in tree['contents']:
        if item['type'] == "directory" and item['contents'] != None:
            fullTreeGetFiles(item, basePath+"/"+item['name'], individualFilesWithHashes, individualFIlesWithPaths) 
        else:
            h = {}
            h['nameFake'] = item['nameFake']
            h['hash'] = item['hash']
            individualFilesWithHashes.append(h)
            f = {}
            f['nameFake'] = item['nameFake']
            f['fullPath'] = basePath+"/"+item['name']
            individualFIlesWithPaths.append(f)
              
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
    
    # determine if base path is a file or dir
    individualFilesWithHashes = []
    individualFIlesWithPaths = []
    if task['task']['type'] == 'directory':
        #directory
        log("Base path " + task['task']['name'])
        encryptFullTree(task['task'], task['task']['name'], tempPlace, token, individualFilesWithHashes, individualFIlesWithPaths)
    else:
        raise Exception("Only dir allowed")
    
    if tempPlace is None:
        tempPlace = task['task']['name']
        
    task['task']['individualFilesWithHashes'] = individualFilesWithHashes
    task['task']['individualFilesWithPaths'] = individualFIlesWithPaths
    
    dummyMetadataName = nameFile()
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
    
def syncFilesToRemote(conn, task, tempPlace):
    cur = conn.cursor()
    cur.execute(f"select file_encryption_key from authenticated_user where 'admin' = any(scopes)")
    token = cur.fetchone()
    if token is None:
        raise Exception('No admin key found')
    token = token['file_encryption_key']
    cur.close()
    
    # TODO CHANGE THIS LATER TO SOME OTHER LOGIC BASED ON SYNC INTERVAL?
    # make sure sync task didn't already run
    # cur = conn.cursor()
    # cur.execute(f"select id from taskqueue where id <> {task['id']} and name = 'Sync' and ts between now() - interval '3 HOURS' and now()")
    # old = cur.fetchone()
    # cur.close()
    
    
    # first do the new syncs
    # if not old:
    
    log("Starting sync task for remote " + task['task']['nickname'])
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
                        and status = 'Scheduled'
                    ) as s on s.nickname = r.nickname
                    where s.try < 4
                    and r.nickname = '{task['task']['nickname']}'
                    and s.status = 'Scheduled'""")
    newSyncs = cur.fetchall()
    cur.close()
    for sync in newSyncs:
        log("Start sync for newsync " + sync['nickname'])
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
            log(f"Remote {sync['nickname']} not up or not accepting syncs currently. Will try again later.")
            continue
        # update the syncs task
        cur = conn.cursor()
        cur.execute(f"update taskqueue set status = 'Syncing' where id = {sync['id']}")
        conn.commit()
        cur.close()
        

    # next do the existing syncs
    # get all the existing syncs
    cur = conn.cursor()
    cur.execute(f"""select *
                    from my_syncs
                    where metadata::json ->> 'remote' = '{task['task']['nickname']}'""")
    existingSyncs = cur.fetchall()
    cur.close()
    
    for sync in existingSyncs:
        log("Start sync for existing sync " + sync['name'])

        metadata = sync['metadata']
        # first recalculate the sync
        tree = getDirTreeDict(metadata['name'])
        addHashesToTree(tree, metadata['name'], True)
        # print(tree)
        # print(metadata)
        
        individualFilesWithHashes = []
        individualFIlesWithPaths = []
        if metadata['type'] == 'directory':
            #directory
            log("Base path " + metadata['name'])
            fullTreeGetFiles(tree, tree['name'], individualFilesWithHashes, individualFIlesWithPaths)
        else:
            raise Exception("Only dir supported")

        # first check for new files or updated files
        individualFilesWithHashesChanges = []
        outCount = 0
        for file in individualFilesWithHashes:
            inCount = 0
            origFileExists = False
            for origFile in metadata['individualFilesWithHashes']:
                if individualFIlesWithPaths[outCount]['fullPath'] == metadata['individualFilesWithPaths'][inCount]['fullPath']:
                    origFileExists = True
                    print(file['hash'] + " " + origFile['hash'] + individualFIlesWithPaths[outCount]['fullPath'])
                    # same old file now compare hashes
                    if file['hash'] != origFile['hash']:
                        # file was changed
                        log(f"File {individualFIlesWithPaths[outCount]} was changed. Will trigger a re sync")
                        individualFilesWithHashesChanges.append(file)
                        # encrypt the file
                        err = openSslEncryptFile(token, individualFIlesWithPaths[outCount]['fullPath'], tempPlace+"/"+file['nameFake'])
                        if err:
                            print("err got here")
                            print(err)
                            raise(Exception(err))
                    break
                inCount = inCount + 1
            if not origFileExists:
                # this file is new
                log(f"File {individualFIlesWithPaths[outCount]} is new. Will trigger it to sync")
                # add the other metadata
                individualFilesWithHashesChanges.append(file)
                # encrypt the file
                err = openSslEncryptFile(token, individualFIlesWithPaths[outCount]['fullPath'], tempPlace+"/"+file['nameFake'])
                if err:
                    print("err got here")
                    print(err)
                    raise(Exception(err))
            outCount = outCount + 1
            
        filesToMarkDelete = []
        # next check for any deleted files
        for origFile in metadata['individualFilesWithPaths']:
            fileStillExists = False
            for file in individualFIlesWithPaths:
                if file['fullPath'] == origFile['fullPath']:
                    fileStillExists = True
                    break
            if not fileStillExists:
                # file was deleted but in case something went wrong will notify the user before remote deletion happens
                log(f"File {origFile['fullPath']} was deleted locally. Will log the file to be deleted on remote after confirmation")
                filesToMarkDelete.append(origFile)
                
        
            
        if len(individualFilesWithHashesChanges) > 0 or len(filesToMarkDelete) > 0:
            
            # calulate the size difference (already including deletes technically)
            newSize = int(getDirSize(metadata['name']))
            
            cur = conn.cursor()
            cur.execute(f"select * from my_remotes where nickname = '{metadata['remote']}'")
            remote = cur.fetchall()
            cur.close()
            remainingSize = remote[0]['remaining_space_in_mb']
            log("Space remaining on remote " + str(remainingSize) + " MB")
            amountRemaining = int(remainingSize) - 10 # 10mb buffer
            
            if amountRemaining <= newSize:
                raise Exception("Not enough space on remote for changed files")
            
            # handle the changed
            changedData = {}
            changedData['fileChanges'] = individualFilesWithHashesChanges
            changedData['remoteName'] = metadata['remote']
            changedData['syncName'] = metadata['syncFakeName']
            
            
            # update the metadata...
            # props to update: contents, individualFilesWithPaths, individualFilesWithHashes
            if metadata['contents'] != None:
                metadata['contents'] = tree['contents']
            metadata['individualFilesWithPaths'] = individualFIlesWithPaths
            metadata['individualFilesWithHashes'] = individualFilesWithHashes
            
            dummyMetadataName = metadata['syncFakeName']
            inp = tempPlace+"/"+"metadata."+dummyMetadataName
            log("creating metadata json file for sync update")
            # create metadata file
            with open(inp+".json", "w") as metadataFile:
                data = json.dumps(metadata)
                metadataFile.write(data)
            # encrypt the metadata file
            err = openSslEncryptFile(token, inp+".json", inp+".enc")
            if err:
                raise(Exception(err))
            os.remove(inp+".json")
            
            # for these we'll just start up separate tasks since this one is way too long. (DRY right? kek)
            
            if len(filesToMarkDelete) > 0:
                # handle the deleted 
                cur = conn.cursor()
                log("insert pending deleted " + str(filesToMarkDelete))
                cur.execute(f"insert into pending_file_deletes (remote, sync, metadata) values('{metadata['remote']}', '{metadata['syncFakeName']}', '{json.dumps(filesToMarkDelete)}')")
                cur.close()
            
            if len(individualFilesWithHashesChanges) > 0:
                cur = conn.cursor()
                log("insert sync updates task " + str(changedData))
                cur.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Sync update', '{json.dumps(changedData)}', NOW(), 'Queued', 0, NOW())")
                cur.close()
                
            conn.commit()
        else:
            log(f"No changed files for sync {sync}")

    # complete task
    updateTaskStatus(conn, 'Complete', task)
    
def syncUpdate(conn, task):
    log("In sync update")
    
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
        log(f"Remote {task['task']['remote']} not up or not accepting syncs currently. Will try again later. The actual error code was: {req.status_code}")
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
    
    # increase db space taken and reduce available
    cur = conn.cursor()
    cur.execute(f"""update hosted_remotes 
                    set used_space_in_mb = used_space_in_mb + '{sizeNeeded}', 
                    remaining_space_in_mb = remaining_space_in_mb - '{sizeNeeded}' 
                    where nickname = '{task['task']['remote']}'""")
    cur.close()
    conn.commit()
    
    if task['task']['index'] == task['task']['numInSync']:
        log("last file in sync reached")
        # mark the sync as synced
        cur = conn.cursor()
        cur.execute(f"""update hosted_syncs 
                        set status = 'Synced'
                        where name = '{task['task']['syncName']}'""")
        cur.close()
        conn.commit()
        # notify the remote that the sync is completely retrieved
        params = {'name': task['task']['remote'], 'token': task['task']['token'], 'syncName': task['task']['syncName']}
        req = requests.get(f"{addr}/file/notifySyncComplete", params=params)
        if req.status_code != 200:
            log(f"Remote {task['task']['remote']} not up or not accepting syncs currently. Will try again later. The actual error code was: {req.status_code}")
            raise Exception
    
    updateTaskStatus(conn, 'Complete', task)
    
# delete temp files and complete the task
def cleanupNewSync(task, tempPlace, conn):
    if tempPlace is None:
        tempPlace = task['task']['name']
        
    for file in task['task']['individualFilesWithPaths']:
        fakeName = file['nameFake']
        if os.path.exists(f"{tempPlace}/{fakeName}"):
            log(f"Removing temporary file: {tempPlace}/{fakeName}")
            os.remove(f"{tempPlace}/{fakeName}")
        else:
            log(f"Temp file not found {tempPlace}/{fakeName}")
    # clean metadata file
    if os.path.exists(f"{task['task']['metadataFilePath']}"):
            log(f"Removing temporary file: {task['task']['metadataFilePath']}")
            os.remove(f"{task['task']['metadataFilePath']}")
    log("Cleaned up files in sync {task}.")
            
    updateTaskStatus(conn, 'Complete', task)