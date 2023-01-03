from math import ceil
from scheduler import log
from utility.file import openSslEncryptFile, nameFile, nameFileSeeded, getDirTreeDict, addHashesToTree, getDirSize
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
def fullTreeGetFiles(tree, basePath, individualFilesWithHashes, individualFilesWithPaths):
    for item in tree['contents']:
        if item['type'] == "directory" and item['contents'] != None:
            fullTreeGetFiles(item, basePath+"/"+item['name'], individualFilesWithHashes, individualFilesWithPaths) 
        else:
            h = {}
            f = {}
            # temp = ""
            # temp = nameFileSeeded(basePath+"/"+item['name'])+".enc"
            # h['nameFake'] = temp
            # f['nameFake'] = temp
            # item['nameFake'] = temp
            h['hash'] = item['hash']
            h['nameFake'] = item['nameFake']
            individualFilesWithHashes.append(h)
            f['fullPath'] = basePath+"/"+item['name']
            f['nameFake'] = item['nameFake']
            individualFilesWithPaths.append(f)
              
# recursive tree read
def encryptFullTree(tree, basePath, tempPlace, token, individualFilesWithHashes, individualFilesWithPaths):
    for item in tree['contents']:
        if item['type'] == "directory" and item['contents'] != None:
            encryptFullTree(item, basePath+"/"+item['name'], tempPlace, token, individualFilesWithHashes, individualFilesWithPaths) 
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
            individualFilesWithPaths.append(f)

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
    individualFilesWithPaths = []
    if task['task']['type'] == 'directory':
        #directory
        log("Base path " + task['task']['name'])
        encryptFullTree(task['task'], task['task']['name'], tempPlace, token, individualFilesWithHashes, individualFilesWithPaths)
    else:
        raise Exception("Only dir allowed")
    
    if tempPlace is None:
        tempPlace = task['task']['name']
        
    task['task']['individualFilesWithHashes'] = individualFilesWithHashes
    task['task']['individualFilesWithPaths'] = individualFilesWithPaths
    
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
        
        #build object to send the remote
        sendObj = {}
        sendObj['syncSize'] = sync['task']['syncSize']
        sendObj['name'] = sync['task']['syncFakeName']
        sendObj['metadataFileName'] = sync['task']['metadataFileName']
        sendObj['individualFilesWithHashes'] = sync['task']['individualFilesWithHashes']
        
        # trigger the sync on the remote
        # and post the files it will need to download
        addr = determineAddress(sync['address'], sync['port'])
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
        individualFilesWithPaths = []
        if metadata['type'] == 'directory':
            #directory
            log("Base path " + metadata['name'])
            fullTreeGetFiles(tree, tree['name'], individualFilesWithHashes, individualFilesWithPaths)
        else:
            raise Exception("Only dir supported")

        # first check for new files or updated files
        individualFilesWithHashesChanges = []
        outCount = 0
        for file in individualFilesWithHashes:
            inCount = 0
            origFileExists = False
            for origFile in metadata['individualFilesWithHashes']:
                if individualFilesWithPaths[outCount]['fullPath'] == metadata['individualFilesWithPaths'][inCount]['fullPath']:
                    individualFilesWithPaths[outCount]['nameFake'] = metadata['individualFilesWithPaths'][inCount]['nameFake']
                    file['nameFake'] = origFile['nameFake']
                    
                    origFileExists = True
                    # log(file['hash'] + " " + origFile['hash'] + individualFilesWithPaths[outCount]['fullPath'])
                    # same old file now compare hashes
                    if file['hash'] != origFile['hash']:
                        # file was changed
                        file['status'] = "Update"
                        log(f"File {individualFilesWithPaths[outCount]} was changed. Will trigger a re sync")
                        individualFilesWithHashesChanges.append(file)
                        
                        # encrypt the file
                        err = openSslEncryptFile(token, individualFilesWithPaths[outCount]['fullPath'], tempPlace+"/"+file['nameFake'])
                        if err:
                            raise(Exception(err))
                    break
                inCount = inCount + 1
            if not origFileExists:
                # this file is new
                # file['nameFake'] = individualFilesWithPaths[outCount]['nameFake']
                log(f"File {individualFilesWithPaths[outCount]} is new. Will trigger it to sync")
                # add the other metadata
                file['status'] = "New"
                individualFilesWithHashesChanges.append(file)
                # encrypt the file
                err = openSslEncryptFile(token, individualFilesWithPaths[outCount]['fullPath'], tempPlace+"/"+file['nameFake'])
                if err:
                    print("err got here")
                    print(err)
                    raise(Exception(err))
            outCount = outCount + 1
            
        filesToMarkDelete = []
        notifyDeleteObj = []
        # next check for any deleted files
        for count, origFile in enumerate(metadata['individualFilesWithPaths']):
            fileStillExists = False
            for file in individualFilesWithPaths:
                if file['fullPath'] == origFile['fullPath']:
                    fileStillExists = True
                    break
            if not fileStillExists:
                # file was deleted but in case something went wrong will notify the user before remote deletion happens
                log(f"File {origFile['fullPath']} was deleted locally. Will log the file to be deleted on remote after confirmation")
                filesToMarkDelete.append(origFile)
                notifyDeleteObj.append(origFile['nameFake'])
                    
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
            
            # update the metadata...
            # props to update: contents, individualFilesWithPaths, individualFilesWithHashes
            metadata['contents'] = tree['contents']
            metadata['individualFilesWithPaths'] = individualFilesWithPaths
            metadata['individualFilesWithHashes'] = individualFilesWithHashes
            metadata['fileChanges'] = individualFilesWithHashesChanges
            metadata['pendingDeletes'] = notifyDeleteObj
            metadata['syncSize'] = newSize
            
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
            
            if len(filesToMarkDelete) > 0:
                # handle the deleted 
                cur = conn.cursor()
                log("insert pending deleted " + str(filesToMarkDelete))
                cur.execute(f"insert into pending_file_deletes (remote, sync, metadata) values('{metadata['remote']}', '{metadata['syncFakeName']}', '{json.dumps(filesToMarkDelete)}')")
                cur.close()
            
            if len(individualFilesWithHashesChanges) > 0:
                cur = conn.cursor()
                log("insert sync updates task " + str(metadata))
                cur.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Sync update', '{json.dumps(metadata)}', NOW(), 'Queued', 0, NOW())")
                cur.close()
                
            conn.commit()
        else:
            log(f"No changed files for sync {sync}")

    # complete task
    updateTaskStatus(conn, 'Complete', task)
    
def syncUpdate(conn, task):
    log("In sync update")
    
    cur = conn.cursor()
    cur.execute(f"select * from my_remotes where nickname = '{task['task']['remote']}'")
    remote = cur.fetchone()
    cur.close()
    
    metadata = task['task']
    
    # build object to send the remote
    sendObj = {}
    sendObj['syncSize'] = metadata['syncSize']
    sendObj['name'] = metadata['syncFakeName']
    sendObj['metadataFileName'] = metadata['metadataFileName']
    sendObj['fileChanges'] = metadata['fileChanges']
    sendObj['pendingDeletes'] = metadata['pendingDeletes']
    
    # trigger the sync on the remote
    # and post the files it will need to download
    addr = determineAddress(remote['address'], remote['port'])
    params = {'name': metadata['remote'], 'token': remote['token']}
    try:
        req = requests.post(f'{addr}/file/syncUpdateFromRemote', params=params, json=sendObj)
        if req.status_code != 200:
            raise Exception
    except:
        log(f"Remote {metadata['remote']} not up or not accepting syncs currently. Will try again later.")
        raise Exception
    
    updateTaskStatus(conn, 'Syncing', task)
    
    
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
    
    oldFileSize = None
    if 'status' in task['task'] and task['task']['status'] == 'Update':
        # print(f"{task['task']['filePath']}/{task['task']['nameFake']}")
        oldFileSize = getDirSize(f"{task['task']['filePath']}/{task['task']['nameFake']}")
        # print(oldFileSize)
        oldFileSize = int(oldFileSize)
    
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
    
    if oldFileSize:
        actualSize = actualSize - oldFileSize
    
    # increase db space taken and reduce available
    with conn.cursor() as cur:
        cur.execute(f"""update hosted_remotes 
                        set used_space_in_mb = used_space_in_mb + '{sizeNeeded}', 
                        remaining_space_in_mb = remaining_space_in_mb - '{sizeNeeded}' 
                        where nickname = '{task['task']['remote']}'""")
   
    with conn.cursor() as cur:
        cur.execute(f"""update hosted_syncs 
                        set size = size + {sizeNeeded}
                        where name = '{task['task']['syncName']}'""")
    
    conn.commit()
    
    if task['task']['index'] == task['task']['numInSync']:
        log("last file in sync reached")
        # mark the sync as synced
        
        with conn.cursor() as cur:
            cur.execute(f"""update hosted_syncs 
                            set status = 'Synced'
                            where name = '{task['task']['syncName']}'""")
        conn.commit()
        usedSpace = 0
        with conn.cursor() as cur:
            cur.execute(f"""select size from hosted_syncs where name = '{task['task']['syncName']}'""")
            usedSpace = int(cur.fetchone()['size'])
        isUpdate = False
        if 'status' in task['task']:
            isUpdate = True
        # notify the remote that the sync is completely retrieved and tell it what the new size is
        params = {'name': task['task']['remote'], 'token': task['task']['token'], 
                    'syncName': task['task']['syncName'], 'isUpdate': isUpdate, 'usedSpace': usedSpace}
        req = requests.get(f"{addr}/file/notifySyncComplete", params=params)
        if req.status_code != 200:
            log(f"Remote {task['task']['remote']} not up or not accepting syncs currently. Will try again later. The actual error code was: {req.status_code}")
            raise Exception
    
    updateTaskStatus(conn, 'Complete', task)
    
# def determineSyncSizeForPendingDeletes(conn, task):
    
#     syncSizeObj = task['task']
#     sizeOfDels = 0
#     for delObj in syncSizeObj:
#         size = getDirSize(f"{syncSizeObj['filePath']}/{delObj}")
#         sizeOfDels = sizeOfDels + size
    
#     log(f"Size of all the pending deletes in sync {syncSizeObj['syncName']}")
#     data = {}
#     data['sizeofDels'] = sizeOfDels
    
#     cur = conn.cursor()
#     cur.execute(f"update taskqueue set status = '{status}', task = '{json.dumps(data)}' where id = {task['id']}")
#     conn.commit()
#     cur.close()
        
# delete temp files and complete the task
def cleanupNewSync(task, tempPlace, conn):
    if tempPlace is None:
        tempPlace = task['task']['name']
        
    if task['name'] == "Add sync path":
        for file in task['task']['individualFilesWithPaths']:
            fakeName = file['nameFake']
            if os.path.exists(f"{tempPlace}/{fakeName}"):
                log(f"Removing temporary file: {tempPlace}/{fakeName}")
                os.remove(f"{tempPlace}/{fakeName}")
            else:
                log(f"Temp file not found {tempPlace}/{fakeName}")
    else:
        for file in task['task']['fileChanges']:
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