from math import ceil
from scheduler import log
from utility.file import openSslEncryptFile, nameFile, nameFileSeeded, getDirTreeDict, addHashesToTree, getDirSize
import json
import pathlib
import os
import requests
import shutil
import psycopg

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
            
# def injectFileInContentsRecursive2(folderNameToFind, contents: list, thisStepPath, fileObj):
#     thisFolderName = os.path.basename(thisStepPath)
#     if thisFolderName == folderNameToFind:
#         # found here
#         contents.append(fileObj)
#         return True
#     for nested in contents:
#         if nested['type'] == 'directory':
#             found = injectFileInContentsRecursive2(folderNameToFind, contents, thisStepPath+"/"+nested['name'], fileObj)
#             if found:
#                 return True
#     return False

def delFileInContentsRecursive(relPathSplit, contents: list, fakeName):
    if len(relPathSplit) == 1:
        # final folder found
        for file in contents:
            if file['nameFake'] == fakeName:
                contents.remove(file)
                break
        return
    for element in contents:
        if element['type'] == 'directory' and relPathSplit[1] == element['name']:
            delFileInContentsRecursive(relPathSplit[1:], element['contents'], fakeName)

def injectDelFileInContentsRecursive(relPathSplit, contents: list, fileObj):
    if len(relPathSplit) == 1:
        # final folder found
        log("FOUND NEW FILE LOCATION")
        contents.append(fileObj)
        return
    found = False
    for element in contents:
        if element['type'] == 'directory' and relPathSplit[1] == element['name']:
            injectDelFileInContentsRecursive(relPathSplit[1:], element['contents'], fileObj)
            found = True
            return
    if not found:
        # need to create remainder of path
        dirObj = {'name': relPathSplit[1], 'type': 'directory', 'contents': []}
        contents.append(dirObj)
        injectDelFileInContentsRecursive(relPathSplit[1:], dirObj['contents'], fileObj)
        

def placeDelFileInMetadata(filePath, rootPath: str, contents: list, fileObj):
    relPath = os.path.relpath(filePath, start=rootPath)
    relPathSplit = str(relPath).split("/")[:-1] # remove last element (the file itself) so we can find the folder
    relPathSplit.insert(0, os.path.basename(rootPath)) 
    log("RELPATH " + str(relPathSplit))
    injectDelFileInContentsRecursive(relPathSplit, contents, fileObj)
    
def removeDelFileInMetadata(filePath, rootPath: str, contents: list, fakeName):
    relPath = os.path.relpath(filePath, start=rootPath)
    relPathSplit = str(relPath).split("/")[:-1] # remove last element (the file itself) so we can find the folder
    relPathSplit.insert(0, os.path.basename(rootPath)) 
    log("RELPATH " + str(relPathSplit))
    delFileInContentsRecursive(relPathSplit, contents, fakeName)

def addSyncPathTask(conn, task, tempPlace):
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
    
def syncFilesToRemote(conn: psycopg.connection, task, tempPlace):
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
            if req.status_code == 403:
                # remote is not up currently... delay task until its up
                with conn.cursor() as cur:
                    minutesUntil = int(json.loads(req.text)['detail'])
                    log(f"sleeping task until remote is ready in {minutesUntil} minutes" + str(task))
                    cur.execute(f"update taskqueue set retry_ts = NOW() + {minutesUntil} * interval '1 minute' where id = {task['id']}")
                    conn.commit()
                return
            elif req.status_code != 200:
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
                    
        confirmedDelList = []
        print("GOT")
        with conn.cursor() as cur:
            cur.execute(f"select * from pending_file_deletes where sync = '{metadata['syncFakeName']}'")
            pd = cur.fetchone()
            print("HERE")
            print(pd)
            if pd:
                if pd['confirmed'] == 'Y':
                    print(pd['metadata'])
                    confirmedDelList = pd['metadata']
                else:
                    filesToMarkDelete.append(pd['metadata'])             
                         
        if len(individualFilesWithHashesChanges) > 0 or len(filesToMarkDelete) > 0 or len(confirmedDelList) > 0:
            
            # calulate the size difference (already including deletes technically)
            newSize = int(getDirSize(metadata['name']))
            oldSize = int(metadata['syncSize'])
            if newSize > oldSize:
                newSize = newSize - oldSize
            else:
                newSize = oldSize - newSize
            
            cur = conn.cursor()
            cur.execute(f"select * from my_remotes where nickname = '{metadata['remote']}'")
            remote = cur.fetchall()
            cur.close()
            remainingSize = remote[0]['remaining_space_in_kb']
            log("Space remaining on remote " + str(remainingSize) + " KB")
            amountRemaining = int(remainingSize) - 100 # 10mb buffer
            
            if amountRemaining <= newSize:
                raise Exception("Not enough space on remote for changed files")
            
            for d in filesToMarkDelete:
                # add the delete back to metadata so remote can still track it until delete is finalized
                fileObj = { 'hash': None, 
                            'name': os.path.basename(d['fullPath']),
                            'type': None,
                            'markedDelete': True,
                            'nameFake': d['nameFake']}
                placeDelFileInMetadata(d['fullPath'], tree['name'], tree['contents'], fileObj)
                
            # update the metadata...
            # props to update: contents, individualFilesWithPaths, individualFilesWithHashes
            metadata['contents'] = tree['contents']
            metadata['individualFilesWithPaths'] = individualFilesWithPaths
            metadata['individualFilesWithHashes'] = individualFilesWithHashes
            metadata['fileChanges'] = individualFilesWithHashesChanges
            if len(confirmedDelList) > 0:
                metadata['pendingDeletes'] = confirmedDelList
            else:
                metadata['pendingDeletes'] = None
            metadata['syncSize'] = newSize
            
            if len(filesToMarkDelete) > 0:
                # handle the new deleted 
                log("insert pending deleted " + str(filesToMarkDelete))
                try: # delete old deletes for this sync
                    with conn.cursor() as cur:
                        cur.execute(f"delete from pending_file_deletes where sync = '{metadata['syncFakeName']}'")
                except:
                    pass
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"insert into pending_file_deletes (remote, sync, metadata) values('{metadata['remote']}', '{metadata['syncFakeName']}', '{json.dumps(filesToMarkDelete)}')")
                except:
                    pass
                # if nothing else was changed besides deletes,
                # go ahead and just update metadata for when 
                # we run sync update again
                if len(individualFilesWithHashesChanges) == 0 and len(confirmedDelList) == 0:
                    with conn.cursor() as cur:
                        cur.execute(f"""update my_syncs
                                        set metadata = '{json.dumps(metadata)}'
                                        where name = '{metadata['syncFakeName']}'""")
            
            
            if len(individualFilesWithHashesChanges) > 0 or len(confirmedDelList) > 0:
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
                
                with conn.cursor() as cur:
                    log("insert sync updates task " + str(metadata['name']))
                    cur.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Sync update', '{json.dumps(metadata)}', NOW(), 'Queued', 0, NOW())")
                
            conn.commit()
        else:
            log(f"No changed files for sync {sync}")

    # complete task
    updateTaskStatus(conn, 'Complete', task)
    
def syncUpdate(conn, task):
    log("In sync update")
    
    with conn.cursor() as cur:
        cur.execute(f"select * from my_remotes where nickname = '{task['task']['remote']}'")
        remote = cur.fetchone()
    
    metadata = task['task']
    
    # build object to send the remote
    sendObj = {}
    sendObj['syncSize'] = metadata['syncSize']
    sendObj['name'] = metadata['syncFakeName']
    sendObj['metadataFileName'] = metadata['metadataFileName']
    sendObj['fileChanges'] = metadata['fileChanges']
    delList = []
    for d in metadata['pendingDeletes']:
        delList.append(d['nameFake'])
    sendObj['pendingDeletes'] = delList
    
    # trigger the sync on the remote
    # and post the files it will need to download
    addr = determineAddress(remote['address'], remote['port'])
    params = {'name': metadata['remote'], 'token': remote['token']}
    try:
        req = requests.post(f'{addr}/file/syncUpdateFromRemote', params=params, json=sendObj)
        if req.status_code == 403:
            # remote is not up currently... delay task until its up
            with conn.cursor() as cur:
                minutesUntil = int(json.loads(req.text)['detail'])
                log(f"sleeping task until remote is ready in {minutesUntil} minutes" + str(task))
                cur.execute(f"update taskqueue set retry_ts = NOW() + {minutesUntil} * interval '1 minute' where id = {task['id']}")
                conn.commit()
            return
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
    cur.execute(f"select remaining_space_in_kb from hosted_remotes where nickname = '{task['task']['remote']}'")
    remainingSpace = cur.fetchone()
    cur.close()
    remainingSpace = remainingSpace['remaining_space_in_kb']
    
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
    diskTotal = diskStat.total*0.001
    diskFree = diskStat.free*0.001
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
    actualSize = os.path.getsize(f"{task['task']['filePath']}/{task['task']['nameFake']}") * 0.001
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
                        set used_space_in_kb = used_space_in_kb + '{sizeNeeded}', 
                        remaining_space_in_kb = remaining_space_in_kb - '{sizeNeeded}' 
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
    
def deleteSyncFiles(conn: psycopg.connection, task):
    metadata = task['task']
    files = []
    # get sync data
    with conn.cursor() as cur:
        cur.execute(f"""select * from hosted_syncs where name = '{metadata['syncName']}'""")
        files = cur.fetchone()
        files = files['files']
        print(files)
    
    for delFile in metadata['pendingDeletes']:
        # get file size on disk.
        actualSize = os.path.getsize(f"{metadata['filePath']}/{delFile}") * 0.001
        
        # delete file from disk
        if os.path.exists(f"{metadata['filePath']}/{delFile}"):
            os.remove(f"{metadata['filePath']}/{delFile}")
            log("Removing the file as remote requested. File fake name: " + delFile)
            
        # remove file from files list
        for file in files:
            if file['nameFake'] == delFile:
                files.remove(file)
                
        # update sync data
        # decreate db space taken and increase available
        with conn.cursor() as cur:
            cur.execute(f"""update hosted_remotes 
                            set used_space_in_kb = used_space_in_kb - '{actualSize}', 
                            remaining_space_in_kb = remaining_space_in_kb + '{actualSize}' 
                            where nickname = '{metadata['remoteName']}'""")
    
        with conn.cursor() as cur:
            cur.execute(f"""update hosted_syncs 
                            set size = size - {actualSize}
                            where name = '{metadata['syncName']}'""")
        conn.commit()
        
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
def cleanupNewSync(conn, task, tempPlace):
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
    
def addSyncPathGetSubTask(task, TEMP_PATH, conn):
    if task['status'] == 'Cleaning up':
        cleanupNewSync(conn, task, TEMP_PATH)
    else:
        addSyncPathTask(conn, task, TEMP_PATH)
        
def syncUpdateGetSubTask(task, TEMP_PATH, conn):
    if task['status'] == 'Cleaning up':
        cleanupNewSync(conn, task, TEMP_PATH)
    else:
        syncUpdate(conn, task)
        
# def triggerFileDelete(conn: psycopg.Connection, task, tempPlace):
    
#     metadata = task['task']
#     with conn.cursor() as cur:
#         cur.execute(f"select * from my_remotes where nickname = '{metadata['remote']}'")
#         remote = cur.fetchone()
    
#     with conn.cursor() as cur:
#         cur.execute(f"select file_encryption_key from authenticated_user where 'admin' = any(scopes)")
#         token = cur.fetchone()
#     if token is None:
#         raise Exception('No admin key found')
#     token = token['file_encryption_key']
    
#     with conn.cursor() as cur:
#         cur.execute(f"select metadata from my_syncs where name = '{metadata['sync']}'")
#         syncmetadata = json.loads(cur.fetchone())   
    
#     sendDelObj = {}
#     delList = []
#     for d in json.loads(metadata['metadata']):
#         # update metadata and remove the file from the thing
#         removeDelFileInMetadata(d['fullPath'], syncmetadata['name'], syncmetadata['contents'], d['nameFake'])
#         delList.append(d['nameFake'])
        
#     dummyMetadataName = syncmetadata['syncFakeName']
#     inp = tempPlace+"/"+"metadata."+dummyMetadataName
#     log("creating metadata json file for sync update")
#     # create metadata file
#     with open(inp+".json", "w") as metadataFile:
#         data = json.dumps(syncmetadata)
#         metadataFile.write(data)
#     # encrypt the metadata file
#     err = openSslEncryptFile(token, inp+".json", inp+".enc")
#     if err:
#         raise(Exception(err))
#     os.remove(inp+".json")
    
#     sendDelObj['delList'] = delList
#     sendDelObj['syncName'] = metadata['sync']
    
    
#     print(json.dumps(sendDelObj))
#     addr = determineAddress(remote['address'], remote['port'])
#     params = {'name': metadata['remote'], 'token': remote['token']}
#     try:
#         req = requests.post(f'{addr}/file/syncUpdateFromRemote', params=params, json=sendDelObj)
#         if req.status_code == 403:
#             # remote is not up currently... delay task until its up
#             with conn.cursor() as cur:
#                 minutesUntil = int(json.loads(req.text)['detail'])
#                 log(f"sleeping task until remote is ready in {minutesUntil} minutes" + str(task))
#                 cur.execute(f"update taskqueue set retry_ts = NOW() + {minutesUntil} * interval '1 minute' where id = {task['id']}")
#                 conn.commit()
#             return
#         if req.status_code != 200:
#             raise Exception
#     except:
#         log(f"Remote {metadata['remote']} not up or not accepting syncs currently. Will try again later.")
#         raise Exception