from genericpath import isdir
import os
from pathlib import Path
import random
import string
import aiofiles
from fastapi import APIRouter, Depends, Header, HTTPException, status, Request
from fastapi.responses import FileResponse, StreamingResponse
from numpy import floor
from utility.auth import verifyJwt, verifyJwtOrLocal, verifyRemote
from utility.database import database
from fastapi.concurrency import run_in_threadpool
import subprocess
import json
from model.pydantics import SyncUpdateData, Token, SyncData
from datetime import datetime, time, timedelta
from logs.asynccustomlogger import Asynclog

CHUNK_SIZE = 1024 * 1024 # 1MB

KNOWN_TYPES = ["jpg", "jpeg", "png", "txt", "mp4", "mp3", "csv", "m4a", "mkv", "sh", "py"]

router = APIRouter(
    prefix="/file",
    tags=["file"],
)

log = Asynclog("file")

# openssl enc -aes-256-cbc -pbkdf2 -in sample.txt -out sample.txt.enc -pass pass:test123
# openssl aes-256-cbc -d -pbkdf2 -in sample.txt.enc -out sample_decrypted.txt -pass pass:test123
# openssl md5 {filepath}  
# tree -J /home/jeremiah/development/nas-share2.0


def openSslEncryptFile(token, inputFilePath, outPutFilePath):
    #cmd = f"openssl enc -aes-256-cbc -pbkdf2 -in {inputFilePath} -out {outPutFilePath} -pass pass:{token}".split(" ")
    cmd = ["openssl", "enc", "-aes-128-cbc", "-engine", "aesni", "-pbkdf2", "-in", inputFilePath, "-out", outPutFilePath, "-pass", f"pass:{token.replace('$', '')}"]
    return subprocess.run(cmd, capture_output=True, text=True).stderr

def openSslDecryptFile(token, inputFilePath, outPutFilePath):
    cmd = ["openssl", "-aes-128-cbc", "-d", "-engine", "aesni", "-in", inputFilePath, "-out", outPutFilePath, "-pass", f"pass:{token.replace('$', '')}"]

    # cmd = f"openssl aes-128-cbc -d -pbkdf2 -engine aesni -in {inputFilePath} -out {outPutFilePath} -pass pass:{token}".split(" ")
    return subprocess.run(cmd, capture_output=True, text=True).stderr
    
def getDirSize(inputFilePath):
    '''outputs in kilobytes'''
    cmd = ["du", "-sm", f'{inputFilePath}']
    return int(subprocess.run(cmd, capture_output=True, text=True).stdout.split('\t')[0]) * 1000 # convert to KB
    
@router.get("/thetest")
def computeMd5FileHash(inputFilePath):
    return subprocess.check_output(args=['md5sum',inputFilePath]).decode().split(' ')[0].strip()

def getDirTreeDict(inputDirPath):
    tree = json.loads(subprocess.check_output(args=['tree', '-J', f'{inputDirPath}', '-L', '10']).decode())[0]
    return tree

def nameFile():
    return ''.join(random.choice(random.choice(string.ascii_letters)) for i in range(20))

def nameFileSeeded(seed):
    random.seed(seed)
    return ''.join(random.choice(string.ascii_letters) for i in range(20))

def detFileType(name):
    try:
        t = name.rsplit(".", 1)[1]
        if t in KNOWN_TYPES:
            return t
        else:
            return None
    except:
        return None

# might take some time
def addHashesToTree(tree, basePath, new: bool):
    try:
        for item in tree['contents']:
            if new:
                item['nameFake'] = nameFileSeeded(basePath+"/"+item['name'])+".enc"
            if item['type'] == "directory" and item['contents'] != None:
                addHashesToTree(item, basePath+"/"+item['name'], new) 
            else:
                item['hash'] = computeMd5FileHash(basePath+"/"+item['name'])
                item['type']= detFileType(item['name'])
    except Exception:
        log.error(f"Exception building tree: {tree} probably too deep")
      
async def getSyncFileFromFakeName(name, fakeName):
    remoteWithSyncs = await database.fetch_all(f"""select r.address, r.token, s.*
                        from my_remotes r
                        inner join lateral 
                        (
                            select task::json ->> 'remote' as nickname,
                            task::json ->> 'individualFilesWithPaths' as individualFilesWithPaths,
                            task::json ->> 'metadataFilePath' as metadatafilepath,
                            task::json ->> 'metadataFileName' as metadatafilename,
                            t2.* 
                            from taskqueue t2
                            where name = 'Add sync path'
                        ) as s on s.nickname = r.nickname
                        where r.nickname = '{name}'
                        and s.try < 4""")
    properties = await database.fetch_all(f"""select *
                        from properties p""")
    try:
        p = json.loads(properties[0]['props'])
        if (p['tempPath'] == None):
            raise Exception()
        filePath = p['tempPath'] + "/" +fakeName
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=e,
            headers={"WWW-Authenticate": "Bearer"},
        )
    found = None
    for sync in remoteWithSyncs:
        # need to find the one that has the right file
        for file in json.loads(sync['individualfileswithpaths']):
            if file['nameFake'] == fakeName:
                found = "f"
                break
        if found:
            break
        # see if it was the metadata file..
        #metadata.bOPaYCcTrfqtRGn.enc
        #
        # await run_in_threadpool(lambda: print(sync['metadatafilename'] + fakeName))
        if sync['metadatafilename'] == fakeName:
            filePath = sync['metadatafilepath']
            break
        
    if filePath is None:
        # log(f"{task['task']['nameFake']} not found or a thing went wrong...")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="file path not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    log("the path found: " + filePath)
  
    return filePath
        
async def runIntervalCheck():
    props = await database.fetch_one(f"select props from properties")
    props = json.loads(props['props'])
    
    syncIntervalStart = time(props['syncIntervalStart']['hour'], props['syncIntervalStart']['minute'])
    syncIntervalStop = time(props['syncIntervalStop']['hour'], props['syncIntervalStop']['minute'])
    now = datetime.now()
    if syncIntervalStart < now.time() < syncIntervalStop:
        return True # able to sync now
    
    # not able to sync yet so tell how many minutes until sync is open
    # hacky doing it this way because we have to say interval can only start in the same day as it ends
    
    # spaghetti
    dt = now
    if now.hour <= 23 and now.hour > props['syncIntervalStart']['hour']:
        dt = now + timedelta(days=1)
    else:
        dt = now
    dt = datetime(dt.year, dt.month, dt.day, props['syncIntervalStart']['hour'], (props['syncIntervalStart']['minute'] + 1), 0 , 0)
    
    timeDiff = dt - now
    
    return floor(timeDiff.total_seconds() / 60)
        
@router.post("/addNewSync")
async def newSyncPath(filePath: str, token: Token, remoteName: str, request: Request):
    # await verifyJwt(token.access_token)
    if not await verifyJwtOrLocal(token.access_token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    # check dir size with du -sh /path/to/dir
    # units in mb
    sizeNeeded = await run_in_threadpool(lambda: getDirSize(filePath))

    r = await database.fetch_one(f"select remaining_space_in_kb as space from my_remotes where nickname = '{remoteName}'")
    if not r:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid remote name {remoteName}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    amountRemaining = int(r['space']) - 10 # 10mb buffer
    # return {"a": sizeNeeded, "b": amountRemaining}
    # return amountRemaining >= sizeNeeded
    if amountRemaining <= sizeNeeded:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Not enough space on {remoteName}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    tree = None
    # read path
    try:
        tree = await run_in_threadpool(lambda: getDirTreeDict(filePath))
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invalid file or directory"
        )
        
    if tree['contents'] == None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must be a directory not a file"
        )
    
    addHashesToTree(tree, tree['name'], True)
    tree['remote'] = remoteName
    tree['syncSize'] = sizeNeeded
    await database.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Add sync path', '{json.dumps(tree)}', NOW(), 'Queued', 0, NOW())")
        
    return tree


# check sync interval from properties table
@router.get("/checkIfInInterval")
async def checkInterval(name: str, token: str, request: Request):
    if not await verifyRemote(name, token, request) and not await verifyJwtOrLocal(None, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )   
    i = await runIntervalCheck()
    if i != True:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=i,
            headers={"WWW-Authenticate": "Bearer"},
        )
    return i

@router.get("/testd")
async def t():
    return await run_in_threadpool(lambda: getDirSize('"/media/drives/Media-1/TV Shows"'))
     
@router.post("/syncFromRemote")
async def syncFromRemote(name: str, token: str, sync: SyncData, request: Request):
    if not await verifyRemote(name, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    i = await runIntervalCheck()
    if i != True:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=i,
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    # verify they have enough space based on what they claim... 
    # (actual validation will have to be done using each file)
    # units in KB
    sizeNeeded = sync.syncSize

    r = await database.fetch_one(f"select remaining_space_in_kb as space from hosted_remotes where nickname = '{name}'")
    if not r:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid remote name {name}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    amountRemaining = int(r['space']) + 10 # 10mb buffer
    
    if amountRemaining <= sizeNeeded:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Not enough space on {name}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # task runner is the downloader
    # api is the uploader
    
    # put the sync in db
    f = await database.fetch_one(f"select filepath, address, port from hosted_remotes where nickname = '{name}' and token = '{token}'")
    
    if not f:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"hosted_sync not found??",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    await database.execute(f"insert into hosted_syncs (name, size, files, metadata_file_name, status) values ('{sync.name}', 0, '{json.dumps(sync.individualFilesWithHashes)}', '{sync.metadataFileName}', 'Pending Sync')")
    
    sync.individualFilesWithHashes.append({"hash": "notAhash", "nameFake": sync.metadataFileName}) # also add the metadata file
    
    # create tasks for all the files for the taskrunner to process
    
    for i, file in enumerate(sync.individualFilesWithHashes):
        taskObj = {}
        taskObj['hash'] = file['hash']
        taskObj['nameFake'] = file['nameFake']
        taskObj['remote'] = name
        taskObj['token'] = token
        taskObj['filePath'] = f['filepath']
        taskObj['address'] = f['address']
        taskObj['port'] = f['port']
        taskObj['index'] = i
        taskObj['numInSync'] = len(sync.individualFilesWithHashes) - 1
        taskObj['syncName'] = sync.name

        await database.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Retrieve file from remote', '{json.dumps(taskObj)}', NOW(), 'Queued', 0, NOW())")

    return json.dumps(sync.individualFilesWithHashes)

@router.post("/syncUpdateFromRemote")
async def syncUpdateFromRemote(name: str, token: str, sync: SyncUpdateData, request: Request):
    if not await verifyRemote(name, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    i = await runIntervalCheck()
    if i != True:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=i,
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # task runner is the downloader
    # api is the uploader
    
    f = await database.fetch_one(f"select filepath, address, port from hosted_remotes where nickname = '{name}' and token = '{token}'")
    
    if not f:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"remote not found for this sync",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    await database.execute(f"update hosted_syncs set status = 'Pending Sync' where name = '{sync.name}'")
    
    sync.fileChanges.append({"hash": "notAhash", "nameFake": sync.metadataFileName, "status": "Update"})# also add the metadata file
    
    # # create a task that will calculate the sync size based on changes
    # syncSizeObj = {}
    # syncSizeObj['pendingDeletes'] = sync.pendingDeletes
    # syncSizeObj['remote'] = name
    # syncSizeObj['token'] = token
    # syncSizeObj['filePath'] = f['filepath']
    # syncSizeObj['address'] = f['address']
    # syncSizeObj['port'] = f['port']
    # syncSizeObj['syncName'] = sync.name
    # syncSizeObj['usedSpace'] = f['used_space_in_kb']
    # await database.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Determine pending deletes size', '{json.dumps(syncSizeObj)}', NOW(), 'Queued', 0, NOW())")

    if len(sync.pendingDeletes) > 0:
        delObj = {'pendingDeletes': sync.pendingDeletes, 'remoteName': name, 'syncName': sync.name, 'filePath': f['filepath']}
        await database.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Delete sync files', '{json.dumps(delObj)}', NOW(), 'Queued', 0, NOW())")
    

    # create tasks for all the files for the taskrunner to process
    
    for i, file in enumerate(sync.fileChanges):
        await run_in_threadpool(lambda: print(file))
        taskObj = {}
        taskObj['hash'] = file['hash']
        taskObj['nameFake'] = file['nameFake']
        taskObj['status'] = file['status'] # status = New, Update
        taskObj['remote'] = name
        taskObj['token'] = token
        taskObj['filePath'] = f['filepath']
        taskObj['address'] = f['address']
        taskObj['port'] = f['port']
        taskObj['index'] = i
        taskObj['numInSync'] = len(sync.fileChanges) - 1
        taskObj['syncName'] = sync.name

        # does this work without await??
        await database.execute(f"insert into taskqueue (name, task, ts, status, try, retry_ts) values('Retrieve file from remote', '{json.dumps(taskObj)}', NOW(), 'Queued', 0, NOW())")

    
    return "OK"


# @router.get("/testDirThing")
# async def test():
#     tree = await run_in_threadpool(lambda: getDirTreeDict("/home/jeremiah/development/nas-share2.0"))
#     await run_in_threadpool(lambda: addHashesToTree(tree, tree['name']))
#     #await database.execute(f"insert into documents (data) values('{json.dumps(tree)}')")

#     return tree

# @router.get("/encryptionTest")
# async def encryptionTest():
#     token = await database.fetch_one(f"select password from authenticated_user where username = 'Jeremiah'")
#     token = token.password
    # 
#     inputFilePath = "/home/jeremiah/development/nas-share2.0/samplee.txt"
#     outPutFilePath = "/home/jeremiah/development/nas-share2.0/sample.txt.enc"
    # 
#     fileHash = await run_in_threadpool(lambda: computeMd5FileHash(inputFilePath))
#     #await run_in_threadpool(lambda: print(fileHash))
    # 
#     await run_in_threadpool(lambda: openSslEncryptFile(token, inputFilePath, outPutFilePath))
#     inputFilePath = "/home/jeremiah/development/nas-share2.0/sample.txt.enc"
#     outPutFilePath = "/home/jeremiah/development/nas-share2.0/sample_decrypted.mp4"
#     await run_in_threadpool(lambda: openSslDecryptFile(token, inputFilePath, outPutFilePath))
    # return fileHash

@router.get("/getFileSize")
async def checkFileSize(name: str, token: str, fakeName: str, request: Request):
    if not await verifyRemote(name, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    filePath = await getSyncFileFromFakeName(name, fakeName)
    return await run_in_threadpool(lambda: os.path.getsize(filePath) * 0.001)

@router.get("/getPendingDeletes")
async def getPendingDeletes(token: str, request: Request):
    # change to just jwt later
    if not await verifyJwtOrLocal(token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # query db for pending del files
    pendingDels = await database.fetch_all(f"select * from pending_file_deletes where confirmed is null")
    
    return pendingDels

@router.get("/acceptDelete")
async def acceptDeleteFromUser(token: str, deleteId: int, request: Request):
    # change to just jwt later
    if not await verifyJwtOrLocal(token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # insert task for the pending delete
    # things needed:
    # namefakes for all del files (metadata)
    # remote name
    # sync name
    # await database.execute(f"""insert into taskqueue 
    #                         (name, task, ts, status, try, retry_ts) 
    #                         values('Sync delete', '{json.dumps(pendingDel)}', NOW(), 'Queued', 0, NOW())""")

    # update the pending del
    await database.execute(f"""update pending_file_deletes set confirmed = 'Y' where id = {deleteId}""")

@router.get("/notifySyncComplete")
async def notifySyncComplete(name: str, token: str, syncName: str, isUpdate: bool, usedSpace: int, request: Request):
    if not await verifyRemote(name, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    
    await database.execute(f"""update taskqueue t
                                set status = 'Cleaning up'
                                where id in (
                                    select s.id
                                    from (
                                        select task::json ->> 'syncFakeName' as syncFakeName,
                                        task::json ->> 'individualFilesWithPaths' as individualFilesWithPaths,
                                        task::json ->> 'metadataFilePath' as metadatafilepath,
                                        task::json ->> 'metadataFileName' as metadatafilename,
                                        t2.* 
                                        from taskqueue t2
                                        where t2.status = 'Syncing'
                                        and (t2.name = 'Add sync path' or t2.name = 'Sync update')
                                    ) as s 
                                    where s.syncFakeName = '{syncName}'
                                )""")
    
    await database.execute(f"""update my_remotes
                                set used_space_in_kb = used_space_in_kb + '{usedSpace}', 
                                remaining_space_in_kb = remaining_space_in_kb - '{usedSpace}' 
                                where nickname = '{name}'""")
    
    if isUpdate:
        # need to complete the original sync task & insert into sync table & delete temp files
        task = await database.fetch_one(f"""
                                        select s.syncpath, s.syncfakename, s.syncsize, s.task
                                        from (
                                            select task::json ->> 'syncFakeName' as syncFakeName,
                                            task::json ->> 'syncSize' as syncSize,
                                            task::json ->> 'name' as syncPath,
                                            task::json ->> 'metadataFilePath' as metadatafilepath,
                                            task::json ->> 'metadataFileName' as metadatafilename,
                                            t2.* 
                                            from taskqueue t2
                                            where t2.name = 'Sync update'
                                            and t2.status <> 'Complete'
                                        ) as s 
                                        where s.syncFakeName = '{syncName}'""")
        await database.execute(f"""update my_syncs
                                set size = {usedSpace}, metadata = '{task['task']}'
                                where name = '{syncName}'""")
    else:
        # need to complete the original sync task & insert into sync table & delete temp files
        task = await database.fetch_one(f"""
                                        select s.syncpath, s.syncfakename, s.syncsize, s.task
                                        from (
                                            select task::json ->> 'syncFakeName' as syncFakeName,
                                            task::json ->> 'syncSize' as syncSize,
                                            task::json ->> 'name' as syncPath,
                                            task::json ->> 'metadataFilePath' as metadatafilepath,
                                            task::json ->> 'metadataFileName' as metadatafilename,
                                            t2.* 
                                            from taskqueue t2
                                            where t2.name = 'Add sync path'
                                            and t2.status <> 'Complete'
                                        ) as s 
                                        where s.syncFakeName = '{syncName}'""")
        await database.execute(f"""insert into my_syncs
                                (name, size, metadata, status, real_path)
                                values('{syncName}', {usedSpace}, '{task['task']}', 'Synced', '{task['syncpath']}')""")

    return task
    
@router.get("/downloadForSync")
async def hostFileDownloadForSync(name: str, token: str, fakeName: str, request: Request):
    if not await verifyRemote(name, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # if not await runIntervalCheck():
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN,
    #         detail="Not allowed to sync at this time",
    #         headers={"WWW-Authenticate": "Bearer"},
    #     )
    
    filePath = await getSyncFileFromFakeName(name, fakeName)    
    
    async def iterfile():
       async with aiofiles.open(filePath, 'rb') as f:
            while chunk := await f.read(CHUNK_SIZE):
                yield chunk

    headers = {'Content-Disposition': f'attachment; filename="{fakeName}"'}
    return StreamingResponse(iterfile(), headers=headers, media_type='application/octet-stream')
