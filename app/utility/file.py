from genericpath import isdir
import os
from pathlib import Path
import random
import string
import aiofiles
from fastapi import APIRouter, Depends, Header, HTTPException, status, Request
from fastapi.responses import FileResponse, StreamingResponse
from utility.auth import verifyJwt, verifyJwtOrLocal, verifyRemote
from utility.database import database
from fastapi.concurrency import run_in_threadpool
import subprocess
import json
from model.pydantics import Token, SyncData
from datetime import datetime, time

CHUNK_SIZE = 1024 * 1024

KNOWN_TYPES = ["jpg", "jpeg", "png", "txt", "mp4", "mp3", "csv", "m4a", "mkv", "sh", "py"]

router = APIRouter(
    prefix="/file",
    tags=["file"],
)

# openssl enc -aes-256-cbc -pbkdf2 -in sample.txt -out sample.txt.enc -pass pass:test123
# openssl aes-256-cbc -d -pbkdf2 -in sample.txt.enc -out sample_decrypted.txt -pass pass:test123
# openssl md5 {filepath}  
# tree -J /home/jeremiah/development/nas-share2.0



def openSslEncryptFile(token, inputFilePath, outPutFilePath):
    #cmd = f"openssl enc -aes-256-cbc -pbkdf2 -in {inputFilePath} -out {outPutFilePath} -pass pass:{token}".split(" ")
    cmd = ["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-in", inputFilePath, "-out", outPutFilePath, "-pass", f"pass:{token.replace('$', '')}"]
    return subprocess.run(cmd, capture_output=True, text=True).stderr

def openSslDecryptFile(token, inputFilePath, outPutFilePath):
    cmd = f"openssl aes-256-cbc -d -pbkdf2 -in {inputFilePath} -out {outPutFilePath} -pass pass:{token}".split(" ")
    subprocess.run(cmd, capture_output=True, text=True)
    
# outputs in megabytes
def getDirSize(inputFilePath):
    #cmd = du -sh /path/to/dir
    cmd = ["du", "-sm", f'{inputFilePath}']
    return subprocess.run(cmd, capture_output=True, text=True).stdout.split('\t')[0]
    
def computeMd5FileHash(inputFilePath):
    return subprocess.check_output(args=f'md5sum {inputFilePath}', shell=True).decode().split(' ')[0].strip()

def getDirTreeDict(inputDirPath):
    tree = json.loads(subprocess.check_output(args=['tree', '-J', f'{inputDirPath}', '-L', '10']).decode())[0]
    return tree

def nameFile():
    return ''.join(random.choice(random.choice(string.ascii_letters)) for i in range(15))

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
                item['nameFake'] = nameFile()+".enc"
            if item['type'] == "directory" and item['contents'] != None:
                addHashesToTree(item, basePath+"/"+item['name'], new) 
            else:
                item['hash'] = computeMd5FileHash('"'+basePath+"/"+item['name']+'"')
                item['type']= detFileType(item['name'])
    except Exception:
        print(f"Exception building tree: {tree} probably too deep")
      
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
    # return remoteWithSyncs
    filePath = None
    for sync in remoteWithSyncs:
        # need to find the one that has the right file
        for file in json.loads(sync['individualfileswithpaths']):
            if file['nameFake'] == fakeName:
                filePath = file['fullPath']
                break
        if filePath:
            break
        # see if it was the metadata file..
        #metadata.bOPaYCcTrfqtRGn.enc
        #
        await run_in_threadpool(lambda: print(sync['metadatafilename'] + fakeName))
        if sync['metadatafilename'] == fakeName:
            filePath = sync['metadatafilepath']
            break
        
    if filePath is None:
        # log(f"{task['task']['nameFake']} not found or a thing went wrong...")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return filePath
        
async def runIntervalCheck():
    props = await database.fetch_one(f"select props from properties")
    props = props['props']
    props = json.loads(props)
    syncIntervalStart = time(props['syncIntervalStart']['hour'], props['syncIntervalStart']['minute'])
    syncIntervalStop = time(props['syncIntervalStop']['hour'], props['syncIntervalStop']['minute'])
    if syncIntervalStart < datetime.now().time() < syncIntervalStop:
        return True
    return False
        
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
    sizeNeeded = int(await run_in_threadpool(lambda: getDirSize(filePath)))

    r = await database.fetch_one(f"select remaining_space_in_mb as space from my_remotes where nickname = '{remoteName}'")
    if not r:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid remote name {remoteName}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    amountRemaining = int(r['space']) + 10 # 10mb buffer
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
    
    addHashesToTree(tree, tree['name'], True)
    tree['remote'] = 'test'
    tree['syncSize'] = sizeNeeded
    await database.execute(f"insert into taskqueue (name, task, ts, status, try) values('Add sync path', '{json.dumps(tree)}', NOW(), 'Queued', 0)")
        
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
    return await runIntervalCheck()   

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
    
    if not await runIntervalCheck():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not allowed to sync at this time",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    # verify they have enough space based on what they claim... 
    # (actual validation will have to be done using each file)
    # units in mb
    sizeNeeded = sync.syncSize

    r = await database.fetch_one(f"select remaining_space_in_mb as space from hosted_remotes where nickname = '{name}'")
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
        
    await database.execute(f"insert into hosted_syncs (name, size, files, metadata_file_name, status) values ('{sync.name}', {sync.syncSize}, '{json.dumps(sync.individualFilesWithHashes)}', '{sync.metadataFileName}', 'Pending Sync')")
    
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

        await database.execute(f"insert into taskqueue (name, task, ts, status, try) values('Retrive file from remote', '{json.dumps(taskObj)}', NOW(), 'Queued', 5)")

    return json.dumps(sync.individualFilesWithHashes)

@router.get("/testDirThing")
async def test():
    tree = await run_in_threadpool(lambda: getDirTreeDict("/home/jeremiah/development/nas-share2.0"))
    await run_in_threadpool(lambda: addHashesToTree(tree, tree['name']))
    #await database.execute(f"insert into documents (data) values('{json.dumps(tree)}')")

    return tree

@router.get("/encryptionTest")
async def encryptionTest():
    token = await database.fetch_one(f"select password from authenticated_user where username = 'Jeremiah'")
    token = token.password
    
    inputFilePath = "/home/jeremiah/development/nas-share2.0/samplee.txt"
    outPutFilePath = "/home/jeremiah/development/nas-share2.0/sample.txt.enc"
    
    fileHash = await run_in_threadpool(lambda: computeMd5FileHash(inputFilePath))
    #await run_in_threadpool(lambda: print(fileHash))
    
    await run_in_threadpool(lambda: openSslEncryptFile(token, inputFilePath, outPutFilePath))
    inputFilePath = "/home/jeremiah/development/nas-share2.0/sample.txt.enc"
    outPutFilePath = "/home/jeremiah/development/nas-share2.0/sample_decrypted.mp4"
    await run_in_threadpool(lambda: openSslDecryptFile(token, inputFilePath, outPutFilePath))
    return fileHash

@router.get("/getFileSize")
async def checkFileSize(name: str, token: str, fakeName: str, request: Request):
    if not await verifyRemote(name, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    filePath = await getSyncFileFromFakeName(name, fakeName)
    return await run_in_threadpool(lambda: os.path.getsize(filePath) * 0.000001)
    
@router.get("/downloadForSync")
async def hostFileDownloadForSync(name: str, token: str, fakeName: str, request: Request):
    if not await verifyRemote(name, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not await runIntervalCheck():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not allowed to sync at this time",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    filePath = await getSyncFileFromFakeName(name, fakeName)    
    
    async def iterfile():
       async with aiofiles.open(filePath, 'rb') as f:
            while chunk := await f.read(CHUNK_SIZE):
                yield chunk

    headers = {'Content-Disposition': f'attachment; filename="{fakeName}"'}
    return StreamingResponse(iterfile(), headers=headers, media_type='application/octet-stream')
