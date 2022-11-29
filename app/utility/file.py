from genericpath import isdir
import os
from pathlib import Path
import random
import string
import aiofiles
from fastapi import APIRouter, Depends, Header, HTTPException, status, Request
from utility.auth import verifyJwt, verifyJwtOrLocal
from utility.database import database
from fastapi.concurrency import run_in_threadpool
import subprocess
import json
from model.pydantics import Token

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
    print(cmd)
    return subprocess.run(cmd, capture_output=True, text=True).stderr

def openSslDecryptFile(token, inputFilePath, outPutFilePath):
    cmd = f"openssl aes-256-cbc -d -pbkdf2 -in {inputFilePath} -out {outPutFilePath} -pass pass:{token}".split(" ")
    subprocess.run(cmd, capture_output=True, text=True)
    
# do this multiprocessed? 
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
            
@router.post("/addNewSync")
async def newSyncPath(filePath: str, token: Token, request: Request):
    # check dir size with du -sh /path/to/dir
    # compare to leftover disk size allocated 
    # need to add remotes feature
    #await verifyJwt(token.access_token)
    await verifyJwtOrLocal(token.access_token, request)
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
    await database.execute(f"insert into taskqueue (name, task, ts, status, try) values('Add sync path', '{json.dumps(tree)}', NOW(), 'Queued', 0)")
        
    return tree

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

@router.get("/test")
async def readTestDir1():
    async with aiofiles.open('../testDir/test.txt', mode='r') as f:
        contents = await f.read()
        return contents
