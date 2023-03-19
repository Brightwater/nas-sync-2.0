import asyncio
import datetime
import time
from fastapi import APIRouter, HTTPException, Request, status
from utility.database import database
from utility.auth import verifyJwt, verifyJwtOrLocal, verifyRemote
import json
import aiohttp
from utility.task import determineAddress
from fastapi.concurrency import run_in_threadpool
import psutil
from logs.asynccustomlogger import Asynclog

router = APIRouter(
    prefix="/dashboard",
    tags=["dashboard"],
)
log = Asynclog("Dashboard")

async def getActiveTask():
    return await database.fetch_one(f"""select ts, name, status from taskqueue where status <> 'Complete' order by ts desc limit 1""")

async def getLocalStorageUsage():
    return await database.fetch_one(f"""select sum(used_space_in_kb) as total_local_space_used_in_kb from hosted_remotes hr""")

async def getRemoteStorageUsage():
    return await database.fetch_one(f"""select sum(used_space_in_kb) as total_remote_space_used_in_kb from my_remotes mr""")

async def getsProps():
    props = await database.fetch_one(f"""select props from properties""")
    props = json.loads(props['props'])
    return props
    #['syncIntervalStart']
    
async def timeDiff(startTime):
    start_datetime = datetime.datetime.fromtimestamp(startTime)
    end_datetime = datetime.datetime.fromtimestamp(time.time())
    diff = end_datetime - start_datetime
    days = diff.days
    hours = diff.seconds // 3600
    minutes = (diff.seconds // 60) % 60
    return {'days': days, 'hours': hours, 'minutes': minutes} 

async def getRemotesStatus():
    remotes = await database.fetch_all(f"""select nickname, address, token, port from my_remotes mr""")
    remotesObj = []
    
    for remote in remotes:
        address = determineAddress(remote['address'], remote['port'])
        status = "Down"
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{address}/dashboard/status?token={remote['token']}&remoteName={remote['nickname']}") as resp:
                data = await resp.json()
                if data == "OK":
                    status = "UP"
                rObj = {'remoteName': remote['nickname'], 'status': status}
                remotesObj.append(rObj)
    return remotesObj
                    
@router.get("/dashboardData")
async def getDashboardData(token: str, request: Request):
    # change to just jwt later
    if not await verifyJwtOrLocal(token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    

    activeTask, totalLocalSpaceUsedInKb, totalRemoteSpaceUsedInKb, remoteStatuses, props = await asyncio.gather(getActiveTask(), 
                                                                                         getLocalStorageUsage(), 
                                                                                         getRemoteStorageUsage(),
                                                                                         getRemotesStatus(),
                                                                                         getsProps())
    total,used,free,percent = psutil.disk_usage(props['tempPath'])
    
    return {
                "activeTask": activeTask,
                "totalLocalSpaceUsedInGb": round(float(totalLocalSpaceUsedInKb['total_local_space_used_in_kb']) * .000001, 1),
                "totalRemoteSpaceUsedInGb": round(float(totalRemoteSpaceUsedInKb['total_remote_space_used_in_kb']) * .000001, 1),
                "remotesStatus": remoteStatuses,
                "nextSyncTime": props['syncIntervalStart'],
                "diskSpaceRemaining": round(free * .000000001, 1),
                "diskSpaceTotal": round(total * .000000001, 1),
                "runtime": await timeDiff(props['startup_time'])
           }
    
@router.get("/status")
async def getStatus(token: str, remoteName: str, request: Request):
    # change to just jwt later
    if not await verifyRemote(remoteName, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # could return sync interval?  
    return "OK"
    

    