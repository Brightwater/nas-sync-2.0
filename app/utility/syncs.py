from fastapi import APIRouter, HTTPException, Request, status
from model.pydantics import ServerInfo
from utility.task import determineAddress

from utility.auth import verifyJwtOrLocal, verifyRemote
from utility.database import database

import aiohttp

router = APIRouter(
    prefix="/syncs",
    tags=["syncs"],
)


@router.get("/remotes")
async def getRemotes(token: str, request: Request):
    # change to just jwt later
    if not await verifyJwtOrLocal(token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    ret = await database.fetch_all(f"""select mr.nickname, mr.address, mr.token, mr.port, mr.used_space_in_kb, mr.remaining_space_in_kb, hr.used_space_in_kb as hosted_used_space_in_kb, hr.remaining_space_in_kb as hosted_remaining_space_in_kb 
                                       from my_remotes mr
                                       inner join hosted_remotes hr on hr.nickname = mr.nickname""")
    obj = []
    for r in ret:
        new = {
            'used_space_in_gb': round(float(r['used_space_in_kb']) * .000001, 1),
            'nickname': r['nickname'],
            'address': r['address'],
            'token': r['token'],
            'port': r['port'],
            'remaining_space_in_gb': round(float(r['remaining_space_in_kb']) * .000001, 1),
            'hosted_remaining_space_in_gb': round(float(r['hosted_remaining_space_in_kb']) * .000001, 1),
            'hosted_used_space_in_gb': round(float(r['hosted_used_space_in_kb']) * .000001, 1)
        }
        # r['used_space_in_kb'] = round(float(r['used_space_in_kb']) * .000001, 1)
        # r['remaining_space_in_kb'] = round(float(r['remaining_space_in_kb']) * .000001, 1)
        obj.append(new)

    return obj

@router.post("/updateRemoteSettings")
async def updateRemoteSettings(token: str, si: ServerInfo, request: Request):
    # change to just jwt later
    if not await verifyJwtOrLocal(token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    newHostedRemainingSpaceInKb = si.hosted_remaining_space_in_gb * 1000000
    
    ret = await database.fetch_all(f"""select remaining_space_in_kb, token, address, port
                                       from hosted_remotes mr
                                       where nickname = '{si.nickname}'""")
    oldHostedRemainingSpaceInGb = round(float(ret[0]['remaining_space_in_kb']) * .000001, 1)
 
    if oldHostedRemainingSpaceInGb != si.hosted_remaining_space_in_gb:
        # notify the remote of the new amount of space they can use
        address = determineAddress(si.address, si.port)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{address}/syncs/manualUpdateStorageAvailable?token={ret[0]['token']}&remoteName={si.nickname}&value={newHostedRemainingSpaceInKb}") as resp:
                data = await resp.json()
                if data != "OK":
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Remote server error",
                        headers={"WWW-Authenticate": "Bearer"},
                    )    
                    
        await database.execute(f"""update hosted_remotes
                               set remaining_space_in_kb = {newHostedRemainingSpaceInKb}
                               where nickname = '{si.nickname}'""")    
    
    
    await database.execute(f"""update my_remotes 
                               set address = '{si.address}', 
                               port = {si.port} 
                               where nickname = '{si.nickname}'""")
    
    
    return
    
    
@router.get("/manualUpdateStorageAvailable")
async def manualUpdateStorageAvailable(token: str, remoteName: str, value: float, request: Request):
    if not await verifyRemote(remoteName, token, request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    await database.execute(f"""update my_remotes
                               set remaining_space_in_kb = {value}
                               where nickname = '{remoteName}'""")
    return "OK"
    
    