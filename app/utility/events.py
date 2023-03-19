from fastapi import APIRouter, HTTPException, Request, status
from utility.database import database
from utility.auth import verifyJwt, verifyJwtOrLocal, verifyRemote
import json

router = APIRouter(
    prefix="/events",
    tags=["events"],
)

@router.get("/getAll")
async def getEventLog(token: str, offset: int):
    # change to just jwt later
    if not await verifyJwt(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    

    return await database.fetch_all(f"""select ts, name, status from taskqueue order by ts desc offset {offset} limit 20""")


# @router.get("/TESTINSRT")
# async def acceptDeleteFromUser():
#     dict = {'test': 'test'}
#     for i in range(100):
#         await database.execute(f"""insert into taskqueue (name, task, ts, status, try, retry_ts) values('Test task {i}', '{json.dumps(dict)}', NOW(), 'Queued', 0, NOW())""")

#     return "OK"
