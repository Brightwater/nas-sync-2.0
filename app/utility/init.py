from fastapi import APIRouter, HTTPException, status
from model.pydantics import User, Init
from utility.auth import getPasswordHash
from fastapi.concurrency import run_in_threadpool
from utility.database import database
from cryptography.fernet import Fernet

router = APIRouter(
    prefix="/init",
    tags=["init"],
)

@router.post("/")
async def initApp(init: Init):
        
    #check if init already done
    userExists = await database.fetch_one(f"select username from authenticated_user")
    if userExists:
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Already init"
        ) 
        
    hashedPassword = await run_in_threadpool(lambda: getPasswordHash(init.user.password))
    
    transaction = await database.transaction()
    try:
        insert = f"""insert into authenticated_user 
        values('{init.user.username}', '{hashedPassword}', 
            null, null, ARRAY ['admin'])"""
        
        await database.execute(insert)
        
    except:
        await transaction.rollback()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Init failed"
        ) 
    else:
        await transaction.commit()
        return init.user.username
    
