import datetime
import logging
import time
from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse
import aiofiles
import uvicorn
from model.pydantics import *
from utility.database import database
from utility import auth, file, init, events, dashboard, syncs
from fastapi.middleware.cors import CORSMiddleware
from logs.asynccustomlogger import AsyncApilog

CHUNK_SIZE = 1024 * 1024  # = 1MB - adjust the chunk size as desired

appInitialized = False
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_methods=["*"],
    allow_headers=["*"],
)
app.state = "uninit"  # type: ignore

log = AsyncApilog("API")
# to run the app  uvicorn main:app --reload --port 4304 --host 0.0.0.0

@app.on_event("startup")
async def startup():
    app.include_router(auth.router)
    app.include_router(file.router)
    app.include_router(init.router)
    app.include_router(events.router)
    app.include_router(dashboard.router)
    app.include_router(syncs.router)
    await database.connect()
    userExists = await database.fetch_one(f"select username from authenticated_user")
    if userExists:
        app.state = True  # type: ignore
    
    
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
    
@app.get("/hello")
async def hello():
    return "hello world"

@app.get("/db")
async def db():
    #print (await database.fetch_one(f"select username from authenticated_user"))
    return await database.fetch_one(f"select username from authenticated_user")

@app.get("/state")
async def getState(request: Request):

    return request.headers

# @app.middleware("http")
# async def add_process_time_header(request: Request, call_next):
#     start_time = time.time()
#     response = await call_next(request)
#     process_time = time.time() - start_time
#     response.headers["X-Process-Time"] = str(process_time)
#     return response

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    response = await call_next(request)

    process_time = round((time.time() - start_time), 5)

    log.info(f"[{request.method}] {request.url.path} {request.client.host}:{request.client.port} -- {response.status_code}: processed in {process_time}s")

    return response


# @app.get("/files")
# async def files():
#     return FileResponse(path="/home/jeremiah", filename="jere", media_type='application/octet-stream')

# @app.get("/downloadTest")
# async def fileDol():
#     async def iterfile():
#        async with aiofiles.open("/media/drives/Media-1/nassyncTemp/bVQGGLbGYugaovp.enc", 'rb') as f:
#             while chunk := await f.read(CHUNK_SIZE):
#                 yield chunk

#     headers = {'Content-Disposition': 'attachment; filename="outtest.enc"'}
#     return StreamingResponse(iterfile(), headers=headers, media_type='application/octet-stream')

