
from datetime import timedelta, datetime, timezone
import secrets
import string
from fastapi import HTTPException, status, APIRouter, Request
import json
from model.pydantics import *
from utility.database import database
from passlib.context import CryptContext
from fastapi.concurrency import run_in_threadpool
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordBearer
from logs.asynccustomlogger import Asynclog

SECRET_KEY = ""
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
log = Asynclog("Auth")

router = APIRouter(
    prefix="/auth",
    tags=["auth"],
)

async def verifyJwt(token: str):
    #token = req.headers['Authorization']
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("username")  # type: ignore
        if username is None:
            raise credentials_exception
        #token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    return True

async def verifyJwtOrLocal(token: None, request: Request):
    if request.client[0][0:9] == "192.168.1" or request.client[0][0:9] == "127.0.0.1":
        return True
    else:
        return await verifyJwt(token)    
    
async def verifyRemote(name: str, token: str, request: Request):
    remotes = await database.fetch_one(f"select address, token from hosted_remotes where nickname = '{name}' and token = '{token}'")
    if not remotes:
        return False
    # return True
    if request.client[0][0:8] == "192.168." or request.client[0][0:9] == "127.0.0.1" or request.client[0] == remotes['address']:
        return True
    else:
        return False

def getPasswordHash(password):
    return pwd_context.hash(password)

def verifyPassword(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def createJwt(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# @router.get("/test", tags=["test"])
# async def root():
#     # query = "select * from authenticated_user"
#     query = f"select username, encrypted_refresh_token as refresh, scopes from authenticated_user where username = 'abc'"

#     data = await database.fetch_all(query)
#     return data

@router.post("/login/", tags=['auth'])
async def login(user: User):
    # return "OK"
    d = await database.fetch_all(f"select username, password from authenticated_user where username = '{user.username}'")
    if not await run_in_threadpool(lambda: verifyPassword(user.password, d[0].password)):  # type: ignore
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    refreshToken = await run_in_threadpool(lambda: ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(20)))
    encodedRefreshToken = await run_in_threadpool(lambda: getPasswordHash(refreshToken))
    expireTime = round(datetime.now(timezone.utc).timestamp()) + 87600 * REFRESH_TOKEN_EXPIRE_DAYS
    
    
    
    oldRefreshTokens = await database.fetch_one(f"""select refresh_token_data from authenticated_user where username = '{user.username}'""")
    
    log.info(f"token data {oldRefreshTokens['refresh_token_data']}")
    if oldRefreshTokens['refresh_token_data'] == None or len(json.loads(oldRefreshTokens['refresh_token_data'])) == 0:
        oldRefreshTokens = []
        lastTokenIndex = -1
    else:
        oldRefreshTokens = json.loads(oldRefreshTokens['refresh_token_data'])
        lastTokenIndex = oldRefreshTokens[-1]['tokenIndex']
    oldRefreshTokens: list = oldRefreshTokens
    
    oldRefreshTokens.append({"encrypted_refresh_token": encodedRefreshToken, 'refresh_token_expiration': expireTime, "tokenIndex": lastTokenIndex+1})

    response = {'username':user.username ,'refreshToken': refreshToken, 'exp': expireTime, "tokenIndex": lastTokenIndex+1}
   
    await database.execute(f"""update authenticated_user 
                               set refresh_token_data = '{json.dumps(oldRefreshTokens)}'
                               where username = '{user.username}'""")

    # await database.execute(f"update authenticated_user set encrypted_refresh_token = '{encodedRefreshToken}', refresh_token_expiration = to_timestamp({expireTime}) where username = '{user.username}'")
    return response

@router.post("/createUser/")
async def login3(user: User):
    hashedPassword = await run_in_threadpool(lambda: getPasswordHash(user.password))
    userVal = {"username": user.username, "password": hashedPassword}
    
    insert = f"insert into authenticated_user values(:username, :password)"
    return await database.execute(insert, values=userVal)

@router.post("/login/token")
async def loginToken(user: User, tokenIndex: int):
    # dUser = await database.fetch_all(f"select username, encrypted_refresh_token as refresh, extract(epoch from refresh_token_expiration) as exp, scopes from authenticated_user where username = '{user.username}'")
    ret = await database.fetch_all(f"select username, refresh_token_data, scopes from authenticated_user where username = '{user.username}'")
    refreshData = json.loads(ret[0]['refresh_token_data'])
    count = -1
    for d in refreshData:
        count = count + 1
        if d['tokenIndex'] == tokenIndex:
            break
    dUser = {'username': ret[0]['username'], 'refresh': refreshData[count]['encrypted_refresh_token'], 'exp': refreshData[count]['refresh_token_expiration'], 'scopes': ret[0]['scopes']}
  
    if not dUser['exp'] or round(dUser['exp']) <= round(datetime.now(timezone.utc).timestamp()):  # type: ignore
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired, please login again",
            headers={"WWW-Authenticate": "Bearer"},
        ) 
    if not await run_in_threadpool(lambda: verifyPassword(user.password, dUser['refresh'])):  # type: ignore
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = createJwt(
        data={"username": user.username, "scopes": dUser['scopes']}, expires_delta=access_token_expires  # type: ignore
    )
    
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/verifyToken")
async def get_current_user(token):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("username")  # type: ignore
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    
    user = await database.fetch_all(f"select username, password from authenticated_user where username = '{token_data.username}'")
    if user is None:
        raise credentials_exception
    
    return user

