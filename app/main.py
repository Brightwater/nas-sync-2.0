
from datetime import timedelta, datetime, timezone
import secrets
import string
from fastapi import Depends, FastAPI, HTTPException, status
import uvicorn
from model.pydantics import *
from utility.database import database
from passlib.context import CryptContext
from fastapi.concurrency import run_in_threadpool
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordBearer

SECRET_KEY = ""
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5
REFRESH_TOKEN_EXPIRE_DAYS = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app = FastAPI()

# to run the app  uvicorn main:app --reload --port 4304 --host 0.0.0.0
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

@app.on_event("startup")
async def startup():
    await database.connect()
    
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# @app.get("/")
# async def root():
#     # query = "select * from authenticated_user"
#     query = f"select username, encrypted_refresh_token as refresh, scopes from authenticated_user where username = 'abc'"

#     data = await database.fetch_all(query)
#     return data

@app.post("/login/")
async def login(user: User):
    d = await database.fetch_all(f"select username, password from authenticated_user where username = '{user.username}'")
    if not await run_in_threadpool(lambda: verifyPassword(user.password, d[0].password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    refreshToken = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(20))
    encodedRefreshToken = await run_in_threadpool(lambda: getPasswordHash(refreshToken))
    expireTime = round(datetime.now(timezone.utc).timestamp()) + 87600 * REFRESH_TOKEN_EXPIRE_DAYS
    response = {'username':user.username ,'refreshToken': refreshToken, 'exp': expireTime}
    await run_in_threadpool(lambda: print(expireTime))
    await database.execute(f"update authenticated_user set encrypted_refresh_token = '{encodedRefreshToken}', refresh_token_expiration = to_timestamp({expireTime}) where username = '{user.username}'")
    return response

@app.post("/createUser/")
async def login3(user: User):
    hashedPassword = await run_in_threadpool(lambda: getPasswordHash(user.password))
    userVal = {"username": user.username, "password": hashedPassword}
    
    insert = f"insert into authenticated_user values(:username, :password)"
    return await database.execute(insert, values=userVal)

@app.post("/login/token")
async def loginToken(user: User):
    dUser = await database.fetch_all(f"select username, encrypted_refresh_token as refresh, extract(epoch from refresh_token_expiration) as exp, scopes from authenticated_user where username = '{user.username}'")
    if not dUser[0].exp or round(dUser[0].exp) <= round(datetime.now(timezone.utc).timestamp()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired, please login again",
            headers={"WWW-Authenticate": "Bearer"},
        ) 
    if not await run_in_threadpool(lambda: verifyPassword(user.password, dUser[0].refresh)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = createJwt(
        data={"username": user.username, "scopes": dUser[0].scopes}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/verifyToken")
async def get_current_user(token):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("username")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    
    user = await database.fetch_all(f"select username, password from authenticated_user where username = '{token_data.username}'")
    if user is None:
        raise credentials_exception
    
    return user