from sqlalchemy import String

from pydantic import BaseModel
from fastapi import FastAPI

class User(BaseModel):
    username: str
    password: str
    
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None