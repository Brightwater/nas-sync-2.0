from sqlalchemy import String

from pydantic import BaseModel, Field
from fastapi import FastAPI
from typing import List, Dict


class User(BaseModel):
    username: str
    password: str
    
class Init(BaseModel):
    user: User
    basePath: str
    
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: str | None = None
    
class SyncTree(BaseModel):
    treeJson: str
    
class SyncData(BaseModel):
    syncSize: int = Field(None, title="syncSize")
    metadataFileName: str = Field(None, title="metadataFileName")
    name: str = Field(None, title="name")
    individualFilesWithHashes: List[Dict[str, str]] = Field([], title="individualFilesWithHashes")
    
class SyncUpdateData(BaseModel):
    syncSize: int = Field(None, title="syncSize")
    metadataFileName: str = Field(None, title="metadataFileName")
    name: str = Field(None, title="name")
    fileChanges: List[Dict[str, str]] = Field([], title="fileChanges")
    pendingDeletes: List[str]
    
class ServerInfo(BaseModel):
    used_space_in_gb: float
    nickname: str
    address: str
    token: str
    port: int
    remaining_space_in_gb: float
    hosted_remaining_space_in_gb: float
    hosted_used_space_in_gb: float
