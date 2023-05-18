from pydantic import BaseModel

class User(BaseModel):
    username:str
    is_active:bool
    franquicia:str