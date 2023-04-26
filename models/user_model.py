from pydantic import BaseModel


''' Model Schema Using Pydantic '''


class User(BaseModel):
    username: str
    password: str