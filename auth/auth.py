from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from jose import JWTError, jwt
from passlib.context import CryptContext

from models.user_model import User as UserModel
from models.token_model import TokenData
from query.auth_query import login_usuario, obtener_datos_usuario
from settings import Settings

settings = Settings()


SECRET_KEY = settings.secret_key
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = settings.token_expire


# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/login")


# def verify_password(plain_password, password):
#     return pwd_context.verify(plain_password, password)


# def get_password_hash(password):
#     return pwd_context.hash(password)


# def get_user(username: str):
#     return UserModel.filter((UserModel.email == username) | (UserModel.username == username)).first()


def authenticate_user(username: str, password: str):
    log = login_usuario(username, password)
    if (log == True):
        datos_usuario = obtener_datos_usuario(username)
        generate_token(datos_usuario)
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Usuario o contraseña incorrectos", headers={"WWW-Authenticate": "Bearer"})

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def generate_token(datos_usuario:dict):
    # if not user:
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="Incorrect email/username or password",
    #         headers={"WWW-Authenticate": "Bearer"},
    #     )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    token = create_access_token(create_access_token(
        data={"sub": datos_usuario['usuario']}, expires_delta=access_token_expires
    ))
    data = {"token": token, "username":datos_usuario['usuario'], "franquicia":datos_usuario['franquicia']}
    return data
