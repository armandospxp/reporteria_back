from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import jwt
from starlette import status
from starlette.middleware.base import RequestResponseEndpoint
from starlette.requests import Request

from auth.auth import verificar_usuario, SECRET_KEY
from database.database import iniciar_base
from middleware.auth_middleware import VerificadorToken
from routes.routes import api_route
import uvicorn

app = FastAPI()

app.add_middleware(VerificadorToken)

origins = [
    "http://localhost",
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_route, prefix="/api/reporteria", tags=["reporteria"])


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == '__main__':
    iniciar_base()
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
