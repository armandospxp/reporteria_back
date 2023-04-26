from fastapi import APIRouter
from typing import List
import json
from query.query import obtener_cantidad_operaciones, obtener_suma_monto_operaciones

api_route = APIRouter()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api_route.get("/cantidad-operaciones", status_code=200)
async def obtener_cantidad_operaciones():
    return obtener_cantidad_operaciones()

@api_route.get("/suma-operaciones", status_code=200)
async def obtener_suma_operaciones():
    return obtener_suma_operaciones()

# @api_route.post("/login", response_class=User, status_code=201)
# def login(username: User.username, password:User.password)->json:
#     pass
