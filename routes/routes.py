from typing import Annotated
from fastapi import APIRouter, Body
from query.query import obtener_cantidad_operaciones, obtener_suma_monto_operaciones, obtener_comparativo_desembolso, obtener_sucursales_franquicia


api_route = APIRouter()


@api_route.post("/cantidad-operaciones", status_code=200)
async def obtener_cantidad_operaciones_ruta(fechas1:str=None, alt_franquicia:str=None, fechas: Annotated[str | None, Body()] = None):
    return obtener_cantidad_operaciones(alt_franquicia, fechas)

@api_route.get("/suma-operaciones", status_code=200)
async def obtener_suma_operaciones():
    return obtener_suma_monto_operaciones()

@api_route.get("/comparativo-suma-operaciones", status_code=200)
async def obtener_suma_comparativo_desembolso():
    return obtener_comparativo_desembolso()

@api_route.get("/obtener-sucursales", status_code=200)
async def obtener_sucursales():
    return obtener_sucursales_franquicia()

# @api_route.post("/login", response_class=User, status_code=201)
# def login(username: User.username, password:User.password)->json:
#     pass
