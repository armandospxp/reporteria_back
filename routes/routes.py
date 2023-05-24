from typing import Annotated
from fastapi import APIRouter, Body, Depends
from fastapi.security import OAuth2PasswordBearer
from query.query import obtener_cantidad_operaciones, obtener_suma_monto_operaciones, obtener_comparativo_desembolso,\
    obtener_sucursales_franquicia, suma_monto_operaciones_sucursales, obtener_versus_mes, obtener_metas_franquicia, \
    obtener_situacion_venta_actual

from auth.auth import authenticate_user, verificar_usuario


api_route = APIRouter()


@api_route.post("/cantidad-operaciones", status_code=200)
async def obtener_cantidad_operaciones_ruta(fechas: Annotated[dict | None, Body()] = None):
    return obtener_cantidad_operaciones(fechas)


@api_route.get("/cantidad-operaciones", status_code=200)
async def obtener_cantidad_operaciones_ruta():
    return obtener_cantidad_operaciones()


@api_route.get("/suma-operaciones", status_code=200)
async def obtener_suma_operaciones():
    return obtener_suma_monto_operaciones()


@api_route.post("/suma-operaciones", status_code=200)
async def obtener_suma_operaciones(fechas: Annotated[dict | None, Body()] = None):
    return obtener_suma_monto_operaciones(fechas)


@api_route.get("/suma-operaciones-sucursal", status_code=200)
async def obtener_suma_operaciones():
    return suma_monto_operaciones_sucursales()


@api_route.post("/suma-operaciones-sucursal", status_code=200)
async def obtener_suma_operaciones(fechas: Annotated[dict | None, Body()] = None):
    return suma_monto_operaciones_sucursales(fechas)


@api_route.get("/comparativo-suma-operaciones", status_code=200)
async def obtener_suma_comparativo_desembolso():
    return obtener_comparativo_desembolso()


@api_route.post("/comparativo-suma-operaciones", status_code=200)
async def obtener_suma_comparativo_desembolso(fechas: Annotated[dict | None, Body()] = None):
    return obtener_comparativo_desembolso()


@api_route.get("/obtener-sucursales", status_code=200)
async def obtener_sucursales():
    return obtener_sucursales_franquicia()


@api_route.get("/obtener-versus-mensual", status_code=200)
async def obtener_versus():
    return obtener_versus_mes()


@api_route.get("/obtener-metas-franquicias", status_code=200)
async def obtener_metas():
    return obtener_metas_franquicia()


@api_route.get("/obtener-situacion-franquicias", status_code=200)
async def obtener_situacion():
    return obtener_situacion_venta_actual()


@api_route.get("/obtener-situacion-franquicias2", status_code=200)
async def obtener_situacion(token: Annotated[str, Depends(verificar_usuario)]):
    return obtener_situacion_venta_actual()


@api_route.post("/login")
async def login(user: Annotated[str | None, Body()] = None, password: Annotated[str | None, Body()] = None):
    return authenticate_user(user, password)

# @api_route.post("/login", response_class=User, status_code=201)
# def login(username: User.username, password:User.password)->json:
#     pass
