from database.database import engine
from sqlalchemy import text
from sqlalchemy.ext.serializer import loads, dumps
from fastapi.encoders import jsonable_encoder
import pdb


conn = engine.connect()


def login_usuario(username: str, password: str):
    query1 = "CALL LoginUsuario('"+username+"', '"+password+"', @mensaje);"
    query2 = "SELECT @mensaje as p_out;"
    dato1 = conn.execute(text(query1))
    dato2 = conn.execute(text(query2))
    resultado = dato2.fetchall()
    #pdb.set_trace()
    if resultado[0] == (0,):
        return True
    else:
        return False


def obtener_datos_usuario(username:str):
    query = "select u.username as nombre_usuario, f.nombre_franquicia as franquicia from usuarios u join franquicias f where u.fk_franquicia = f.id_franquicia where u.username='"+username+"';"
    dato = conn.execute(text(query))
    result = []
    for i in dato.fetchall():
        result.append({"usuario": i[0], "franquicia": i[1]})
    return result