# from sqlalchemy.orm import Session
import datetime
from database.database import engine, db2_engine
from sqlalchemy import text, create_engine
from sqlalchemy.ext.serializer import loads, dumps
from fastapi.encoders import jsonable_encoder
import pdb

# conn = engine.connect()
# db2_conn = db2_engine.connect()

global franquicia

franquicia = "BOSAMAZ"


def conectar_base(engine: create_engine):
    try:
        return engine.connect()
    except:
        print("Error al conectar a la base de datos")


def obtener_cantidad_operaciones(fechas=None):
    conn = conectar_base(db2_engine)
    if fechas:
        fecha_desde = fechas['fechaDesde']
        fecha_hasta = fechas['fechaHasta']
        query = "SELECT rtrim(s.FRGERSUC) AS SUCURSAL, count(*) AS CANTIDAD FROM DB2ADMIN.VEVOCARTERA v JOIN DB2ADMIN.FSTFRANLEV s ON v.VECSUCU  = s.FRSUC "\
            "WHERE v.VECANHO BETWEEN YEAR(date('"+fecha_desde+"')) AND YEAR(date('"+fecha_hasta+"')) AND v.vecmes BETWEEN MONTH(date('"+fecha_desde+"')) "\
            "AND MONTH(date('"+fecha_hasta+"')) AND v.VECFRNOM LIKE '%"+franquicia + \
                "%' GROUP BY s.FRGERSUC ORDER BY cantidad desc;"
        print(query)
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        datos.close()
        desconectar_base(engine)
        return results
    else:
        query = "SELECT rtrim(s.FRGERSUC) AS SUCURSAL, count(*) AS CANTIDAD FROM DB2ADMIN.VEVOCARTERA v JOIN DB2ADMIN.FSTFRANLEV s ON v.VECSUCU  = s.FRSUC "\
            "WHERE v.VECANHO = YEAR(now()) AND v.VECFRNOM LIKE '%"+franquicia + \
                "%' GROUP BY s.FRGERSUC ORDER BY cantidad desc;"
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        datos.close()
        conn.close()
        return results


def obtener_suma_monto_operaciones(fechas=None):
    band = 0
    conn = conectar_base(db2_engine)
    query = "SELECT l.FRGERSUC, "\
        "SUM(CASE WHEN MONTH(f.BFFCHV) = MONTH(CURRENT DATE - 2 MONTHS) THEN f.BFSOLI ELSE 0 END) AS MES1, "\
        "SUM(CASE WHEN MONTH(f.BFFCHV) = MONTH(CURRENT DATE - 1 MONTHS) THEN f.BFSOLI ELSE 0 END) AS MES2, "\
        "SUM(CASE WHEN MONTH(f.BFFCHV) = MONTH(CURRENT DATE) THEN f.BFSOLI ELSE 0 END) AS MES3 " \
        "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
        "WHERE f.BFFCHV >= CURRENT DATE - 3 MONTHS and f.BFOPER not in (405,410) and "\
            "f.BFESTA in (7,10) AND l.FRDIRSUC LIKE '%"+franquicia+"%' "\
            "GROUP BY l.FRGERSUC;"
    datos = conn.execute(text(query))
    results = []
    if (band == 0):
        for i in datos.fetchall():
            results.append({"name": i[0], "series": [{
                "name": "marzo",
                "value": i[3]
            }, {
                "name": "abril",
                "value": i[2]
            },
                {
                "name": "mayo",
                "value": i[1]
            }]})
    else:
        for i in datos.fetchall():
            results.append({"name": i[0], "series": [{
                "name": fecha_desde,
                "value": i[1]
            }, {
                "name": fecha_hasta,
                "value": i[2]
            }]})
    datos.close()
    conn.close()
    return results


def suma_monto_operaciones_sucursales(fechas=None):
    conn = conectar_base(db2_engine)
    if fechas:
        fecha_desde = fechas['fechaDesde']
        fecha_hasta = fechas['fechaHasta']
        query = "SELECT RTRIM(s.FRGERSUC) AS SUCURSAL, sum(v.VECDESMB) AS MONTO FROM DB2ADMIN.VEVOCARTERA v JOIN DB2ADMIN.FSTFRANLEV s ON v.VECSUCU  = s.FRSUC "\
            "WHERE v.VECANHO BETWEEN YEAR(date('"+fecha_desde+"')) AND YEAR(date('"+fecha_hasta+"')) AND v.vecmes BETWEEN MONTH(date('"+fecha_desde+"')) "\
            "AND MONTH(date('"+fecha_hasta+"')) AND v.VECFRNOM LIKE '%"+franquicia+"%' "\
            "GROUP BY s.FRGERSUC ORDER BY monto desc;"
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        datos.close()
        conn.close()
        return results
    else:
        query = "SELECT RTRIM(s.FRGERSUC) AS SUCURSAL, sum(v.VECDESMB) AS MONTO FROM DB2ADMIN.VEVOCARTERA v JOIN DB2ADMIN.FSTFRANLEV s ON v.VECSUCU  = s.FRSUC "\
            "WHERE v.VECANHO = YEAR(now()) AND v.VECFRNOM LIKE '%"+franquicia+"%' "\
            "GROUP BY s.FRGERSUC ORDER BY monto desc;"
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        datos.close()
        conn.close()
        return results


def obtener_comparativo_desembolso():
    query = "SELECT rtrim(c.SUCURSAL) as SUCRUSAL, sum(CASE WHEN YEAR(c.FECHAOPE) = YEAR(CURRENT_DATE()) AND MONTH(c.FECHAOPE) >= MONTH(CURRENT_DATE()) - 3 THEN c.MONTO_DESEMBOLSADO ELSE 0 END) "\
        "AS operaciones_anio_actual, sum(CASE WHEN YEAR(c.FECHAOPE) = YEAR(CURRENT_DATE()) - 1 AND MONTH(c.FECHAOPE) >= MONTH(CURRENT_DATE()) - 3 THEN c.MONTO_DESEMBOLSADO ELSE 0 END) "\
        "AS operaciones_anio_pasado FROM operaciones.colocacion c WHERE YEAR(c.FECHAOPE) IN (YEAR(CURRENT_DATE()), YEAR(CURRENT_DATE()) - 1) "\
        "AND MONTH(c.FECHAOPE) >= MONTH(CURRENT_DATE()) - 3 AND c.FRANQUICIA like '%" + \
        franquicia + "%' GROUP BY c.SUCURSAL"

    datos = conn.execute(text(query))
    results = []
    for i in datos.fetchall():
        results.append({"name": i[0], "series": [{
            "name": "anterior",
            "value": i[2]
        }, {
            "name": "actual",
            "value": i[1]
        }]})

    return results


def obtener_sucursales_franquicia(alt_franquicia=None):
    conn = conectar_base(db2_engine)
    query = "SELECT DISTINCT(rtrim(s.FRGERSUC)) AS SUCURSAL FROM DB2ADMIN.VEVOCARTERA v JOIN DB2ADMIN.FSTFRANLEV s ON v.VECSUCU  = s.FRSUC "\
        "WHERE v.VECANHO = YEAR(now()) AND v.VECFRNOM LIKE '%"+franquicia+"%';"
    datos = conn.execute(text(query))
    results = []
    for i in datos.fetchall():
        results.append({"name": i[0], "seleccionado": True})
    datos.close()
    conn.close()
    return results


def obtener_versus_mes(alt_franquicia=None):
    conn = conectar_base(db2_engine)
    query = "SELECT EXTRACT(MONTH FROM c.BFFCHV) AS mes, "\
        "SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) THEN c.BFSOLI ELSE 0 END) AS año_actual, "\
        "SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) -1 THEN c.BFSOLI ELSE 0 END) AS año_pasado "\
        "FROM DB2ADMIN.FSD0122 c JOIN DB2ADMIN.FSTFRANLEV f ON c.BFSUCU = f.FRSUC WHERE EXTRACT(YEAR FROM c.BFFCHV) IN (YEAR(CURRENT DATE), YEAR(CURRENT DATE)-1) "\
        "AND c.BFTIP = 'A' and c.BFOPER not in (405,410) and c.BFESTA in (7,10) AND f.FRDIRSUC LIKE '%"+franquicia+"%' "\
        "GROUP BY EXTRACT(MONTH FROM c.BFFCHV) ORDER BY EXTRACT(MONTH FROM c.BFFCHV);"
    datos = conn.execute(text(query))
    results = []
    results.append({"name": "año_actual", "series": []})
    results.append({"name": "año_anterior", "series": []})
    for i in datos.fetchall():
        if i[1] == 0:
            pass
        else:
            results[0]["series"].append({"name": i[0], "value": i[1]})
        results[1]["series"].append({"name": i[0], "value": i[2]})
    datos.close()
    conn.close()
    return results


def obtener_metas_franquicia():
    conn = conectar_base(engine)
    conn2 = conectar_base(db2_engine)
    query = "select m.franquicia, m.meta from metas m where month(m.fecha) = month(now()) and year(m.fecha) = year(now()) and m.franquicia like '%"+franquicia+"%'"
    query2 = "SELECT sum(f.BFSOLI) monto FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV s ON f.BFSUCU = s.FRSUC "\
        "WHERE s.FRDIRSUC LIKE '%BOSAMAZ%' AND YEAR(f.BFFCHV) = YEAR (now()) AND MONTH(BFFCHV) = month(now()) and f.BFOPER not in (405,410) and f.BFESTA in (7,10)"
    datos = conn.execute(text(query))
    datos2 = conn2.execute(text(query2))
    results = []
    for i in datos.fetchall():
        results.append({"name": "meta", "value": i[1]})
    for j in datos2.fetchall():
        results.append({"name": "actual", "value": j[0]})
    datos.close()
    datos2.close()
    conn.close()
    conn2.close()
    return results


# def get_user(db: Session, user_id: int):
#     return db.query(models.User).filter(models.User.id == user_id).first()


# def get_user_by_email(db: Session, email: str):
#     return db.query(models.User).filter(models.User.email == email).first()


# def get_users(db: Session, skip: int = 0, limit: int = 100):
#     return db.query(models.User).offset(skip).limit(limit).all()


# def create_user(db: Session, user: schemas.UserCreate):
#     fake_hashed_password = user.password + "notreallyhashed"
#     db_user = models.User(email=user.email, hashed_password=fake_hashed_password)
#     db.add(db_user)
#     db.commit()
#     db.refresh(db_user)
#     return db_user


# def get_items(db: Session, skip: int = 0, limit: int = 100):
#     return db.query(models.Item).offset(skip).limit(limit).all()


# def create_user_item(db: Session, item: schemas.ItemCreate, user_id: int):
#     db_item = models.Item(**item.dict(), owner_id=user_id)
#     db.add(db_item)
#     db.commit()
#     db.refresh(db_item)
#     return db_item

# SELECT
#   SUM(CASE WHEN c.FECHAOPE = date('2021-01-01') THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS monto_fecha1,
#   SUM(CASE WHEN c.FECHAOPE = date('2021-01-02') THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS monto_fecha2
# from colocacion c
# WHERE c.FECHAOPE  IN (date('2021-01-01'), date('2021-01-02')) and c.FRANQUICIA like '%BOSAMAZ%';

# select DATE('2021-01-01')
