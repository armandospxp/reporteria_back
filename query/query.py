# from sqlalchemy.orm import Session
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
    query = "select rtrim(c.SUCURSAL) as SUCURSAL, SUM(CASE WHEN MONTH(c.FECHAOPE) = MONTH(CURDATE()) THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS 'mes_actual', "\
        "SUM(CASE WHEN month (c.FECHAOPE) = month(CURDATE()) - 1 THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS 'mes_pasado', "\
        "SUM(CASE WHEN month(c.FECHAOPE) = MONTH(CURDATE()) - 2 THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS 'mes_antepasado' from operaciones.colocacion c "\
        "where c.FRANQUICIA like '%BOSAMAZ%' and c.FECHAOPE <= DATE_SUB(CURDATE(), INTERVAL 2 month) GROUP BY c.SUCURSAL order by MONTO_CONSOLIDADO desc;"
    datos = conn.execute(text(query))
    results = []
    if (band == 0):
        for i in datos.fetchall():
            results.append({"name": i[0], "series": [{
                "name": "febrero",
                "value": i[3]
            }, {
                "name": "marzo",
                "value": i[2]
            },
                {
                "name": "abril",
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
        print(results)

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
    query = "SELECT DAY(c.fechaope) AS dia, SUM(CASE WHEN YEAR(c.FECHAOPE) = YEAR(date('2022-05-01')) THEN c.MONTO_DESEMBOLSADO ELSE 0 END) "\
        "AS ventas_actual, SUM(CASE WHEN YEAR(c.FECHAOPE) = YEAR(date('2022-05-01')) - 1 THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS ventas_anterior "\
            "FROM operaciones.colocacion c WHERE MONTH(c.FECHAOPE) = MONTH(date('2022-05-01')) AND DAYOFWEEK(c.FECHAOPE) BETWEEN 2 AND 6 and c.FRANQUICIA like '%BOSAMAZ%' "\
        "GROUP BY DAY(c.FECHAOPE) ORDER BY DAY(c.FECHAOPE);"

    print(query)
    datos = conn.execute(text(query))
    results = []
    results.append({"name": "mes_actual", "series": []})
    results.append({"name": "mes_anterior", "series": []})
    for i in datos.fetchall():
        results[0]["series"].append({"name": i[0], "value": i[1]})
        results[1]["series"].append({"name": i[0], "value": i[2]})
    return results


def obtener_metas_franquicia():
    conn = conectar_base(engine)
    conn2 = conectar_base(db2_engine)
    query = "select m.franquicia, m.meta from metas m where month(m.fecha) = month(now()) and year(m.fecha) = year(now()) and m.franquicia like '%"+franquicia+"%'"
    query2 = "SELECT sum(f.BFSOLI) monto FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV s ON f.BFSUCU = s.FRSUC "\
        "WHERE s.FRGERSUC LIKE '%BOSAMAZ%' AND YEAR(f.BFFCHV) = YEAR (now()) AND MONTH(BFFCHV) = month(now())"
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


def obtener_suma_monto_desembolsado():
    conn = conectar_base(db2_engine)
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
