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
        query = "SELECT rtrim(l.FRGERSUC) AS sucursal, count(f.BFOPE1) cantidad "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFFCHV BETWEEN '"+fecha_desde+"' AND '"+fecha_hasta+"' and f.BFOPER not in (405,410) and "\
            "f.BFESTA in (7,10) AND l.FRDIRSUC LIKE '%"+franquicia+"%' "\
            "GROUP BY l.FRGERSUC ORDER BY cantidad desc;"
        print(query)
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        datos.close()
        conn.close()
        return results
    else:
        query = "SELECT rtrim(l.FRGERSUC) AS sucursal, count(f.BFOPE1) cantidad "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE YEAR(f.BFFCHV) = year(now()) and MONTH(f.BFFCHV) = MONTH(now()) and f.BFOPER not in (405,410) and f.BFESTA in (7,10) AND l.FRDIRSUC LIKE '%"+franquicia+"%' "\
            "GROUP BY l.FRGERSUC ORDER BY cantidad desc;"
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
    query = "SELECT rtrim(l.FRGERSUC), "\
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
        query = "SELECT rtrim(l.FRGERSUC) AS sucursal, sum(f.BFSOLI) monto "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC WHERE f.BFFCHV between '"+fecha_desde+"' "\
            "and '"+fecha_hasta+"' and f.BFOPER not in (405,410) and f.BFESTA in (7,10) AND l.FRDIRSUC LIKE '%"+franquicia+"%' "\
            "GROUP BY l.FRGERSUC ORDER BY monto desc;"
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        datos.close()
        conn.close()
        return results
    else:
        query = "SELECT rtrim(l.FRGERSUC) AS sucursal, sum(f.BFSOLI) monto "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE YEAR(f.BFFCHV) = year(now()) and month(f.BFFCHV) = month(now()) and f.BFOPER not in (405,410) and f.BFESTA in (7,10) AND l.FRDIRSUC LIKE '%"+franquicia+"%' "\
            "GROUP BY l.FRGERSUC ORDER BY monto desc;"
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
    query = "SELECT rtrim(f.FRGERSUC) FROM DB2ADMIN.FSTFRANLEV f WHERE f.FRDIRSUC LIKE '" + \
        franquicia+"%';"
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
    query = "select m.meta from metas m where month(m.fecha) = month(now()) and year(m.fecha) = year(now()) and m.franquicia like '%"+franquicia+"%'"
    datos = conn.execute(text(query))
    results = []
    for i in datos.fetchall():
        results.append({"name": "meta", "value": i[0]})
    datos.close()
    conn.close()
    return results


def obtener_situacion_venta_actual():
    conn = conectar_base(db2_engine)
    query = "SELECT sum(f.BFSOLI) monto FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV s ON f.BFSUCU = s.FRSUC "\
        "WHERE s.FRDIRSUC LIKE '%BOSAMAZ%' AND YEAR(f.BFFCHV) = YEAR (now()) AND MONTH(BFFCHV) = month(now()) and f.BFOPER not in (405,410) and f.BFESTA in (7,10)"
    datos = conn.execute(text(query))
    results = []
    for i in datos.fetchall():
        results.append({"name": "actual", "value": i[0]})
    datos.close()
    conn.close()
    return results


def obtener_variacion_colocacion_banca_tipo(filtros: dict = None):
    conn = conectar_base(db2_engine)
    tipo_banca = str(filtros.get("tipo_banca"))
    anterior = filtros.get("anterior")
    debito = filtros.get("debito")
    # pdb.set_trace()
    if debito == 1:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (601) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (600) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (606) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (605) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) = year(now()) AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia+"%';"
    elif debito == 2:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (601) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (600) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (606) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (605) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) =  year(now()) -"+str(anterior)+" AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia+"%';"
    elif anterior:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (201) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (200) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (305) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (202, 205) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) = year(now()) -"+str(anterior)+" AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia+"%';"
    else:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (201) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (200) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (305) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (202, 205) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) = year(now()) AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia+"%';"
    print(query)
    datos = conn.execute(text(query))
    results = []
    for i in datos.fetchall():
        results.append({"name": "DESCUENTO CHEQUES", "value": i[0]})
        results.append({"name": "NUEVOS INT", "value": i[1]})
        results.append({"name": "NUEVOS MET", "value": i[2]})
        results.append({"name": "RECURR INT", "value": i[3]})
        results.append({"name": "RECURR MET", "value": i[4]})
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
