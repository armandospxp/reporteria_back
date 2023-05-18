# from sqlalchemy.orm import Session
from database.database import engine
from sqlalchemy import text
from sqlalchemy.ext.serializer import loads, dumps
from fastapi.encoders import jsonable_encoder
import pdb

conn = engine.connect()

global franquicia

franquicia = "BOSAMAZ"


def obtener_cantidad_operaciones(fechas=None):
    if fechas:
        fecha_desde = fechas['fechaDesde']
        fecha_hasta = fechas['fechaHasta']
        query = "select rtrim(c.SUCURSAL) as SUCURSAL, COUNT(*) as CANTIDAD from operaciones.colocacion c "\
            + "where c.FRANQUICIA like '%" + franquicia + \
                "%' AND C.FECHAOPE BETWEEN date('"+fecha_desde+"') and date('" + \
            fecha_hasta+"') GROUP BY c.SUCURSAL order by CANTIDAD desc;"
        print(query)
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        return results
    else:
        query = "select rtrim(c.SUCURSAL) as SUCURSAL, COUNT(*) as CANTIDAD from operaciones.colocacion c "\
            + "where c.FRANQUICIA like '%" + franquicia + \
                "%'GROUP BY c.SUCURSAL order by CANTIDAD desc;"
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        return results


def obtener_suma_monto_operaciones(fechas=None):
    band = 0
    # if fechas:
    #     fecha_desde = fechas['fechaDesde']
    #     fecha_hasta = fechas['fechaHasta']
    #     query = "SELECT c.SUCURSAL, rtrim(c.sucursal), SUM(CASE WHEN c.FECHAOPE = date("+fecha_desde+") THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS monto_fecha1, SUM(CASE WHEN c.FECHAOPE = date("+fecha_hasta+")"\
    #          "THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS monto_fecha2 from colocacion c WHERE c.FECHAOPE  IN (date("+fecha_desde+"), date("+fecha_hasta+")) and c.FRANQUICIA like '%BOSAMAZ%' group by c.SUCURSAL;"
    #     #pdb.set_trace()
    #     band = 1

    # else:
    #     query ="select rtrim(c.SUCURSAL) as SUCURSAL, SUM(CASE WHEN MONTH(c.FECHAOPE) = MONTH(CURDATE()) THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS 'mes_actual', "\
    #             "SUM(CASE WHEN month (c.FECHAOPE) = month(CURDATE()) - 1 THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS 'mes_pasado', "\
    #                 "SUM(CASE WHEN month(c.FECHAOPE) = MONTH(CURDATE()) - 2 THEN c.MONTO_DESEMBOLSADO ELSE 0 END) AS 'mes_antepasado' from operaciones.colocacion c "\
    #                     "where c.FRANQUICIA like '%BOSAMAZ%' and c.FECHAOPE <= DATE_SUB(CURDATE(), INTERVAL 2 month) GROUP BY c.SUCURSAL order by MONTO_CONSOLIDADO desc;"
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
    if fechas:
        fecha_desde = fechas['fechaDesde']
        fecha_hasta = fechas['fechaHasta']
        query = "select rtrim(c.SUCURSAL) as SUCURSAL, SUM(c.MONTO_DESEMBOLSADO) as CANTIDAD from operaciones.colocacion c where c.FRANQUICIA like '%BOSAMAZ%'  AND C.FECHAOPE BETWEEN date('"+fecha_desde+"')"\
            "and date('"+fecha_hasta + \
            "') GROUP BY c.SUCURSAL order by CANTIDAD desc;"
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        return results
    else:
        query = "select rtrim(c.SUCURSAL) as SUCURSAL, SUM(c.MONTO_DESEMBOLSADO) as CANTIDAD from operaciones.colocacion c "\
            + "where c.FRANQUICIA like '%" + franquicia + \
                "%'GROUP BY c.SUCURSAL order by CANTIDAD desc;"
        datos = conn.execute(text(query))
        results = []
        for i in datos.fetchall():
            results.append({"name": i[0], "value": i[1]})
        return results
        " and date('"+fecha_hasta + \
            "') GROUP BY c.SUCURSAL order by CANTIDAD desc;"


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
    query = "select distinct rtrim(c.SUCURSAL) as SUCURSAL from operaciones.colocacion c where c.FRANQUICIA like '%"+franquicia+"%'"
    datos = conn.execute(text(query))
    results = []
    for i in datos.fetchall():
        results.append({"name": i[0], "seleccionado": True})

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
    results.append({"name": "mes_anterior", "series":[]})
    for i in datos.fetchall():
        results[0]["series"].append({"name":i[0], "value":i[1]})
        results[1]["series"].append({"name":i[0], "value":i[2]})
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
