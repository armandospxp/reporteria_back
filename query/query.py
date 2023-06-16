import datetime
from database.database import engine, db2_engine
from sqlalchemy import text, create_engine
from sqlalchemy.ext.serializer import loads, dumps
from fastapi.encoders import jsonable_encoder
import pdb

global franquicia

franquicia = "BOSAMAZ"


class QueryConsult:
    def __init__(self, engine, query, nombre=None, sucursal=None, versus=None, variacion=None):
        self.engine = engine
        self.query = query
        self.nombre = nombre
        self.sucursal = sucursal
        self.versus = versus
        self.variacion = variacion

    def conectar_base(self):
        try:
            return self.engine.connect()
        except:
            print("Error al conectar a la base de datos")

    def obtener_datos(self):
        conn = self.conectar_base()
        datos = conn.execute(text(self.query))
        results = []
        if self.versus:
            results.append({"name": "año_actual", "series": []})
            results.append({"name": "año_anterior", "series": []})
            for i in datos.fetchall():
                if i[1] == 0:
                    pass
                else:
                    results[0]["series"].append({"name": i[0], "value": i[1]})
                    results[1]["series"].append({"name": i[0], "value": i[2]})
        elif self.variacion:
            for i in datos.fetchall():
                results.append({"name": "DESCUENTO CHEQUES", "value": i[0]})
                results.append({"name": "NUEVOS INT", "value": i[1]})
                results.append({"name": "NUEVOS MET", "value": i[2]})
                results.append({"name": "RECURR INT", "value": i[3]})
                results.append({"name": "RECURR MET", "value": i[4]})
        elif self.sucursal:
            for i in datos.fetchall():
                results.append({"name": i[0], "seleccionado": True})
        elif self.nombre:
            for n in self.nombre:
                for i in datos.fetchall():
                    results.append({"name": n, "value": i[0]})
        else:
            for i in datos.fetchall():
                results.append({"name": i[0], "value": i[1]})
        datos.close()
        conn.close()
        return results


def obtener_cantidad_operaciones(fechas=None):
    query = "SELECT rtrim(l.FRGERSUC) AS sucursal, count(f.BFOPE1) cantidad "\
        "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
        " WHERE f.BFOPER not in (405,410) and f.BFESTA in (7,10) AND l.FRDIRSUC LIKE '%" + \
        franquicia+"%' "
    if fechas:
        query = query + "AND f.BFFCHV BETWEEN '"+fechas['fechaDesde']+"' AND '" + \
            fechas['fechaHasta'] + \
            "' GROUP BY l.FRGERSUC ORDER BY cantidad desc;"
    else:
        query = query + \
            "AND YEAR(f.BFFCHV) = year(now()) and MONTH(f.BFFCHV) = MONTH(now()) GROUP BY l.FRGERSUC ORDER BY cantidad desc;"
    qry = QueryConsult(db2_engine, query)
    return qry.obtener_datos()


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


def suma_monto_operaciones_sucursales(**kwargs):
    query = "SELECT rtrim(l.FRGERSUC) AS sucursal, sum(f.BFSOLI) monto "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFOPER not in (405,410) and f.BFESTA in (7,10) AND l.FRDIRSUC LIKE '%" + \
        franquicia+"%' "
    if kwargs:
        query = query + "AND f.BFFCHV between '" + \
            kwargs['kwargs']['fechaDesde']+"' AND '"+kwargs['kwargs']['fechaHasta'] + \
            "' GROUP BY l.FRGERSUC ORDER BY monto desc;"
        qry = QueryConsult(db2_engine, query)
        return qry.obtener_datos()
    else:
        query = query + \
            "AND year(f.BFFCHV) = year(now()) and month(f.BFFCHV) = month(now()) GROUP BY l.FRGERSUC ORDER BY monto desc;"
        qry = QueryConsult(db2_engine, query)
        return qry.obtener_datos()


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
    query = "SELECT rtrim(f.FRGERSUC) FROM DB2ADMIN.FSTFRANLEV f WHERE f.FRDIRSUC LIKE '" + \
        franquicia+"%';"
    qry = QueryConsult(db2_engine, query, sucursal=True)
    return qry.obtener_datos()


def obtener_versus_mes(alt_franquicia=None):
    query = "SELECT EXTRACT(MONTH FROM c.BFFCHV) AS mes, "\
        "SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) THEN c.BFSOLI ELSE 0 END) AS año_actual, "\
        "SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) -1 THEN c.BFSOLI ELSE 0 END) AS año_pasado "\
        "FROM DB2ADMIN.FSD0122 c JOIN DB2ADMIN.FSTFRANLEV f ON c.BFSUCU = f.FRSUC WHERE EXTRACT(YEAR FROM c.BFFCHV) IN (YEAR(CURRENT DATE), YEAR(CURRENT DATE)-1) "\
        "AND c.BFTIP = 'A' and c.BFOPER not in (405,410) and c.BFESTA in (7,10) AND f.FRDIRSUC LIKE '%"+franquicia+"%' "\
        "GROUP BY EXTRACT(MONTH FROM c.BFFCHV) ORDER BY EXTRACT(MONTH FROM c.BFFCHV);"
    qry = QueryConsult(db2_engine, query, versus=True)
    return qry.obtener_datos()


def obtener_versus_mes_dia(alt_franquicia=None):
    query = "SELECT EXTRACT(DAY FROM c.BFFCHV) AS dia, SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) AND DAYOFWEEK(C.BFFCHV) BETWEEN 2 AND 6 THEN c.BFSOLI ELSE 0 END) "\
        "AS año_actual, SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) -1 AND DAYOFWEEK(C.BFFCHV) BETWEEN 2 AND 6 THEN c.BFSOLI ELSE 0 END) AS año_pasado "\
        "FROM DB2ADMIN.FSD0122 c JOIN DB2ADMIN.FSTFRANLEV f ON c.BFSUCU = f.FRSUC WHERE EXTRACT(YEAR FROM c.BFFCHV) IN (YEAR(CURRENT DATE), YEAR(CURRENT DATE)-1) "\
        "AND MONTH(c.BFFCHV) = month(current_date) AND c.BFTIP = 'A' and c.BFOPER not in (405,410) and c.BFESTA in (7,10) AND f.FRDIRSUC LIKE '%BOSAMAZ%' "\
        "GROUP BY EXTRACT(DAY FROM c.BFFCHV) ORDER BY EXTRACT(DAY FROM c.BFFCHV);"
    qry = QueryConsult(db2_engine, query, versus=True)
    return qry.obtener_datos()

def obtener_versus_mes_dia_cantidad(alt_franquicia=None):
    query = "SELECT EXTRACT(DAY FROM c.BFFCHV) AS dia, SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) AND DAYOFWEEK(C.BFFCHV) BETWEEN 2 AND 6 THEN 1 ELSE 0 END) "\
        "AS año_actual, SUM(CASE WHEN EXTRACT(YEAR FROM c.BFFCHV) = YEAR(CURRENT DATE) -1 AND DAYOFWEEK(C.BFFCHV) BETWEEN 2 AND 6 THEN 1 ELSE 0 END) AS año_pasado "\
        "FROM DB2ADMIN.FSD0122 c JOIN DB2ADMIN.FSTFRANLEV f ON c.BFSUCU = f.FRSUC WHERE EXTRACT(YEAR FROM c.BFFCHV) IN (YEAR(CURRENT DATE), YEAR(CURRENT DATE)-1) "\
        "AND MONTH(c.BFFCHV) = month(current_date) AND c.BFTIP = 'A' and c.BFOPER not in (405,410) and c.BFESTA in (7,10) AND f.FRDIRSUC LIKE '%BOSAMAZ%' "\
        "GROUP BY EXTRACT(DAY FROM c.BFFCHV) ORDER BY EXTRACT(DAY FROM c.BFFCHV);"
    qry = QueryConsult(db2_engine, query, versus=True)
    return qry.obtener_datos()


def obtener_metas_franquicia():
    query = "select m.meta from metas m where month(m.fecha) = month(now()) and year(m.fecha) = year(now()) and m.franquicia like '%"+franquicia+"%'"
    qry = QueryConsult(engine, query, nombre=["meta"])
    return qry.obtener_datos()


def obtener_situacion_venta_actual():
    query = "SELECT sum(f.BFSOLI) monto FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV s ON f.BFSUCU = s.FRSUC "\
        "WHERE s.FRDIRSUC LIKE '%BOSAMAZ%' AND YEAR(f.BFFCHV) = YEAR (now()) AND MONTH(BFFCHV) = month(now()) and f.BFOPER not in (405,410) and f.BFESTA in (7,10)"
    qry = QueryConsult(db2_engine, query, nombre=["actual"])
    return qry.obtener_datos()


def obtener_variacion_colocacion_banca_tipo(filtros: dict = None):
    tipo_banca = str(filtros.get("tipo_banca"))
    anterior = filtros.get("anterior")
    debito = filtros.get("debito")
    if debito == 1:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (601) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (600) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (606) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (605) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) = year(now()) AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia + \
                "%' and f.BFOPER not in (405,410) and f.BFESTA in (7,10) and f.BFTIP = 'A';"
    elif debito == 2:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (601) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (600) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (606) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (605) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) =  year(now()) -"+str(anterior)+" AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia + \
                "%' and f.BFOPER not in (405,410) and f.BFESTA in (7,10) and f.BFTIP = 'A';"
    elif anterior:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (201) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (200) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (305) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (202, 205) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) = year(now()) -"+str(anterior)+" AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia + \
                "%' and f.BFOPER not in (405,410) and f.BFESTA in (7,10) and f.BFTIP = 'A';"
    else:
        query = "SELECT SUM(CASE WHEN f.BFOPER IN (401) THEN f.BFSOLI ELSE 0 END) AS descuento_cheques, "\
            "SUM(CASE WHEN f.BFOPER IN (201) THEN f.BFSOLI ELSE 0 END) AS nuevos_int, "\
            "SUM(CASE WHEN f.BFOPER IN (200) THEN f.BFSOLI ELSE 0 END) AS nuevos_met, "\
            "SUM(CASE WHEN f.BFOPER IN (305) THEN f.BFSOLI ELSE 0 END) AS recurr_int, "\
            "SUM(CASE WHEN  f.BFOPER IN (202, 205) THEN f.BFSOLI ELSE 0 END) AS recurr_met "\
            "FROM DB2ADMIN.FSD0122 f JOIN DB2ADMIN.FSTFRANLEV l ON f.BFSUCU = l.FRSUC "\
            "WHERE f.BFAGEN IN ("+tipo_banca+") AND year(f.BFFCHV) = year(now()) AND month(f.BFFCHV) = month(now()) "\
            "AND l.FRDIRSUC LIKE '"+franquicia + \
                "%' and f.BFOPER not in (405,410) and f.BFESTA in (7,10) and f.BFTIP = 'A';"
    qry = QueryConsult(db2_engine, query, variacion=True)
    return qry.obtener_datos()
