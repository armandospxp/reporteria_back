from datetime import date
from pydantic import BaseModel


''' Model Schema Using Pydantic '''


# class User(Base):
#     username: str
#     password: str


class colocacion(BaseModel()):
    ANALISTA: str
    APORTA_IPS: str
    APROB_LINEA: str
    APROB_SCORING: str
    ATRASO: int
    BANCA: int
    BANCA_GRUPO:str
    CALIFICACION: str
    CANAL: str
    CANT_CONDICIONADOS:int
    CANT_CUOTAS:int
    CANT_EXCEPCIONES:int
    CIUDAD_LABORAL:str
    CIUDAD_PARTICULAR:str
    CUENTA:int
    DEPARTAMENTO_LAB:str
    DESEMBOLSO_INSITU:str
    DOCUMENTO:int
    EMPRESA:str
    ESTADO:int
    EXCEP_DESEMBOLSO:str
    EXCEP_PROCESO:str
    EXCEP_REP_APROB:str
    FAJA:str
    FECHA_INGRESO:date
    FECHA_VTA:str
    FECHAOPE:date
    FECHAOPE_GRUPO:str
    FRANQUICIA:str
    FRANQUICIA_GRUPO:str
    HORA_APROBACION:str
    HORA_DESEMBOLSO:str
    MARCA_PREAPROBADO:str
    MEDIO:int
    MEDIO_GRUPO:str
    NRO_OPERACION:int
    PLAZO_OPE_CONSOLIDADA:int
    RANGO_CAPITAL:str
    RANGO_EDAD:str 
    RANGO_SCORE:str
    RECHAZO_CARGA:int
    SCORE:int
    SCORING:str
    SUCURSAL:str
    SUCURSAL_DESEMBOLSO:str
    SUPERVISOR:str
    SUPERVISOR_NOMBRE:str
    tiempo_desembolso:str
    TIPO:int
    TIPO_DESEMBOLSO:str
    TIPO_APROBACION:str
    TIPO_PROCESO:str
    TIPO_SUCURSAL:str 
    tramos_monto:str
    VENDEDOR:int
    VENDEDOR_NOMBRE:str
    VIA_INGRESO:str
    ANALYTICS_CLUSTER:str
    CAPITAL:int
    CAPITAL_CONSOLIDADO:int
    CAPITAL_VTA:str
    DIASPROCESO:int 
    INGRESO_CLIENTE_DESEMBOLSO:int 
    INTERES_CONSOLIDADO:int
    MONTO_CONSOLIDADO:int
    MONTO_DESEMBOLSADO:int
    MONTO_LIQUIDO:int
    VALOR_PAGARE:int

    class Config:
        orm_mode = True
