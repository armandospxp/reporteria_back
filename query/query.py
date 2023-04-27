# from sqlalchemy.orm import Session
from database.database import engine
from sqlalchemy import text
from sqlalchemy.ext.serializer import loads, dumps
from fastapi.encoders import jsonable_encoder
import pdb

conn = engine.connect()

def obtener_cantidad_operaciones(franquicia=None):
    franquicia = "BOSAMAZ"
    query = "select rtrim(c.SUCURSAL) as SUCURSAL, COUNT(*) as CANTIDAD from operaciones.colocacion c where c.FRANQUICIA like '%"+ franquicia +"%'GROUP BY c.SUCURSAL order by CANTIDAD desc;"
    datos = conn.execute(text(query))
    # pdb.set_trace()
    #results = [dic(row) for row in datos]
    results = [dict([(r) for r in datos.fetchall()])]
    return results

def obtener_suma_monto_operaciones(franquicia=None):
    franquicia = "BOSAMAZ"
    query ="select rtrim(c.SUCURSAL) as SUCURSAL, sum(c.MONTO_CONSOLIDADO) as MONTO_CONSOLIDADO from operaciones.colocacion c where c.FRANQUICIA like '%"+ franquicia +"'GROUP BY c.SUCURSAL order by MONTO_CONSOLIDADO desc;"
    datos = conn.execute(text(query))
    results = [dict([(r) for r in datos.fetchall()])]
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