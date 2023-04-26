from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData

# SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
SQLALCHEMY_DATABASE_URL = "mysql+mysqlconnector://root:@127.0.0.1:3306/colocacion"


metadata = sqlalchemy.MetaData()

engine = create_engine(
    SQLALCHEMY_DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


Base = declarative_base()

def iniciar_base()->None:
    metadata = MetaData()
    metadata.create_all(engine)