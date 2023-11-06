from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData

# SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
SQLALCHEMY_DATABASE_URL = "mysql+mysqlconnector://root:@127.0.0.1:3306/operaciones"

# DB2_DATABASE_URL = "db2:///?Server=10.1.253.100&Port=50000&User=armando&Password=Temporal2023&Database=BASE_NF"
# DB2_DATABASE_URL = "db2+ibm_db://armando:Temporal2023@10.1.253.100:50000/BASE_NF"
DB2_DATABASE_URL = "db2+ibm_db://armando:Armando$$2o23@10.1.253.161:50000/BASE_SS"


metadata = MetaData()

engine = create_engine(
    SQLALCHEMY_DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db2_engine = create_engine((DB2_DATABASE_URL))


Base = declarative_base()

def iniciar_base()->None:
    metadata = MetaData()
    metadata.create_all(engine)