from fastapi import FastAPI
from database.database import engine as database


''' FastAPI CONFIGURATION '''
app = FastAPI(title="FastAPI reporteria")



''' APP EVENT SETTING'''
@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()