from fastapi import FastAPI
from .database import database
from .routes import api_route

app = FastAPI()


app.include_router(api_route, prefix="/api/reporteria", tags=["reporteria"])

@app.get("/")
def read_root():
    return {"Hello": "World"}
if __name__ == '__main__':
    database.iniciar_base()
    uvicorn.run("main:app", host="127.0.0.1", port=8000)