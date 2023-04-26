from ...database import engine as database


''' FastAPI CONFIGURATION '''
app = FastAPI(__name__,
              title="FastAPI reporteria",
              docs_url="/docs", redoc_url="/redocs"
)



''' APP EVENT SETTING'''
@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()