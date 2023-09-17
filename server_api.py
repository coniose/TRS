from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "fala ae"}


@app.get("/users")
async def users_route():
    return {"message": "hello user"}
