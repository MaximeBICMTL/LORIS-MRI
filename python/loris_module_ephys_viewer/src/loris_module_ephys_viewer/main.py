from fastapi import FastAPI

api = FastAPI()


@api.get("/")
async def channels():
    return {"message": "Hello World"}
