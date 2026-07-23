from fastapi import FastAPI, Request

app = FastAPI()
@app.get("/hello/{name}")
def say_hello(name: str):
    return {"message": f"Hello, {name}!"}



@app.post("/echo")
async def echo(request: Request):
    data = await request.body()
    return {"echo": data.decode()}