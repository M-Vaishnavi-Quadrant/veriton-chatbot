from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "API is working!"}

@app.get("/test")
def test_api():
    return {
        "status": "success",
        "data": "Test endpoint is working fine"
    }
