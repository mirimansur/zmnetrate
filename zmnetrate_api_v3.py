from fastapi import FastAPI, Query
from fastapi.responses import Response
import json
import zmnetrate_v3   # import your core logic

app = FastAPI(title="Zoom NetRate API v3")

@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.get("/zmnetrate_v3")
def netrate(
    calling_number: str = Query(...),
    called_number: str = Query(...),
    carrier: str = Query(...)
):
    try:
        result = zmnetrate_v3.find_best_vendors(calling_number, called_number, carrier)
        return Response(content=json.dumps(result, indent=2), media_type="application/json")
    except Exception as e:
        return Response(
            content=json.dumps({"error": "Internal server error", "details": str(e)}, indent=2),
            media_type="application/json",
            status_code=500
        )
