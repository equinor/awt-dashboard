# from datetime import date
import uvicorn
from fastapi import FastAPI
import ec_interface as ec
import json

# from fastapi import FastAPI, Response
# from fastapi.encoders import jsonable_encoder
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel
# from typing import Optional


app = FastAPI()


@app.get("/")
async def get_welltests():
    test_device = 'TD_TRB_TEST1'
    date_start = "01.01.2022"
    date_end = "01.02.2022"
    ec_dict = {
    "ec_db_hostname": "znw-db1006.statoil.no",
    "ec_db_port": 10001,
    # "ec_db_service": "U168E",
    "ec_db_service": "U168E",
}
    try:
        # df = ec.get_full_welltest_join(ec_dict, test_device, date_start, date_end)
        # data = df.to_json()
        fn = "welltests.json"
        with open(fn) as f:
            data = json.load(f)
        return data
    except Exception as e:
        data = {'message': f'{e}'}
    return data
    
    
    # return {"Hello": "World"}

if __name__ == "__main__":
    uvicorn.run('main:app', host="127.0.0.1", port=5000,  debug=True, reload=True)