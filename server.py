import json
import uvicorn
from starlette.responses import JSONResponse

from db_operations import DBOperations
import asyncio
import fastapi


db = DBOperations()

app = fastapi.FastAPI()
tables = [f"data_{x}" for x in range(1, 4)]


@app.get('/tables')
async def get_data():
    data = await fetch_tables()

    data = list(filter(lambda x: x is not None, data))

    if data is None or data == []:
        return JSONResponse([])

    if isinstance(data[0], list):
        data = [row for table in data for row in table if row is not None]

    data.sort(key=lambda x: x[0])
    data = [
        {
            'id': id_,
            'name': name
        } for id_, name in data
    ]
    return JSONResponse(data)


async def fetch_tables():
    tasks = [db.execute_dql(f"SELECT * FROM {table};") for table in tables]
    tasks = [asyncio.create_task(task) for task in tasks]
    executed, pending = await asyncio.wait(tasks, timeout=2)

    out = [await task for task in executed]
    [task.cancel() for task in pending]  # Cancel timed out tasks

    return out


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
