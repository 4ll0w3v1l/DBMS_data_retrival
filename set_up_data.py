import warnings

from db_operations import DBOperations
import asyncio

db = DBOperations()


def create_data(start, in_, pad, total):
    """
    :param start: starting point of data
    :param in_: [Xstart1, Xstart2 ... XstartIn_]
    :param pad: [Xstart1, Xstart2 ... XstartIn_, Xpad1, Xpad2 ... XpadIn_]
    :param total: how many rows to create
    """
    out = []
    for i in range(1, total + 1):
        data = ([f"({x}, 'data_{x}')" for x in range(start, start + in_)] +
                [f"({x}, 'data_{x}')" for x in range(pad + start, pad + start + in_)])
        start += in_
        out.append(','.join(data))
    return out


async def fill_indices(data):
    for i in range(len(data)):
        try:
            await db.execute_ddl(f"DROP TABLE IF EXISTS data_{i + 1};")
            db.create_table(f"data_{i + 1}")
            await db.execute_ddl(f"INSERT INTO data_{i+1} VALUES {data[i]};")
        except Exception as e:
            if 'Connect call failed' in str(e):
                warnings.warn("Check if PostgreSQL server is up and running")
            else:
                print(e)



if __name__ == "__main__":
    data = create_data(1, 10, 30, 3)
    asyncio.run(fill_indices(data))

