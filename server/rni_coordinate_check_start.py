import asyncio
import aiohttp
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv


load_dotenv(os.path.dirname(__file__) + '/.env')

HOST = os.getenv('DB_HOST')
SMD_USER = os.getenv('SMD_USER')
SMD_PWD = os.getenv('SMD_PWD')

async def fetch(session, url, route):
    try:
        async with session.post(url, params={'route': route}) as response:
            return  (response.status, route)
    except Exception as e:
        return f"Error route {route}"
    
async def submit_requests():
    engine = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{HOST}:1521/geodbbm")
    routes = engine.connect().execute(text('select distinct(linkid) from rni_2_2024'))

    tasks = []
    url = 'http://localhost:8000/bm/rni_rerun'
    
    async with aiohttp.ClientSession() as session:
        for route in routes.fetchall():
            # print(route[0])

            task = asyncio.create_task(fetch(session, url, route=route[0]))
            tasks.append(task)

            if len(tasks) > 7:
                responses = await asyncio.gather(*tasks)

                for response in responses:
                    if response[0] != 200:
                        print(response)
                
                tasks = []

        responses = await asyncio.gather(*tasks)

        for response in responses:
            print(response)

    return


if __name__ == '__main__':
    asyncio.run(submit_requests())
