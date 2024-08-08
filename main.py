import datetime
import asyncio
import aiohttp
from more_itertools import chunked

from models import init_orm, Session, SwapiCharacter


MAX_REQUESTS = 5


async def get_field(session, url):
    response = await session.get(url)
    json_data = await response.json()
    if json_data.get('title'):
        return json_data.get('title')
    return json_data.get('name')


async def get_people(session, person_id):
    response = await session.get(f"https://swapi.dev/api/people/{person_id}/")
    json_data = await response.json()
    if not json_data.get('detail'):
        if json_data.get('edited'):
            json_data.pop('edited')
        json_data.pop('url')
        json_data.pop('created')
        json_data['participant_id'] = person_id
        json_data['films'] = ', '.join([await get_field(session, url) for url in json_data.get('films')])
        json_data['species'] = ', '.join([await get_field(session, url) for url in json_data.get('species')])
        json_data['starships'] = ', '.join([await get_field(session, url) for url in json_data.get('starships')])
        json_data['vehicles'] = ', '.join([await get_field(session, url) for url in json_data.get('vehicles')])
        json_data['homeworld'] = await get_field(session, json_data.get('homeworld'))
        return json_data


async def insert_data(characters):
    async with Session() as db_session:
        orm_models = [SwapiCharacter(**character) for character in characters if character is not None]
        db_session.add_all(orm_models)
        await db_session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as web_session:
        for people_ids in chunked(range(1, 101), MAX_REQUESTS):
            coros = [get_people(web_session, i) for i in people_ids]
            result = await asyncio.gather(*coros)
            asyncio.create_task(insert_data(result))
            print('DONE')

    all_tasks = asyncio.all_tasks()
    main_task = asyncio.current_task()
    all_tasks.remove(main_task)
    await asyncio.gather(*all_tasks)


if __name__ == '__main__':
    start = datetime.datetime.now()
    # asyncio.run(main())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()
    print(datetime.datetime.now() - start)
