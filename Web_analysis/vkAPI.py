from time import perf_counter, sleep
import multiprocessing as mp

import asyncio
import requests
import nest_asyncio
import pandas as pd

from aiohttp import ClientSession
from config import VK_TOKENS

nest_asyncio.apply()
def cutter(lst: list, n): return [lst[i:i + n] for i in range(0, len(lst), n)]


async def execute_request(session, req, access_token):
    url = "https://api.vk.com/method/execute?"
    req = ",".join(req)
    data = dict(code=f'return[{req}];', access_token=access_token, v='5.131')
    async with session.post(url, data=data) as resp:
        response = await resp.json()
        if 'error' in response:
            sleep(0.5)
            return execute_request(session, req, access_token)
        return response


async def process_execute(query: list, token):
    async with ClientSession() as session:
        tasks = []
        for part in query:
            tasks.append(asyncio.ensure_future(execute_request(session, part, token)))
        return await asyncio.gather(*tasks)


def get_group_members(group_id: int, tokens: list):
    start_time = perf_counter()
    request = f"https://api.vk.com/method/groups.getMembers?group_id={target_group_id}&access_token={tokens[1]}&v=5.131"
    user_count = requests.get(request).json()['response']['count']
    offset_count = user_count // 1000 + 1 if user_count % 1000 != 0 else user_count // 1000
    user_set = set()
    query_list = [f"API.groups.getMembers({{'group_id':{group_id},'offset':{offset}}})"
                  for offset in range(0, offset_count * 1000, 1000)]
    query_list = cutter(query_list, 25)
    response = asyncio.run(process_execute(query_list, tokens[1]))
    for data in response:
        for items in data['response']:
            user_set.update(items['items'])
    print(f"group members collected for {perf_counter() - start_time} sec")
    return list(user_set)


def process_data(group_info: dict):
    if 'deactivated' in group_info or group_info['is_closed'] != 0:
        return group_info['id'], group_info['name'], -1
    if 'members_count' not in group_info:
        return group_info['id'], group_info['name'], -2
    return group_info['id'], group_info['name'], group_info['members_count']


def get_users_groups_members_count(group_id: int, tokens: list):
    start_time = perf_counter()
    user_list = get_group_members(group_id, tokens)
    query_list = [f"API.groups.get({{'user_id':{user},'extended':1,'fields':'members_count'}})"
                  for user in user_list]
    query_list = cutter(query_list, 25)
    groups = []
    response = asyncio.run(process_execute(query_list, tokens[1]))
    print(f"async task completed for {round(perf_counter() - start_time, 3)} sec")
    for data in response:
        for user in data['response']:
            if not user:
                continue
            for item in user['items']:
                groups.append(item)
    print(f"groups collected for {round(perf_counter() - start_time, 3)} sec")
    pool = mp.Pool(mp.cpu_count())
    mp_results = pool.map(process_data, [item for item in groups])
    pool.close()
    print(f"multiprocess task completed for {round(perf_counter() - start_time, 3)} sec")
    groups.clear()
    pd.DataFrame(mp_results, columns=['id', 'name', 'members_count']).set_index('id').to_csv("groups_member_count.csv")
    mp_results.clear()
    print(f"all task completed for {round(perf_counter() - start_time, 3)} sec")
    return groups


if __name__ == "__main__":
    token_list = list(VK_TOKENS.values())
    target_group_id = 197217619
    get_users_groups_members_count(target_group_id, token_list)
