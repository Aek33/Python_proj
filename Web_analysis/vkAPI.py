from time import perf_counter
import multiprocessing as mp

import asyncio
import requests
import nest_asyncio
import pandas as pd

from aiohttp import ClientSession
from config import VK_TOKENS

nest_asyncio.apply()
def cutter(lst: list, n): return [lst[i:i + n] for i in range(0, len(lst), n)]


async def execute_request(session, req, access_token, sem):
    url = "https://api.vk.com/method/execute?"
    req = ",".join(req)
    data = dict(code=f'return[{req}];', access_token=access_token, v='5.131')
    async with sem:
        async with session.post(url, data=data) as resp:
            response = await resp.json()
            if 'error' in response:
                await asyncio.sleep(0.5)
                return await execute_request(session, req, access_token, sem)
            return response


async def process_execute(query: list, tokens: list):
    limit = asyncio.Semaphore(3)
    async with ClientSession() as session:
        tasks = []
        for part in query:
            token = tokens.pop(0)
            tasks.append(asyncio.ensure_future(execute_request(session, part, token, limit)))
            tokens.append(token)
        return await asyncio.gather(*tasks)


def get_group_members(group_id: int, tokens: list):
    start_time = perf_counter()
    request = f"https://api.vk.com/method/groups.getMembers?group_id={group_id}&access_token={tokens[0]}&v=5.131"
    user_count = requests.get(request).json()
    if 'error' in user_count:
        raise Exception(f"VK API error\n{user_count}")
    user_count = requests.get(request).json()['response']['count']
    offset_count = user_count // 1000 + 1 if user_count % 1000 != 0 else user_count // 1000
    user_set = set()
    query_list = [f"API.groups.getMembers({{'group_id':{group_id},'offset':{offset}}})"
                  for offset in range(0, offset_count * 1000, 1000)]
    query_list = cutter(query_list, 25)
    response = asyncio.run(process_execute(query_list, tokens))
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
    groups_list = []
    response = asyncio.run(process_execute(query_list, tokens))
    print(f"async task completed for {round(perf_counter() - start_time, 3)} sec")
    for data in response:
        for user in data['response']:
            if not user:
                continue
            for groups in user['items']:
                for group in groups:
                    if 'deactivated' in group or group['is_closed'] != 0:
                        groups_list.append((group['id'], group['name'], -1))
                    elif 'members_count' not in group:
                        groups_list.append((group['id'], group['name'], -2))
                    else:
                        groups_list.append((group['id'], group['name'], group['members_count']))
    df = pd.DataFrame(groups_list, columns=['id', 'name', 'members_count']).set_index('id').to_csv("groups_member_count.csv")
    print(f"all task completed for {round(perf_counter() - start_time, 3)} sec")
    return df


if __name__ == "__main__":
    t = VK_TOKENS
    target_group_id = 197217619
    token = "95ec26ebf54f29da7f9e6508d78a96f065b9798da64e7ae5e0baa427d7c7ce7d245c951e0157e712510ad"
    request = f"https://api.vk.com/method/groups.getMembers?group_id={target_group_id}&access_token={token}&v=5.131"
    print(requests.get(request).json())