from time import perf_counter
import multiprocessing as mp

import asyncio
import requests
import nest_asyncio
import pandas as pd

from aiohttp import ClientSession
from config import VK_TOKENS

nest_asyncio.apply()


def cutter(lst: list, n: int) -> list: return [lst[i:i + n] for i in range(0, len(lst), n)]


def offset_count(count: int) -> int: return count // 1000 + 1 if count % 1000 != 0 else count // 1000


class Investigator:
    def __init__(self, group_id: int, token_list: list):
        self.group_id = group_id
        self.token_list = token_list
        self.timer = perf_counter()
        self.execute_url = "https://api.vk.com/method/execute?"
        self.semaphore = asyncio.Semaphore(len(self.token_list) * 3)

    async def execute_request(self, session, req, access_token):
        req = ",".join(req)
        data = {'code': f'return[{req}];', 'access_token': access_token, 'v': '5.131'}
        async with self.semaphore:
            async with session.post(self.execute_url, data=data) as resp:
                response = await resp.json()
                if 'error' in response:
                    raise Exception(f"VK API error\n{response}")
                return response['response']

    async def process_execute(self, query: list, tokens: list):
        query = cutter(query, 25)
        async with ClientSession() as session:
            tasks = []
            for part in query:
                token = tokens.pop(0)
                tasks.append(asyncio.ensure_future(self.execute_request(session, part, token)))
                tokens.append(token)
            return await asyncio.gather(*tasks)

    def get_group_members(self, group_id: int, tokens: list):
        request = f"https://api.vk.com/method/groups.getMembers?group_id={group_id}&access_token={tokens[0]}&v=5.131"
        user_count = requests.get(request).json()
        if 'error' in user_count:
            raise Exception(f"VK API error\n{user_count}")
        user_count = requests.get(request).json()['response']['count']
        offset = offset_count(user_count)
        user_set = set()
        query_list = [f"API.groups.getMembers({{'group_id':{group_id},'offset':{offset}}})"
                      for offset in range(0, offset * 1000, 1000)]
        response = asyncio.run(self.process_execute(query_list, tokens))
        for data in response:
            for items in data['response']:
                user_set.update(items['items'])
        return list(user_set)

    def get_users_groups_members_count(self, group_id: int, tokens: list):
        user_list = self.get_group_members(group_id, tokens)
        query_list = [f"API.groups.get({{'user_id':{user},'extended':1,'fields':'members_count'}})"
                      for user in user_list][:50]
        groups_list = []
        response = asyncio.run(self.process_execute(query_list, tokens))
        print(f"async task completed for {round(perf_counter() - self.timer, 3)} sec")
        for data in response:
            for user in data['response']:
                if not user:
                    continue
                for group in user['items']:
                    if 'deactivated' in group or group['is_closed'] != 0:
                        groups_list.append((group['id'], group['name'], -1))
                    elif 'members_count' not in group:
                        groups_list.append((group['id'], group['name'], -2))
                    else:
                        groups_list.append((group['id'], group['name'], group['members_count']))
        df = pd.DataFrame(groups_list, columns=['id', 'name', 'members_count']).set_index('id').to_csv(
            "groups_member_count.csv")
        print(f"all task completed for {round(perf_counter() - self.timer, 3)} sec")
        return df


if __name__ == "__main__":
    # TLIST = VK_TOKENS
    # target_group_id = 197217619
    # get_users_groups_members_count(target_group_id, TLIST)
    q = ", ".join([f"API.users.get({{'user_ids':{443331523}, 'fields':'city, country'}})", f"API.users.get({{'user_ids':{444037492}, 'fields':'city, country'}})"])

    data = dict(code=f'return [{q}];', access_token=VK_TOKENS[2], v='5.131')
    print(requests.post("https://api.vk.com/method/execute?", data=data).json())

