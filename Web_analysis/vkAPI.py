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


class Investigator:
    def __init__(self, group_id: int, token_list: list):
        self.group_id = group_id
        self.token_list = token_list
        self.timer = perf_counter()

    async def execute_request(self, session, req, access_token, semaphore):
        url = "https://api.vk.com/method/execute?"
        req = ",".join(req)
        data = dict(code=f'return[{req}];', access_token=access_token, v='5.131')
        async with semaphore:
            async with session.post(url, data=data) as resp:
                response = await resp.json()
                if 'error' in response:
                    print(response)
                    print(access_token)
                    await asyncio.sleep(5)
                    return -1
                return response

    async def process_execute(self, query: list, tokens: list):
        sem = asyncio.Semaphore(9)
        async with ClientSession() as session:
            tasks = []
            for part in query:
                token = tokens.pop(0)
                tasks.append(asyncio.ensure_future(self.execute_request(session, part, token, sem)))
                tokens.append(token)
            return await asyncio.gather(*tasks)

    # def execute_request(req, access_token):
    #     url = "https://api.vk.com/method/execute?"
    #     req = ",".join(req)
    #     data = dict(code=f'return[{req}];', access_token=access_token, v='5.131')
    #     with requests.post(url, data=data) as resp:
    #         response = resp.json()
    #         if 'error' in response:
    #             print(response)
    #             return execute_request(req, access_token)
    #         return response
    #
    #
    # def process_execute(query: list, tokens: list):
    #     tasks = []
    #     for i in range(len(query)):
    #         print(i)
    #         token = tokens.pop(0)
    #         tasks.append(execute_request(query[i], token))
    #         tokens.append(token)
    #     return tasks

    def get_group_members(self, group_id: int, tokens: list):
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
        # response = process_execute(query_list, tokens)
        response = asyncio.run(self.process_execute(query_list, tokens))
        for data in response:
            for items in data['response']:
                user_set.update(items['items'])
        return list(user_set)

    def get_users_groups_members_count(self, group_id: int, tokens: list):

        user_list = self.get_group_members(group_id, tokens)
        query_list = [f"API.groups.get({{'user_id':{user},'extended':1,'fields':'members_count'}})"
                      for user in user_list][:50]
        query_list = cutter(query_list, 25)
        groups_list = []
        # response = process_execute(query_list, tokens)
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
    TLIST = VK_TOKENS
    # target_group_id = 197217619
    # get_users_groups_members_count(target_group_id, TLIST)
    q = f"https://api.vk.com/method/users.get?user_ids=116262185&fields=country,city&access_token={VK_TOKENS[0]}&v=5.131"
    print(requests.get(q).json())
