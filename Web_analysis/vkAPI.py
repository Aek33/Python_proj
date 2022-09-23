from time import perf_counter
import re
from datetime import date, datetime
import asyncio
import requests
import nest_asyncio

from aiohttp import ClientSession
from database import PostgresDB
from config import VK_TOKENS

nest_asyncio.apply()
db = PostgresDB()


def cutter(lst: list, n: int) -> list: return [lst[i:i + n] for i in range(0, len(lst), n)]


def offset_count(count: int) -> int: return count // 1000 + 1 if count % 1000 != 0 else count // 1000


def get_age(bdate: str):
    if re.match("^[0-9]{1,2}.[0-9]{1,2}.[0-9]{4}$", bdate) is not None:
        _date = datetime.strptime(bdate, "%d.%m.%Y")
        return date.today().year - _date.year - ((date.today().month, date.today().day) < (_date.month, _date.day))
    else:
        return -1


def process_user_info(user_info: list):
    d = user_info[0]
    user_id = d['id']
    active = False if 'deactivated' in d else True
    age = get_age(d['bdate']) if 'bdate' in d else -1
    sex = d['sex'] if 'sex' in d else -1
    if 'counters' in d:
        friends_count = d['counters']['friends'] if 'friends' in d['counters'] else -1
        groups_count = d['counters']['groups'] if 'groups' in d['counters'] else 0
    else:
        friends_count = -1
        groups_count = -1
    if 'is_closed' in d:
        is_closed = d['is_closed']
    else:
        is_closed = True
    country = d['country']['title'].replace("'", "") if 'country' in d else "unknown"
    city = d['city']['title'].replace("'", "") if 'city' in d else "unknown"
    first_name = d['first_name'].replace("'", "") if 'first_name' in d else "unknown"
    last_name = d['last_name'].replace("'", "") if 'last_name' in d else "unknown"
    return user_id, active, age, sex, friends_count, groups_count, country, city, first_name, last_name, is_closed


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

    async def process_execute(self, query: list):
        query = cutter(query, 25)
        async with ClientSession() as session:
            tasks = []
            for part in query:
                token = self.token_list.pop(0)
                tasks.append(asyncio.ensure_future(self.execute_request(session, part, token)))
                self.token_list.append(token)
            return await asyncio.gather(*tasks)

    def get_group_members(self):
        request = f"https://api.vk.com/method/groups.getMembers?group_id={self.group_id}" \
                  f"&access_token={self.token_list[0]}&v=5.131"
        user_count = requests.get(request).json()
        if 'error' in user_count:
            raise Exception(f"VK API error\n{user_count}")
        user_count = requests.get(request).json()['response']['count']
        offset = offset_count(user_count)
        query_list = [f"API.groups.getMembers({{'group_id':{self.group_id},'offset':{offset}}})"
                      for offset in range(0, offset * 1000, 1000)]
        response = asyncio.run(self.process_execute(query_list))
        user_set = set()
        for item in response[0]:
            user_set.update(item['items'])
        return list(user_set)

    def get_members_info(self):
        user_list = self.get_group_members()
        print(f"users id collected completed for {round(perf_counter() - self.timer, 3)} sec")
        query_list = [f"API.users.get({{'user_ids':{user}, 'fields':'bdate, sex, counters, city, country'}})"
                      for user in user_list]
        response = asyncio.run(self.process_execute(query_list))
        print(f"async task completed for {round(perf_counter() - self.timer, 3)} sec")
        for part in response:
            for user in part:
                if user:
                    data = process_user_info(user)
                    q = f"""
                    INSERT INTO vk_parser.group_members
                    (member_id, active, age, sex, friends_count, groups_count, country,
                     city, first_name, last_name, is_closed) VALUES {data}"""
                    db.modify(q)
        print(f"all tasks completed for {round(perf_counter() - self.timer, 3)} sec")


if __name__ == "__main__":
    target_group_id = 197217619
    hololive = Investigator(target_group_id, VK_TOKENS)
    hololive.get_members_info()
