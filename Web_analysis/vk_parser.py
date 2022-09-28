import re
import asyncio

import nest_asyncio
import requests
import pandas as pd

from time import perf_counter
from datetime import date, datetime
from aiohttp import ClientSession
from database import PostgresDB
from config import VK_TOKENS

nest_asyncio.apply()
db = PostgresDB()


def cutter(lst: list[str], n: int) -> list[list]: return [lst[i:i + n] for i in range(0, len(lst), n)]


def offset_count(count: int, div: int) -> int: return count // div + 1 if count % div != 0 else count // div


def get_age(bdate: str) -> int:
    if re.match("^[0-9]{1,2}.[0-9]{1,2}.[0-9]{4}$", bdate) is not None:
        dtime_date: datetime = datetime.strptime(bdate, "%d.%m.%Y")
        age: int = date.today().year - dtime_date.year - ((date.today().month, date.today().day)
                                                          < (dtime_date.month, dtime_date.day))
        return age
    return -1


def process_user_info(users_info: list) -> tuple:
    d: dict = users_info[0]

    user_id: int = d['id']

    active: bool = False if 'deactivated' in d else True

    age: int = get_age(d['bdate']) if 'bdate' in d else -1

    sex: int = d['sex'] if 'sex' in d else -1

    if 'counters' in d:
        friends_count = d['counters']['friends'] if 'friends' in d['counters'] else -1
        pages_count = d['counters']['pages'] if 'pages' in d['counters'] else 0
        groups_count = d['counters']['groups'] if 'groups' in d['counters'] else 0
        groups_count += pages_count
    else:
        friends_count = -1
        groups_count = -1

    if 'is_closed' in d:
        is_closed = d['is_closed']
    else:
        is_closed = True

    country: str = d['country']['title'].replace("'", "") if 'country' in d else "unknown"

    city: str = d['city']['title'].replace("'", "") if 'city' in d else "unknown"

    first_name: str = d['first_name'].replace("'", "") if 'first_name' in d else "unknown"

    last_name: str = d['last_name'].replace("'", "") if 'last_name' in d else "unknown"

    return user_id, active, is_closed, first_name, last_name, age, sex, friends_count, groups_count, country, city,


def process_group_info(group_info: dict) -> tuple:
    active: bool = False if 'deactivated' in group_info else True
    group_id: int = group_info['id']
    name: str = group_info['name'].replace("'", "") if 'name' in group_info else "unknown_group_name"
    members_count: int = group_info['members_count'] if 'members_count' in group_info else -1
    return group_id, active, name, members_count


class Investigator:

    def __init__(self, group_id: int, token_list: list[str]):
        self.group_id = group_id
        self.token_list = token_list
        self.timer = perf_counter()
        self.execute_url = "https://api.vk.com/method/execute?"
        self.semaphore = asyncio.Semaphore(len(self.token_list) * 3)

    async def _execute_request(self, session: ClientSession, req: list, access_token: str) -> dict:
        req = ",".join(req)
        data = {'code': f'return[{req}];', 'access_token': access_token, 'v': '5.131'}

        async with self.semaphore:
            async with session.post(self.execute_url, data=data) as resp:
                response: dict = await resp.json()
                if 'error' in response:
                    print(response)
                    print(access_token)
                    raise Exception(f"VK API error\n{response}")
                return response['response']

    async def _process_execute(self, query: list[str]):
        query = cutter(query, 25)

        async with ClientSession() as session:
            tasks = []
            for part in query:
                token = self.token_list.pop(0)
                tasks.append(asyncio.ensure_future(self._execute_request(session, part, token)))
                self.token_list.append(token)
            return await asyncio.gather(*tasks)

    def get_group_members(self):
        request = f"https://api.vk.com/method/groups.getMembers?group_id={self.group_id}" \
                  f"&access_token={self.token_list[0]}&v=5.131"
        user_count: dict = requests.get(request).json()
        if 'error' in user_count:
            raise Exception(f"VK API error\n{user_count}")

        user_count: int = requests.get(request).json()['response']['count']
        offset = offset_count(user_count, 1000)
        assert offset > 0

        query_list = [f"API.groups.getMembers({{'group_id':{self.group_id},'offset':{offset * 1000}}})"
                      for offset in range(offset)]

        response = asyncio.run(self._process_execute(query_list))

        user_set = set()
        for item in response[0]:
            user_set.update(item['items'])

        list(user_set)
        return list(user_set)

    def get_members_info(self) -> None:
        print(f"get_members_info started in {round(perf_counter() - self.timer, 3)} sec")

        user_list = self.get_group_members()
        print(f"users id collected for {round(perf_counter() - self.timer, 3)} sec")

        query_list = [f"API.users.get({{'user_ids':{user}, 'fields':'bdate, sex, counters, city, country'}})"
                      for user in user_list]
        response = asyncio.run(self._process_execute(query_list))
        print(f"async task completed for {round(perf_counter() - self.timer, 3)} sec")

        for part in response:
            for user in part:
                if user:
                    data = process_user_info(user)
                    q = f"INSERT INTO vk_parser.group_users VALUES {data}"
                    db.modify(q)
        print(f"all tasks completed for {round(perf_counter() - self.timer, 3)} sec")

    def get_members_groups(self) -> None:
        print(f"get_users_groups_info started in {round(perf_counter() - self.timer, 3)} sec")

        q_select = "SELECT member_id, groups_count FROM vk_parser.group_users WHERE active=TRUE and is_closed=FALSE"
        user_list = db.select(q_select)

        query_list = []
        for user in user_list:
            offset = offset_count(user[1], 500)
            assert offset > 0
            temp_list = [f"API.groups.get({{'user_id':{user[0]}, 'extended':1, 'fields':'members_count',"
                         f" 'offset':{offset * 500}, 'count':500}})" for offset in range(offset)]
            query_list += temp_list

        response = asyncio.run(self._process_execute(query_list))
        print(f"async task completed for {round(perf_counter() - self.timer, 3)} sec")

        for user_groups in response:
            for group_list in user_groups:
                if group_list:
                    for items in group_list['items']:
                        data = process_group_info(items)
                        q = f"INSERT INTO  vk_parser.users_groups " \
                            f"VALUES {data} ON CONFLICT ON CONSTRAINT users_groups_pk DO NOTHING;"
                        db.modify(q)

        print(f"all tasks completed for {round(perf_counter() - self.timer, 3)} sec")

    def get_members_friends(self):
        print(f"get_members_friends started in {round(perf_counter() - self.timer, 3)} sec")

        q_select = "SELECT member_id, friends_count " \
                   "FROM vk_parser.group_users " \
                   "WHERE active=TRUE and is_closed=FALSE and friends_count > 0"
        user_list = db.select(q_select)

        query_list = []
        user_accounting_list = []
        friends_accounting_list = []

        for user in user_list:
            offset = offset_count(user[1], 5000)
            for i in range(offset):
                user_accounting_list.append(user[0])
                query_list.append(f"API.friends.get({{'user_id':{user[0]}, 'offset':{i * 5000}}})")
        response = asyncio.run(self._process_execute(query_list))
        print(f"async task completed for {round(perf_counter() - self.timer, 3)} sec")

        count = -1
        for part in response:
            for item in part:
                count += 1
                if item and len(item['items']) != 0:
                    friends_accounting_list.append(item['items'])
                else:
                    user_error = user_accounting_list.pop(count)
                    count -= 1
                    print(user_error)

        data = pd.DataFrame({'user': user_accounting_list, 'friends': friends_accounting_list}) \
            .groupby(by=['user']).sum().to_dict()

        for item in data['friends'].items():
            db.modify(f"INSERT INTO vk_parser.users_friends VALUES ({item[0]}, '{set(item[1])}')"
                      f"ON CONFLICT ON CONSTRAINT users_friends_pk DO NOTHING")
        print(f"all tasks completed for {round(perf_counter() - self.timer, 3)} sec")


if __name__ == "__main__":
    target_group_id = 197217619
    hololive_pics = Investigator(target_group_id, VK_TOKENS)
    # hololive_pics.get_group_members()
    # hololive_pics.get_members_info()
    # hololive_pics.get_members_groups()
    # hololive_pics.get_members_friends()

