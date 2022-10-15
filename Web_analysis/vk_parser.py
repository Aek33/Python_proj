from json import dump
from re import match
from time import perf_counter
from datetime import date, datetime
from collections import Counter

import asyncio
import nest_asyncio
import requests
import pandas as pd
from aiohttp import ClientSession

from database import Database
from config import VK_TOKENS

nest_asyncio.apply()


def offset_count(count: int, offset: int) -> int: return count // offset + (count % offset != 0)


def get_age(bdate: str) -> int:
    if match("^[0-9]{1,2}.[0-9]{1,2}.[0-9]{4}$", bdate) is not None:
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

    return group_id, active, name


class VKAPIError(Exception):
    """Exception class for handling VK API exception.
    Rises when get 'error' instead of 'response' in response json from request to VK API"""
    def __init__(self, token, message):
        self.token = token
        self.message = message

    def __str__(self):
        return f'VK API Error in response!\n{self.message}\nwith token\n{self.token}'


class VKParser:
    def __init__(self, group_id: int, token_list: list[str], db: Database):
        self.db = db
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
                    raise VKAPIError(access_token, response)
                return response['response']

    async def _process_execute(self, query: list[str]):
        query = [query[i:i + 25] for i in range(0, len(query), 25)]

        async with ClientSession() as session:
            tasks = []
            for part in query:
                token = self.token_list.pop(0)
                tasks.append(asyncio.ensure_future(self._execute_request(session, part, token)))
                self.token_list.append(token)
            return await asyncio.gather(*tasks)

    def get_group_info(self) -> None:
        """Writing target group metadata to json file"""
        request: str = f"https://api.vk.com/method/groups.getById?group_id={self.group_id}" \
                       f"&fields=description,members_count&access_token={self.token_list[0]}&v=5.131"
        response: dict = requests.get(request).json()

        if 'error' in response:
            raise VKAPIError(self.token_list[0], response)

        with open(f'{response["response"][0]["screen_name"]}.json', 'w') as f:
            dump(response["response"][0], f)

    def get_group_members(self):
        """Returns users id of the target group"""
        request = f"https://api.vk.com/method/groups.getMembers?group_id={self.group_id}" \
                  f"&access_token={self.token_list[0]}&v=5.131"
        user_count: dict = requests.get(request).json()
        if 'error' in user_count:
            raise VKAPIError(self.token_list[0], user_count)

        user_count: int = requests.get(request).json()['response']['count']
        offset = offset_count(user_count, 1000)

        query_list = [f"API.groups.getMembers({{'group_id':{self.group_id},'offset':{offset * 1000}}})"
                      for offset in range(offset)]

        response = asyncio.run(self._process_execute(query_list))

        user_set = set()
        for item in response[0]:
            user_set.update(item['items'])
        return list(user_set)

    def get_members_info(self) -> None:
        """Collecting and inserting into database target group users attributes"""
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
                    self.db.insert(q)
        print(f"all tasks completed for {round(perf_counter() - self.timer, 3)} sec")

    def get_members_groups(self) -> None:
        """Collecting and inserting into database list of all groups, which include target group members.
        Each row include group id, group name, group status, and popularity among target group users"""
        print(f"get_users_groups_info started in {round(perf_counter() - self.timer, 3)} sec")

        q_select = "SELECT member_id, groups_count FROM vk_parser.group_users WHERE active=TRUE and is_closed=FALSE"
        user_list = self.db.select(q_select)

        query_list = []
        for user in user_list:
            offset = offset_count(user[1], 500)
            temp_list = [f"API.groups.get({{'user_id':{user[0]}, 'extended':1, 'offset':{offset * 500}, 'count':500}})"
                         for offset in range(offset)]
            query_list += temp_list
        response = asyncio.run(self._process_execute(query_list))
        print(f"async task completed for {round(perf_counter() - self.timer, 3)} sec")

        groups_ids, groups_access, groups_names = [], [], []
        for user_groups in response:
            for group_list in user_groups:
                if group_list:
                    for items in group_list['items']:
                        data = process_group_info(items)
                        groups_ids.append(data[0])
                        groups_access.append(data[1])
                        groups_names.append(data[2])

        dataframe = pd.DataFrame({'group_id': groups_ids, 'access': groups_access, 'group_name': groups_names})
        groups_id_count = dict(Counter(dataframe['group_id']))
        dataframe = dataframe.drop_duplicates()
        dataframe['popularity'] = dataframe['group_id'].map(groups_id_count)
        data_list = list(dataframe.to_records(index=False))

        for record in data_list:
            self.db.insert(f"INSERT INTO vk_parser.users_groups VALUES {record}")
        print(f"all tasks completed for {round(perf_counter() - self.timer, 3)} sec")

    def get_members_friends(self):
        """Collecting and inserting into database target group users friend ids lists"""
        print(f"get_members_friends started in {round(perf_counter() - self.timer, 3)} sec")

        q_select = "SELECT member_id, friends_count " \
                   "FROM vk_parser.group_users " \
                   "WHERE active=TRUE and is_closed=FALSE and friends_count > 0"
        user_list = self.db.select(q_select)

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
            self.db.insert(f"INSERT INTO vk_parser.users_friends VALUES ({item[0]}, '{set(item[1])}')"
                           f"ON CONFLICT ON CONSTRAINT users_friends_pk DO NOTHING")
        print(f"all tasks completed for {round(perf_counter() - self.timer, 3)} sec")

    def __del__(self):
        self.db.close()


if __name__ == "__main__":
    target_group_id = 197217619
    postgresql_db = Database()
    hololive_pics = VKParser(target_group_id, VK_TOKENS, postgresql_db)
    # hololive_pics.get_group_info()
    # hololive_pics.get_members_info()
    hololive_pics.get_members_groups()
    # hololive_pics.get_members_friends()
