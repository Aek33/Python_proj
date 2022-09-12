from time import perf_counter
import re
from datetime import date, datetime
from collections import Counter
import multiprocessing as mp
import aiohttp
import asyncio
import vk_api
import pandas as pd
import networkx as nx
from config import VK_TOKENS

import dash
import dash_cytoscape as cyto
from dash import html


vk_session = vk_api.VkApi(token=VK_TOKENS[3])
vk = vk_session.get_api()
app = dash.Dash(__name__)
target_group_id = 197217619


def timer(func):
    """Декортаор для тестов"""
    def wrapper(*args, **kwargs):
        start_time = perf_counter()
        var = func(*args, **kwargs)
        print(f"Execution time: {perf_counter() - start_time}")
        return var
    return wrapper


@timer
def get_group_members(group_id: int, save_file: bool = False):
    """id всех пользователей группы. vk.groups.getMembers() имеет ограничение в 1000 id за раз"""
    user_count = vk.groups.getMembers(group_id=target_group_id)["count"]
    offset = 0
    user_set = set()
    while len(user_set) < user_count:
        members = vk.groups.getMembers(group_id=group_id, sort="id_asc", offset=offset)
        user_set.update(members["items"])
        offset += 999
        user_count = members["count"]
    user_list = list(user_set)
    # Конец return user_list
    if save_file:
        with open("group_users.txt", "w") as f:
            s = ', '.join(map(str, user_list))
            f.write(s)
    return user_list


@timer
def get_users_groups_member_count(group_id: int, from_file: bool = False, save_csv: bool = False):
    """Функция для исследования количества пользователей в группах пользователей исследуемой группы"""
    if from_file:
        with open("group_users.txt", "r") as f:
            user_list = list(map(int, f.read().split(", ")))
    else:
        user_list = get_group_members(group_id)
    # Создаем словарь что бы сохранять только уникальные группы
    users_groups_dict = dict()
    for user_id in user_list:
        try:
            user_info = vk.groups.get(user_id=user_id, extended=1, fields="members_count")
            for item in user_info["items"]:
                if item["is_closed"] == 0:
                    try:
                        users_groups_dict[item["id"]] = [item["name"], item["members_count"]]
                    except KeyError:
                        continue
        except vk_api.exceptions.ApiError:
            continue
    group_id_list, name_list, members_count_list = [], [], []
    for item in users_groups_dict.items():
        group_id_list.append(item[0])
        name_list.append(item[1][0])
        members_count_list.append(item[1][1])
    df = pd.DataFrame({"name": name_list, "members_count": members_count_list},
                      index=group_id_list)
    df = df.sort_values(by=["members_count"], ascending=False)
    print(f"Топ-10 групп по числу участников\n{df.head(10)}")
    # Конец return df
    if save_csv:
        df.to_csv("./members_groups_member_count.csv")
    return users_groups_dict


def get_age(bdate: str):
    """Рассчет возраста по полной дате, если год не указан вернет -1"""
    if re.match("^[0-9]{1,2}.[0-9]{1,2}.[0-9]{4}$", bdate) is not None:
        _date = datetime.strptime(bdate, "%d.%m.%Y")
        return date.today().year - _date.year - ((date.today().month, date.today().day) < (_date.month, _date.day))
    else:
        return -1


@timer
def get_group_members_info(group_id: int, save_csv: bool = False):
    """Функция для сбора информации о пользовтелях группы"""
    user_id_list, active_list, sex_list, age_list, country_list, city_list = [], [], [], [], [], []
    user_list = get_group_members(group_id)
    user_list = [user_list[i:i + 1000] for i in range(0, len(user_list), 1000)]
    for part in user_list:
        fetched_data = vk.users.get(user_ids=part, fields=["city", "county", "sex", "bdate"])
        for user_info in fetched_data:
            user_id_list.append(user_info["id"])
            if "deactivated" in user_info:
                active_list.append(False)
                sex_list.append(-1)
                age_list.append(-1)
                country_list.append(None)
                city_list.append(None)
                continue
            active_list.append(True)
            sex_list.append(user_info["sex"])
            if "bdate" in user_info:
                age = get_age(user_info["bdate"])
                age_list.append(age)
            else:
                age_list.append(-1)
            if "country" in user_info:
                country_list.append(user_info["country"]["title"])
            else:
                country_list.append(None)
            if "city" in user_info:
                city_list.append(user_info["city"]["title"])
            else:
                city_list.append(None)
    dataframe = pd.DataFrame(
        {"active": active_list, "sex": sex_list, "age": age_list, "country": country_list, "city": city_list},
        index=user_id_list)
    # Если нужно сохранить данные
    if save_csv:
        dataframe.to_csv(f"./group_members_info.csv")
    return dataframe


def show_group_members_info(dataframe: pd.DataFrame):
    """Функция вывода отчета с информацией о группе на основе датафрейма"""
    members_count = len(dataframe)
    dataframe = dataframe.loc[dataframe["active"]]
    data_size = len(dataframe)
    inactive_user_count = members_count - data_size
    age_frame = dataframe["age"][dataframe["age"] != -1]
    mean_age = age_frame.describe()["mean"]
    median_age = age_frame.describe()["50%"]
    hidden_age_count = data_size - len(age_frame)
    sex_percentages = dataframe.groupby(by="sex")["sex"].count().transform(
        (lambda x: round(x * 100 / data_size, 2)))
    hidden_location_count = len(dataframe[dataframe["city"].isna()])
    location_counter = Counter(dataframe["city"][dataframe["city"].notna()])
    location_dataframe = pd.DataFrame.from_dict(location_counter, orient="index",
                                                columns=["count"]).sort_values(by="count", ascending=False)
    info_str = f"В группе {members_count} участников из них {inactive_user_count} удалено или заблокировано.\n\n" \
               f"Средний возраст участников группы {int(mean_age)} лет;\n" \
               f"медианное значение {int(median_age)} лет;\n" \
               f"возраст не указан у {int(hidden_age_count * 100 / data_size)} % участников.\n\n" \
               f"В группе {int(sex_percentages[2])} % мужчин, {int(sex_percentages[1])} % женщин.\n" \
               f"Пол не указан у {int(sex_percentages[0])} % участников.\n\n" \
               f"Из участников {round(hidden_location_count * 100 / data_size)} % не указали город проживания;\n" \
               f"топ-10 по городам из указавших город проживания:\n{location_dataframe.head(10)}"
    print(info_str)
    return info_str


def get_users_friends(group_id: int, save_csv: bool = False):
    """Функция для сбора id друзей пользвателей группы"""
    user_list = get_group_members(group_id)
    print("Harvest start!")

    async def _get_user_friends(user_id: int):
        try:
            items = await vk.friends.get(user_id=user_id)['items']
            print(user_id)
            return {user_id, items}
        except vk_api.exceptions.ApiError:
            items = [-1]
            return {user_id, items}

    results = []

    async def main(users):
        for user in users:
            results.append(_get_user_friends(user))
        await asyncio.gather(*results)
        return results
    asyncio.run(main(user_list))
    print("Harvest complete!")
    users_friends_dict = {}

    def _update_dict(pair):
        print(pair.keys())
        return users_friends_dict.update(pair)
    pool = mp.Pool(4)
    results = pool.map(_update_dict, [result for result in results])
    pool.close()
    print(users_friends_dict)
    if save_csv:
        pd.Series(users_friends_dict).to_csv("members_friends.csv")
    return users_friends_dict


@timer
def create_group_graph(users_friends_dict: dict):
    """ва"""
    # users_friends_dict = pd.read_csv("members_friends.csv", index_col=[0], converters={"0": pd.eval})["0"].to_dict()
    user_list = list(users_friends_dict.keys())
    g = nx.Graph(directed=False)
    for user in users_friends_dict:
        g.add_node(user)
        for friend in users_friends_dict[user]:
            if user != friend and user in user_list and friend in user_list:
                g.add_edge(user, friend)
    to_del = [i for i in g if g.degree(i) < 1]
    for i in to_del:
        del users_friends_dict[i]
        user_list.remove(i)
        g.remove_node(i)
    # pos = nx.spring_layout(g)
    # net = Network(height="1000px", width="1000px")
    # net.from_nx(g)
    # net.show("graph.html")
    elements = []
    for user in users_friends_dict:
        elements.append({'data': {'id': str(user), 'label': str(user)}})
        # 'preset': {'x': int(pos[user][0] * 1000), 'y': int(pos[user][1] * 1000)}})
        for friend in users_friends_dict[user]:
            if user != friend and user in user_list and friend in user_list:
                elements.append({'data': {'source': str(user), 'target': str(friend)}})
    # plt.show()
    return elements


if __name__ == "__main__":
    # part 1
    # get_users_groups_member_count(group_id=target_group_id, from_file=True, save_csv=True)
    # part 2
    # get_group_members_info(group_id=target_group_id, from_file=True, save_csv=True)
    # part 3
    # get_users_friends(target_group_id, save_csv=True)
    # graph_data = create_group_graph(from_file=True, save_csv=True)
    get_group_members(target_group_id)
    # app.layout = html.Div([
    #     cyto.Cytoscape(
    #         id='vk-group-members-data',
    #         layout={'name': 'cose'},
    #         style={'width': '100%', 'height': '1000px'},
    #         elements=graph_data
    #     )
    # ])
    # app.run_server(debug=True)
    print("See, sometimes, in life, you try and hit life with a 'hoocha!', but then life decides to hit you with a "
          "'hoocha!'. You know what you do, in this predicament? When life 'hoochas' you? You go AGAIN! (C) Dum Shark")