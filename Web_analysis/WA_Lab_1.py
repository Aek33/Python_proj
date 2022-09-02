import time
import re
from datetime import date, datetime
import vk_api
import pandas as pd
from config import VK_API_1

vk_session = vk_api.VkApi(token=VK_API_1)
vk = vk_session.get_api()

target_group_id = 197217619


def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        print(time.time() - start_time)
    return wrapper


def get_group_members(group_id: int, save_file: bool = False):
    # Начало
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
        return "./group_users.txt"
    else:
        return user_list


def get_users_groups_member_count(user_list: list = None, from_file: bool = False, save_csv: bool = False):
    if from_file:
        with open("group_users.txt", "r") as f:
            user_list = list(map(int, f.read().split(", ")))
    # Начало
    # Создаем словарь что бы сохранять только уникальные группы
    users_groups = dict()
    for user_id in user_list:
        try:
            user_info = vk.groups.get(user_id=user_id, extended=1, fields="members_count")
            for item in user_info["items"]:
                if item["is_closed"] == 0:
                    try:
                        users_groups[item["id"]] = [item["name"], item["members_count"]]
                    except KeyError:
                        continue
        except vk_api.exceptions.ApiError:
            continue
    group_id_list, name_list, members_count_list = [], [], []
    for item in users_groups.items():
        group_id_list.append(item[0])
        name_list.append(item[1][0])
        members_count_list.append(item[1][1])
    df = pd.DataFrame({"name": name_list, "members_count": members_count_list},
                      index=group_id_list)
    df = df.sort_values(by=["members_count"], ascending=False)
    # Конец return df
    if save_csv:
        df.to_csv("./members_groups_member_count")
    return users_groups


def get_age(bdate: str):
    if re.match("^[0-9]{1,2}.[0-9]{1,2}.[0-9]{4}$", bdate) is not None:
        _date = datetime.strptime(bdate, "%d.%m.%Y")
        return date.today().year - _date.year - ((date.today().month, date.today().day) < (_date.month, _date.day))
    else:
        return -1


@timer
def get_group_members_info(group_id: int, save_csv: bool = False):
    # Начало
    user_id_list, inactive_list, sex_list, age_list, country_list, city_list = [], [], [], [], [], []
    user_list = get_group_members(group_id)
    members_count = len(user_list)
    user_list = [user_list[i:i + 1000] for i in range(0, len(user_list), 1000)]
    for part in user_list:
        fetched_data = vk.users.get(user_ids=part, fields=["city", "county", "sex", "bdate"])
        for user_info in fetched_data:
            user_id_list.append(user_info["id"])
            if "deactivated" in user_info:
                inactive_list.append(True)
                sex_list.append(-1)
                age_list.append(-1)
                country_list.append(None)
                city_list.append(None)
                continue
            inactive_list.append(False)
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
        {"inactive": inactive_list, "sex": sex_list, "age": age_list, "country": country_list, "city": city_list},
        index=user_id_list)
    # Конец return members_count, df
    if save_csv:
        dataframe.to_csv(f"./group_members{members_count}_info")
    return members_count, dataframe


if __name__ == "__main__":
    # part 1
    # get_group_members(target_group_id, save_file=True)
    # get_users_groups_member_count(from_file=True, save_csv=True)
    # print(pd.read_csv("groups_member_count_archive", index_col=0).head(20))
    # part 2
    get_group_members_info(target_group_id, save_csv=True)