import time
import re
from datetime import date, datetime
import vk_api
import pandas as pd
from config import VK_API_1

vk_session = vk_api.VkApi(token=VK_API_1)
vk = vk_session.get_api()

target_group_id = 197217619


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
    start_time = time.time()

    if from_file:
        with open("group_users.txt", "r") as f:
            user_list = list(map(int, f.read().split(", ")))
    count = 0
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
        finally:  # Не обязательно
            count += 1
            if count % 100 == 0:
                print(f"{count} users processed for {(time.time() - start_time)} second")
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


def get_group_members_info(group_id: int, save_csv: bool = False):
    count = 0
    start_time = time.time()
    # Начало
    user_list = get_group_members(group_id)
    members_count = len(user_list)
    date_pattern = "^[0-9]{1,2}.[0-9]{1,2}.[0-9]{4}$"
    today = date.today()
    inactive_list, sex_list, age_list, country_list, city_list = [], [], [], [], []
    for user in user_list:
        user_info = vk.users.get(user_ids=[user], fields=["city", "county", "sex", "bdate"])[0]
        if "deactivated" in user_info:
            inactive_list.append(True)
            sex_list.append(-1)
            age_list.append(-1)
            country_list.append(None)
            city_list.append(None)
            continue
        inactive_list.append(False)
        sex_list.append(user_info["sex"])
        if "bdate" in user_info and re.match(date_pattern, user_info["bdate"]) is not None:
            bdate = datetime.strptime(user_info["bdate"], "%d.%m.%Y")
            age = today.year - bdate.year - ((today.month, today.day) < (bdate.month, bdate.day))
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
        # Не обязательно
        count += 1
        if count % 100 == 0:
            print(f"{count} users processed for {(time.time() - start_time)} second")
    dataframe = pd.DataFrame(
        {"inactive": inactive_list, "sex": sex_list, "age": age_list, "country": country_list, "city": city_list},
        index=user_list)
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
    m, df = get_group_members_info(target_group_id)
    print(m)
    print(df)
