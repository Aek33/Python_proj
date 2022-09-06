import time
import re
from datetime import date, datetime
from collections import Counter

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
        print(f"Execution time: {time.time() - start_time}")
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


@timer
def get_users_groups_member_count(group_id: int, from_file: bool = False, save_csv: bool = False):
    if from_file:
        with open("group_users.txt", "r") as f:
            user_list = list(map(int, f.read().split(", ")))
    else:
        user_list = get_group_members(group_id)
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
    print(f"Топ-10 групп по числу участников\n{df.head(10)}")
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
def get_group_members_info(group_id: int, from_file: bool = False, save_csv: bool = False):
    if from_file:
        dataframe = pd.read_csv("group_members_info")
    else:
    # Начало
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
            dataframe.to_csv(f"./group_members_info")
    # Блок обработки
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
               f"В группе {int(sex_percentages[2])} % мужчин, {int(sex_percentages[1])} % женщин" \
               f" и {int(sex_percentages[0])} % участников не указали пол.\n\n" \
               f"Из участников {round(hidden_location_count * 100 / data_size)} % не указали город проживания;\n" \
               f"топ-10 по городам из указавших город проживания:\n{location_dataframe.head(10)}"
    print(info_str)
    return info_str


if __name__ == "__main__":
    # part 1
    # get_users_groups_member_count(group_id=target_group_id, from_file=True, save_csv=True)
    # part 2
    get_group_members_info(group_id=target_group_id, from_file=True, save_csv=True)
    # part 3
