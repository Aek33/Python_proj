import time

import vk_api
from config import VK_API_2
import pandas as pd

vk_session = vk_api.VkApi(token=VK_API_2)
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
        print(f"Processing: {round(len(user_set) * 100 / user_count, 1)}%")
    user_list = list(user_set)
    # Конец return user_list
    if save_file:
        with open("group_users.txt", "w") as f:
            s = ', '.join(map(str, user_list))
            f.write(s)
        return "./group_users.txt"
    else:
        return user_list


def get_users_groups(user_list: list = None, from_file: bool = False, save_csv: bool = False):
    if from_file:
        with open("./group_users.txt", "r") as f:
            user_list = list(map(int, f.read().split(", ")))
    # Начало
    users_groups = dict()
    count = 0
    for user_id in user_list:
        print(f"User id: {user_id}")
        try:
            user_info = vk.groups.get(user_id=user_id, extended=1, fields="members_count")
            for item in user_info["items"]:
                print(f"Group id: {item['id']}", f"Count: {item['members_count']}")
                if item["is_closed"] == 0:
                    try:
                        users_groups[item["id"]] = item["members_count"]
                        print(users_groups.items())
                    except KeyError:
                        continue
        except vk_api.exceptions.ApiError:
            continue
        finally:
            count += 1
            if count % 1000 == 0:
                print(count)
    # Конец return users_groups
    if save_csv:
        df = pd.DataFrame(users_groups.items(), columns=["group_id", "members_count"])
        df = df.sort_values(by=["members_count"], ascending=False)
        df.to_csv("./groups_member_count")
    return users_groups


if __name__ == "__main__":
    # get_group_members(target_group_id, save_file=True)
    # get_users_groups(from_file=True, save_csv=True)
    print(vk.users.get(user_ids=[474644480, 397705222]))