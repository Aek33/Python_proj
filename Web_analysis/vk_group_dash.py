import json
import os
import plotly.express as px
import pandas as pd

from dash import Dash, dcc, html, Input, Output

target_group_id = 197217619
with open(f"{target_group_id}.json", "r") as f:
    group_metadata = json.loads(f.read())

group_users = pd.read_csv("hololivepics_group_users.csv", sep=";")
group_users['active'] = group_users['active'].apply(lambda x: True if x == "t" else False)
group_users['is_closed'] = group_users['is_closed'].apply(lambda x: True if x == "t" else False)

users_friends = pd.read_csv("hololivepics_users_friends.csv", sep=";")
users_friends.users_friends = users_friends.users_friends.map(lambda x: x.strip("{}").split(","))

users_groups = pd.read_csv("hololivepics_users_groups.csv", sep=";", on_bad_lines="skip")
users_groups.access = users_groups.access.astype(bool)

app = Dash(__name__, assets_folder=f"{os.getcwd()}/assets")

pie_chart = group_users.groupby("active").count().reset_index().loc[:, ["active", "member_id"]]

fig = px.pie(pie_chart, values="member_id", names="active")

app.layout = html.Div([
    html.Header([
        html.H3([f"Демографический портрет сообщества ", html.Span(group_metadata['name'])]),
        html.Img(src=group_metadata["photo_200"])
    ], className="header"),
    html.Div([
        html.Div([
            html.P([
                f"Количество участников сообщества: ",
                html.Span(len(group_users['active']), style={"color": "#8be9fd"}),
                ". Из них ",
                html.Span(len(group_users['active'][~group_users['active']]), style={"color": "#ff5555"}),
                " заблокировано\\удалено, ",
                html.Span(len(group_users['is_closed'][group_users['is_closed']]), style={"color": "#ffb86c"}),
                " закрыло доступ к странице",
            ], style={"margin-bottom": "20px"}),
            html.P([
                f"Средний возраст участников сообщества ",
                html.Span(round(group_users["age"][group_users["age"] >= 0].mean()), style={"color": "#8be9fd"}),
                ", мединное значение ",
                html.Span(round(group_users["age"][group_users["age"] >= 0].median()), style={"color": "#8be9fd"}),
                ". Не указало возраст ",
                html.Span(round(len(group_users["age"][group_users["age"] < 0]) * 100 / len(group_users["age"])),
                          style={"color": "#ff5555"}),
                " % пользователей",
            ], style={"margin-bottom": "20px"}),
            html.P([
                f"В сообществе: ",
                html.Span(round(len(group_users["sex"][group_users["sex"] == 2]) * 100 / len(group_users["sex"])),
                          style={"color": "#8be9fd"}),
                " % пользователей мужского пола, ",
                html.Span(round(len(group_users["sex"][group_users["sex"] == 1]) * 100 / len(group_users["sex"])),
                          style={"color": "#8be9fd"}),
                " % пользователей женского и ",
                html.Span(round(len(group_users["sex"][group_users["sex"] == 0]) * 100 / len(group_users["sex"]), 3),
                          style={"color": "#ff5555"}),
                " % пользователей не указавших пол",
            ], style={"margin-bottom": "20px"}),
            html.P([
                html.Span(round(len(group_users["country"][group_users["country"] == "unknown"])
                                * 100 / len(group_users["country"])),
                          style={"color": "#ff5555"}),
                " % пользователей не указало страну проживания, ",
                html.Span(round(len(group_users["city"][group_users["city"] == "unknown"])
                                * 100 / len(group_users["city"])),
                          style={"color": "#ff5555"}),
                " % пользователей не указало город проживания",
            ], style={"margin-bottom": "20px"}),
        ], className="members-count-banner"),
        html.Div([
            dcc.Graph(
                className="pie-chart",
                id="group-metrix",
                figure=fig
            )
        ])
    ], className="container-top")
])

if __name__ == "__main__":
    # print(group_users.dtypes)
    # print(group_users.head())
    # print(users_friends.dtypes)
    # print(users_friends.head())
    # print(users_groups.dtypes)
    # print(users_groups.head())
    app.run(debug=True, dev_tools_hot_reload=True)




# import pandas as pd
# import networkx as nx
# info_str = f"В группе {members_count} участников из них {inactive_user_count} удалено или заблокировано.\n\n" \
#            f"Средний возраст участников группы {int(mean_age)} лет;\n" \
#            f"медианное значение {int(median_age)} лет;\n" \
#            f"возраст не указан у {int(hidden_age_count * 100 / data_size)} % участников.\n\n" \
#            f"В группе {int(sex_percentages[2])} % мужчин, {int(sex_percentages[1])} % женщин" \
#            f" и {int(sex_percentages[0])} % участников не указали пол.\n\n" \
#            f"Из участников {round(hidden_location_count * 100 / data_size)} % не указали город проживания;\n" \
#            f"топ-10 по городам из указавших город проживания:\n{location_dataframe.head(10)}"
# def create_group_graph(group_id: int = None, from_file: bool = False, save_csv: bool = False):
#     if from_file:
#         users_friends_dict = pd.read_csv("members_friends.csv", index_col=[0],
#                                          converters={"0": pd.eval})["0"].to_dict()
#         user_list = users_friends_dict.keys()
#     elif group_id is not None:
#         user_list = get_group_members(group_id)
#         pool = mp.Pool(4)
#         results = pool.map(get_users_friends, [user for user in user_list])
#         pool.close()
#         users_friends_dict = {}
#         for result in results:
#             users_friends_dict[result[0]] = result[1]
#         if save_csv:
#             pd.Series(users_friends_dict).to_csv("members_friends.csv")
#     else:
#         return -1
#     count = 0
#     g = nx.Graph(directed=False)
#     for user in users_friends_dict:
#         g.add_node(user)
#         for friend in users_friends_dict[user]:
#             if user != friend and user in user_list and friend in user_list:
#                 g.add_edge(user, friend)
#                 count += 1
#     # net = Network(height="1000px", width="1000px")
#     # net.from_nx(g)
#     # net.show("graph.html")
#     print(count)
#     nx.draw_spring(g)
#     plt.show()
#     return users_friends_dict
