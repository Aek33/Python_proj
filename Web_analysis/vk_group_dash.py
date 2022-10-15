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

app.layout = html.Div([
    html.Header([
        html.H3([f"Демографический портрет сообщества ", group_metadata['name']]),
        html.Img(src=group_metadata["photo_200"])
    ], className="header"),
    html.Div([
        html.Div([
            html.P([
                html.P(["Всего пользователей ", html.Span(len(group_users['active']), style={"color": "#ffb86c"})]),
                html.P(["Заблокировано или удалено ", html.Span(len(group_users['active'][~group_users['active']]), style={"color": "#ffb86c"})]),
                html.P(["Закрыло доступ к странице ", html.Span(len(group_users['is_closed'][group_users['is_closed']]), style={"color": "#ffb86c"})]),
            ], style={"margin-bottom": "20px"}),
            html.P([
                html.P(["Средний возраст ",  html.Span(round(group_users["age"][group_users["age"] >= 0].mean()), style={"color": "#8be9fd"})]),
                html.P(["Мединное значение возраста ",  html.Span(round(group_users["age"][group_users["age"] >= 0].median()), style={"color": "#8be9fd"})]),
                html.P(["Не указало возраст ", html.Span(round(len(group_users["age"][group_users["age"] < 0]) * 100 / len(group_users["age"])),  style={"color": "#ffb86c"}), " %"]),
            ], style={"margin-bottom": "20px"}),
            html.P([
                html.P(["Пользователей мужского пола ", html.Span(round(len(group_users["sex"][group_users["sex"] == 2]) * 100 / len(group_users["sex"])), style={"color": "#ffb86c"}), " %"]),
                html.P(["Пользователей женского пола ", html.Span(round(len(group_users["sex"][group_users["sex"] == 1]) * 100 / len(group_users["sex"])), style={"color": "#ffb86c"}), " %"]),
                html.P(["Пользователей не указавших пол ", html.Span(round(len(group_users["sex"][group_users["sex"] == 0]) * 100 / len(group_users["sex"]), 3), style={"color": "#ffb86c"}), " %"])
            ], style={"margin-bottom": "20px"}),
            html.P([
                html.P(["Пользователей не указавших страну проживания ", html.Span(round(len(group_users["country"][group_users["country"] == "unknown"]) * 100 / len(group_users["country"])), style={"color": "#ffb86c"}), " %"]),
                html.P(["Пользователей не указавших город проживания ", html.Span(round(len(group_users["city"][group_users["city"] == "unknown"]) * 100 / len(group_users["city"])), style={"color": "#ffb86c"}), " %"]),
            ], style={"margin-bottom": "20px"}),
        ], className="members-count-banner"),
        html.Div([
            dcc.Dropdown(options=[
                {"label": "Active", "value": "active"},
                {"label": "Private page", "value": "is_closed"},
                {"label": "Sex", "value": "sex"},
            ], value='active', id="pie-chart-dropdown"),
            dcc.Graph(id="pie-chart", style={'width': '80vh', 'height': '80vh'})
        ], style={'display': 'flex', 'flex-direction': 'column'})
    ], className="container-top")
])

@app.callback(
    Output('pie-chart', 'figure'),
    Input('pie-chart-dropdown', 'value'))
def update_pie_chart(value):
    pie_chart = group_users.groupby(by=value).count().reset_index().loc[:, [value, "member_id"]]
    fig = px.pie(pie_chart, values="member_id", names=value)
    fig.update_layout()
    return fig


if __name__ == "__main__":
    # print(group_users.dtypes)
    # print(group_users.head())
    # print(users_friends.dtypes)
    # print(users_friends.head())
    # print(users_groups.dtypes)
    # print(users_groups.head())
    app.run(debug=True, dev_tools_hot_reload=True)





