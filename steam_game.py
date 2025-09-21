import json
import time
import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, types
import pymysql

# 从环境变量或使用默认值获取配置
tocken = os.environ.get("STEAM_KEY")  # 你的steam_tocken
steamid = os.environ.get("STEAM_ID")  # 你的steam_id
database_url = os.environ['DATABASE_URL']

import requests
import json


def Steam_GetOwnedGames(tocken, steamid):
    url = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=%s&steamid=%s&format=json&include_appinfo=1&include_played_free_games=1" % (tocken, steamid)

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)

    res = json.loads(response.text)

    return res


def Steam_GetRecentlyPlayedGames(tocken, steamid):
    url = "https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key=%s&steamid=%s&format=json" % (tocken, steamid)

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)

    res = json.loads(response.text)

    return res


def SteamDA_OwnedGames(tocken, steamid):
    results = Steam_GetOwnedGames(tocken, steamid)

    # print(results["response"]["games"][0])

    appid = []
    game_name = []
    img_icon_url = []
    playtime_forever = []
    playtime_windows_forever = []
    playtime_mac_forever = []
    playtime_linux_forever = []
    rtime_last_played = []

    for i in results["response"]["games"]:
        appid.append(i["appid"])
        game_name.append(i["name"])
        img_icon_url.append(i["img_icon_url"])
        playtime_forever.append(i["playtime_forever"])
        playtime_windows_forever.append(i["playtime_windows_forever"])
        playtime_mac_forever.append(i["playtime_mac_forever"])
        playtime_linux_forever.append(i["playtime_linux_forever"])
        rtime_last_played.append(i["rtime_last_played"])

    df = pd.DataFrame({"appid": appid, "game_name": game_name, "img_icon_url": img_icon_url, "playtime_forever": playtime_forever, "playtime_windows_forever": playtime_windows_forever,
                       "playtime_mac_forever": playtime_mac_forever, "playtime_linux_forever": playtime_linux_forever, "rtime_last_played": rtime_last_played})

    # print(df)

    con_engine = create_engine(database_url + '?charset=utf8')

    dtype = {
        "appid": types.String(length=100),
        "game_name": types.String(length=255),
        "img_icon_url": types.String(length=255),
        "playtime_forever": types.Integer(),
        "playtime_windows_forever": types.Integer(),
        "playtime_mac_forever": types.Integer(),
        "playtime_linux_forever": types.Integer(),
        "rtime_last_played": types.Integer()
    }

    df.to_sql('dim_steam_owned_game', con_engine, dtype=dtype, if_exists='replace', index=False)


def SteamDA_GamePlayedRecord():
    from urllib.parse import urlparse
    db_url = urlparse(database_url)
    db_config = {
        'pool_name': 'github_trending_pool',
        'user': db_url.username,
        'password': db_url.password,
        'host': db_url.hostname,
        'port': db_url.port,
        'database': db_url.path.lstrip('/'),
        'charset': 'utf8mb4',
        'pool_size': 5,  # 连接池大小
        'pool_reset_session': True  # 重置会话
    }

    db = pymysql.connect(
        host=db_config['host'],
        port=db_url.port,
        user=db_url.username,  # 在这里输入用户名
        password=db_url.password,  # 在这里输入密码
        charset='utf8mb4',
        database=db_config['database']  # 指定操作的数据库
    )

    cursor = db.cursor()  # 创建游标对象

    try:
        create_db = """
        CREATE TABLE IF NOT EXIST `dwd_steam_game_played_record` (
  `id` int NOT NULL AUTO_INCREMENT,
  `appid` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `game_name` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `playtime` int DEFAULT NULL,
  `playtime_windows` int DEFAULT NULL,
  `playtime_mac` int DEFAULT NULL,
  `playtime_linux` int DEFAULT NULL,
  `rtime_last_played` int DEFAULT NULL,
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
"""
        cursor.execute(create_db)
        
        sql = """
        INSERT INTO dwd_steam_game_played_record 
        SELECT 
                NULL id,
                t1.appid,
                t1.game_name,
                COALESCE(t1.playtime_forever - t2.playtime,t1.playtime_forever) playtime,
                COALESCE(t1.playtime_windows_forever - t2.playtime_windows,t1.playtime_windows_forever) playtime_windows,
                COALESCE(t1.playtime_mac_forever - t2.playtime_mac,t1.playtime_mac_forever) playtime_mac,
                COALESCE(t1.playtime_linux_forever - t2.playtime_linux,t1.playtime_linux_forever) playtime_linux,
                t1.rtime_last_played,
                NOW() create_time,
                NOW() update_time 
        FROM (
        SELECT 
                    appid,
                    game_name,
                    playtime_forever,
                    playtime_windows_forever,
                    playtime_mac_forever,
                    playtime_linux_forever,
                    rtime_last_played
        FROM dim_steam_owned_game
        ) t1
        LEFT JOIN
        (
        SELECT 
                    appid,
                    SUM(playtime) playtime,
                    SUM(playtime_windows) playtime_windows,
                    SUM(playtime_mac) playtime_mac,
                    SUM(playtime_linux) playtime_linux,
                    MAX(rtime_last_played) rtime_last_played
        FROM dwd_steam_game_played_record
        GROUP BY appid,game_name
        )t2
        ON t1.appid=t2.appid
        WHERE t2.rtime_last_played IS NULL OR t1.rtime_last_played !=t2.rtime_last_played
        """
        # print(sql)
        cursor.execute(sql)
        db.commit()

    except Exception as e:
        print(e)
        db.rollback()  # 回滚事务

    finally:
        cursor.close()
        db.close()  # 关闭数据库连接


if __name__ == "__main__":
    bt = time.time()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "开始采集Steam游戏数据")

    SteamDA_OwnedGames(tocken, steamid)
    SteamDA_GamePlayedRecord()

    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Steam游戏数据采集结束", time.time() - bt)
