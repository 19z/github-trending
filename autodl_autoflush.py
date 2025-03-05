import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)
DAYS_BEFORE = 4


def login(session, phone, password):
    """登录并获取授权令牌"""
    login_url = "https://www.autodl.com/api/v1/new_login"
    headers = {"referer": "https://www.autodl.com/login"}
    login_data = {
        "phone": phone,
        "password": password,
        "v_code": "",
        "phone_area": "+86",
        "picture_id": None
    }

    # 执行登录
    resp = session.post(login_url, headers=headers, json=login_data)
    resp.raise_for_status()
    login_result = resp.json()
    if login_result["code"] != "Success":
        raise Exception(f"登录失败: {login_result.get('msg', '未知错误')}")

    # 换取authorization
    passport_url = "https://www.autodl.com/api/v1/passport"
    passport_data = {"ticket": login_result["data"]["ticket"]}
    resp = session.post(passport_url, headers=headers, json=passport_data)
    resp.raise_for_status()
    passport_result = resp.json()
    if passport_result["code"] != "Success":
        raise Exception(f"获取授权失败: {passport_result.get('msg', '未知错误')}")

    # 设置会话头
    session.headers.update({
        "content-type": "application/json;charset=UTF-8",
        "authorization": passport_result["data"]["token"],
        "referer": "https://www.autodl.com/console/homepage/personal"
    })
    return session


def get_all_instances(session):
    """获取所有实例"""
    payload = {
        "date_from": "",
        "date_to": "",
        "page_index": 1,
        "page_size": 1000,  # 增大 page_size 以减少分页需求
        "status": [],
        "charge_type": []
    }
    time.sleep(1)
    resp = session.post("https://www.autodl.com/api/v1/instance", json=payload)
    resp.raise_for_status()
    result = resp.json()

    if result["code"] != "Success":
        raise Exception(f"获取实例失败: {result.get('msg', '未知错误')}")
    logger.info(f"获取实例成功: {result}")
    return result["data"]["list"]


def find_earliest_stopped_instance(instances):
    """找到 stopped_at 最早且超过7天的实例"""
    now = datetime.now(timezone(timedelta(hours=8)))
    seven_days_ago = now - timedelta(days=DAYS_BEFORE)

    earliest_instance = None
    earliest_time = None

    for inst in instances:
        if inst["status"] != "shutdown":
            return None

        stopped_at = inst["stopped_at"]
        if not stopped_at["Valid"]:
            continue

        stop_time = datetime.fromisoformat(stopped_at["Time"])
        if stop_time < seven_days_ago and (earliest_time is None or stop_time < earliest_time):
            earliest_instance = inst
            earliest_time = stop_time

    return earliest_instance


def power_on_instance(session, instance):
    """重启单个实例"""
    payload = {
        "instance_uuid": instance["uuid"],
        "payload": "non_gpu"
    }
    try:
        time.sleep(2)
        resp = session.post(
            "https://www.autodl.com/api/v1/instance/power_on",
            json=payload
        )
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"实例开机结果: {result}")
        return result["code"] == "Success"
    except Exception as e:
        logger.warning(f"实例开机失败: {str(e)}")
        return False


def set_shutdown_time(session, instance):
    """设置15分钟后定时关机"""
    shutdown_time = (datetime.now(timezone(timedelta(hours=8)))
                     + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M")

    payload = {
        "instance_uuid": instance["uuid"],
        "shutdown_at": shutdown_time
    }
    try:
        resp = session.post(
            "https://www.autodl.com/api/v1/instance/timed/shutdown",
            json=payload
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"实例定时关机设置失败: {str(e)}")


def main():
    # 从环境变量获取凭证
    phone = os.getenv("AUTODL_PHONE")
    password = os.getenv("AUTODL_PASSWORD")
    if not phone or not password:
        raise ValueError("请设置 AUTODL_PHONE 和 AUTODL_PASSWORD 环境变量")

    with requests.Session() as session:
        try:
            # 1. 登录并获取实例
            login(session, phone, password)
            all_instances = get_all_instances(session)

            # 2. 找到 stopped_at 最早且超过7天的实例
            target_instance = find_earliest_stopped_instance(all_instances)
            if not target_instance:
                print("没有需要处理的实例")
                return
            time.sleep(2)
            # 3. 重启实例
            success = power_on_instance(session, target_instance)
            if not success:
                print(f"实例开机失败")
                return
            time.sleep(5)
            # 4. 设置15分钟后定时关机
            set_shutdown_time(session, target_instance)
            print(f"已为实例设置15分钟后关机")

        except Exception as e:
            print(f"程序运行出错: {str(e)}")
            raise


if __name__ == "__main__":
    main()