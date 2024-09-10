import json

from enum_type.connect_type import ConnectType
from helper.time_helper import current_time


def send_message(websocket, msg, child=False):
    if websocket and "connect_id" in msg and msg.get("connect_id") and not child:
        websocket.send(json.dumps(msg))


def generate_message(connect_id, message_type, message, pid=None, element_id=None, time=None):
    return {
        "connect_id": connect_id,
        "msg": f"{current_time()} {message}",
        "type": message_type,
        "pid": pid,
        "element_id": element_id,
        "time": time,
    }


def generate_cancel_message(connect_id):
    return generate_message(connect_id, ConnectType.Cancel.value, "流程运行结束, 取消执行流程")


def generate_create_message(connect_id):
    return generate_message(
        connect_id,
        ConnectType.Create.value,
        f"{current_time()} 正在创建新进程以执行本流水线",
    )


def generate_open_message(connect_id, pid):
    return generate_message(connect_id, ConnectType.Open.value, f"进程创建完成, 进程标识: {pid}", pid)


def generate_running_message(connect_id, message, element_id=None):
    return generate_message(connect_id, ConnectType.Connecting.value, message, element_id=element_id)


def generate_close_message(connect_id, message, time=1):
    return generate_message(connect_id, ConnectType.Close.value, message, time=time)


def generate_error_message(connect_id, message):
    return generate_message(connect_id, ConnectType.Error.value, message)
