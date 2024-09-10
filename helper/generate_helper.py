from cyksuid.v2 import ksuid

from helper.time_helper import current_time


def generate_uuid():
    """
    生成 uuid
    :return: uuid
    """
    # return uuid.uuid4()
    return str(ksuid())


def uuid_and_now():
    """
    生成 uuid 和 当前日期
    :return: uuid、日期元组
    """
    return generate_uuid(), current_time()
