import base64

from enum_type.secret_type import SecretType
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.sql_helper.init_sql_helper import db_helper1


def get_secret_by_app_key(app_key, publish_id):
    secret_sql = f"select id, app_secret, publish_id, secret_type " f"from ml_secret where app_key='{app_key}'"
    secret_dict = db_helper1.fetchone(secret_sql)
    if secret_dict and secret_dict.get("secret_type") is not None:
        secret_type = secret_dict["secret_type"]
        if secret_type == SecretType.Pipelining.value:
            secret_publish_id = secret_dict.get("publish_id")
            if secret_publish_id == publish_id:
                return secret_dict.get("app_secret")
        else:
            # TODO: 验证该 secret_id 对应的 App 是否有权限调用该接口
            return secret_dict.get("app_secret")
    return None


def valid_token(token, app_key, client_timestamp, publish_id):
    if not token or not client_timestamp or not publish_id:
        return False
    app_secret = get_secret_by_app_key(app_key, publish_id)
    if not app_secret:
        return False
    return token == encrypt(app_secret, client_timestamp)


# AES 'pad' byte array to multiple of BLOCK_SIZE bytes
def pad(byte_array):
    BLOCK_SIZE = 16
    pad_len = BLOCK_SIZE - len(byte_array) % BLOCK_SIZE
    return byte_array + (bytes([pad_len]) * pad_len)


# Remove padding at end of byte array
def unpad(byte_array):
    last_byte = byte_array[-1]
    return byte_array[0:-last_byte]


def encrypt(key, message):
    """
    加密过程 key + message 后进行 base64 编码
    """
    encode_msg = base64.b64encode((key + message).encode("utf-8")).decode("UTF-8")
    return encode_msg


def generate_key(publish_id, user_id, secret_name, description):
    app_secret, now = uuid_and_now()
    app_key = generate_uuid()
    if not description:
        description = ""
    insert_secret_sql = (
        f"insert into ml_secret values(CREATEGUID(), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{secret_name}', "
        f"'{app_key}', '{app_secret}', {SecretType.Pipelining.value}, "
        f"'{publish_id}', '{description}')"
    )
    return db_helper1.execute(insert_secret_sql)


def get_secret_by_publish_id(publish_id):
    secret_sql = f"select * from ml_secret " f"where publish_id='{publish_id}' " f"order by CREATE_TIME desc"
    return db_helper1.fetchall(secret_sql)


def delete_secret_by_id(private_id):
    delete_secret_sql = f"delete from ml_secret " f"where id='{private_id}'"
    return db_helper1.execute(delete_secret_sql)
