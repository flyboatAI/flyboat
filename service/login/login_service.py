import sys

from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from helper.generate_helper import generate_uuid
from helper.pipelining_helper import copy_pipelining_version
from helper.sql_helper.init_sql_helper import db_helper1
from service.experiment_space.experiment_space_service import create_experiment_space


def create_demo_sample_data_and_pipelining(user_id: str):
    # 拷贝样本数据
    copy_result_code = copy_sample_data(user_id)
    if copy_result_code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "复制样例失败") from None

    # 创建空间
    space_id = create_experiment_space(user_id, "样例流水线")
    if not space_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "复制样例失败") from None
    # 拷贝样例流水线
    pipelining_id = "2lMImTS8ep9f0yC7jjwPhVSjZBB"
    version_id = "2lMImTLCU3JRxKa4XGAZ2URk8Nc"
    new_pipelining_id = generate_uuid()
    copy_result = copy_pipelining_version(
        pipelining_id, version_id, user_id=user_id, space_id=space_id, new_pipelining_id=new_pipelining_id
    )
    if copy_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "复制样例失败") from None
    return copy_result.code


def copy_sample_data(user_id: str):
    sample_data_id = "2lMIhYeskAbcoj51JGheZu8liF0"
    sql = (
        f"insert into ml_sample_file_model "
        f"select createguid(), is_deleted, '{user_id}', "
        f"create_time, data_model_name, table_id, file_id, file_type, remark "
        f"from ml_sample_file_model "
        f"where id='{sample_data_id}'"
    )
    return db_helper1.execute(sql)
