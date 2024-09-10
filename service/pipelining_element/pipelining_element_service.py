from enum_type.deleted import Deleted
from helper.sql_helper.init_sql_helper import db_helper1


def delete_pipelining_element_by_id(pipelining_element_id, user_id):
    sql = (
        f"update ml_pipelining_models set is_deleted={Deleted.Yes.value} "
        f"where id='{pipelining_element_id}' and create_user='{user_id}'"
    )

    return db_helper1.execute(sql)
