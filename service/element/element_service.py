from helper.oss_helper.oss_helper import oss_helper1
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1


def get_element_tree():
    """
    获取算子树  根据算子分类组成（算子分类+算子）树
    :return: [{ "id":""主键
                "category_name": "",//数据源ID
                "category_icon": "",数据表名
                "description":"",描述
                “children”:[{算子
                             "id":""主键
                             "element_name":""名称
                             "node_type":""算子代码
                             "element_icon":""算子图标
                             "description":""描述
                            }]
              }]
    """
    data = []
    element_sql = "select * " "from ml_element "
    element = db_helper1.fetchall(element_sql, [])
    if element:
        category_sql = "select * " "from ml_element_category "
        category = db_helper1.fetchall(category_sql, [])

        data = [
            {
                "id": info.get("id", ""),
                "category_name": info.get("category_name", ""),
                "category_icon": oss_helper1.generate_s3_presigned_url_for_image(info.get("category_icon"))
                if info.get("category_icon")
                else "",
                "description": info.get("description", ""),
                "children": [
                    {
                        "id": e.get("id", ""),
                        "element_name": e.get("element_name", ""),
                        "node_type": e.get("node_type", ""),
                        "element_icon": oss_helper1.generate_s3_presigned_url_for_image(e.get("element_icon"))
                        if e.get("element_icon")
                        else "",
                        "description": e.get("description", ""),
                    }
                    for e in list(filter(lambda x: x.get("category_id") == info.get("id"), element))
                ],
            }
            for info in category
        ]

    return execute_success(data=data)
