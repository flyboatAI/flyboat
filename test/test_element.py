import json
from unittest.mock import patch

import pytest

from config.dag_config import DATA_FILTER
from element.data_preprocessing_element.data_filter import DataFilter
from element.input_output_element.database_input import DatabaseInput
from element_configuration.structured_data_insight_configuration import insight_element
from helper.generate_helper import generate_uuid
from helper.result_helper import ExecuteResult


@pytest.mark.parametrize(
    "data_arr, fields_arr",
    [
        (
            [[{"a": 1, "b": 2}, {"a": 3, "b": 4}]],
            [
                [
                    {"name": "a", "nick_name": "a", "data_type": "NUMBER"},
                    {"name": "b", "nick_name": "b", "data_type": "NUMBER"},
                ]
            ],
        ),
    ],
)
def test_data_filter(data_arr, fields_arr):
    # mock 数据库操作
    with patch("helper.sql_helper.init_sql_helper.db_helper1.fetchone") as mock:
        mock.return_value = {
            "filter_type": "all",
            "filter_compare_fields": json.dumps(
                [
                    {
                        "name": "a",
                        "value": 2,
                        "data_type": "NUMBER",
                        "compare_type": "less_than",
                    }
                ]
            ),
        }
        element_id = generate_uuid()
        version_id = generate_uuid()
        e = DataFilter(element_id, version_id, "u_id")
        process_id = generate_uuid()
        # 测试执行
        (
            code,
            message,
            dependency_arr,
            data_arr,
            fields_arr,
            role_arr,
            model_arr,
            scaler_arr,
        ) = e.element_process(process_id, None, data_arr, fields_arr, None, None, None)
        assert code == 0
        assert data_arr == [[{"a": 1, "b": 2}]]
        assert fields_arr == [
            [
                {"name": "a", "nick_name": "a", "data_type": "NUMBER"},
                {"name": "b", "nick_name": "b", "data_type": "NUMBER"},
            ]
        ]

        # 测试洞察
    insight_result: ExecuteResult = insight_element(version_id, element_id, DATA_FILTER, process_id)
    assert insight_result.code == 0
    assert insight_result.data["data"] == [{"a": 1, "b": 2}]
    assert insight_result.data["fields"] == [
        {"name": "a", "nick_name": "a", "data_type": "NUMBER"},
        {"name": "b", "nick_name": "b", "data_type": "NUMBER"},
    ]


def test_datasource_input():
    element_id = "cyf"
    version_id = "cyf"
    e = DatabaseInput(element_id, version_id, "u_id")
    process_id = generate_uuid()
    # 测试执行
    (
        code,
        message,
        dependency_arr,
        data_arr,
        fields_arr,
        role_arr,
        model_arr,
        scaler_arr,
    ) = e.element_process(process_id, None, None, None, None, None, None)
    assert code == 0
    assert data_arr == [[]]
    assert fields_arr == [
        [
            {"data_type": "VARCHAR2", "name": "char", "nick_name": "测试啊"},
            {
                "data_type": "VARCHAR2",
                "name": "character",
                "nick_name": "character".upper(),
            },
            {
                "data_type": "VARCHAR2",
                "name": "varchar",
                "nick_name": "varchar".upper(),
            },
            {
                "data_type": "VARCHAR2",
                "name": "varchar2",
                "nick_name": "varchar2".upper(),
            },
            {"data_type": "NUMBER", "name": "numeric", "nick_name": "numeric".upper()},
            {"data_type": "NUMBER", "name": "decimal", "nick_name": "decimal".upper()},
            {"data_type": "NUMBER", "name": "number", "nick_name": "number".upper()},
            {"data_type": "NUMBER", "name": "dec", "nick_name": "dec".upper()},
            {"data_type": "NUMBER", "name": "bit", "nick_name": "bit".upper()},
            {"data_type": "NUMBER", "name": "integer", "nick_name": "integer".upper()},
            {"data_type": "NUMBER", "name": "int", "nick_name": "int".upper()},
            {"data_type": "NUMBER", "name": "bigint", "nick_name": "bigint".upper()},
            {"data_type": "NUMBER", "name": "tinyint", "nick_name": "tinyint".upper()},
            {"data_type": "NUMBER", "name": "byte", "nick_name": "byte".upper()},
            {
                "data_type": "NUMBER",
                "name": "smallint",
                "nick_name": "smallint".upper(),
            },
            {"data_type": "NUMBER", "name": "binary", "nick_name": "binary".upper()},
            {
                "data_type": "NUMBER",
                "name": "varbinary",
                "nick_name": "varbinary".upper(),
            },
            {"data_type": "NUMBER", "name": "float", "nick_name": "float".upper()},
            {"data_type": "NUMBER", "name": "double", "nick_name": "double".upper()},
            {"data_type": "NUMBER", "name": "real", "nick_name": "real".upper()},
            {
                "data_type": "NUMBER",
                "name": "double_precision",
                "nick_name": "double_precision".upper(),
            },
            {"data_type": "DATE", "name": "date", "nick_name": "date".upper()},
            {"data_type": "VARCHAR2", "name": "time", "nick_name": "time".upper()},
            {
                "data_type": "DATE",
                "name": "timestamp",
                "nick_name": "timestamp".upper(),
            },
            {"data_type": "DATE", "name": "datetime", "nick_name": "datetime".upper()},
            {
                "data_type": "VARCHAR2",
                "name": "time_with_time_zone",
                "nick_name": "time_with_time_zone".upper(),
            },
            {
                "data_type": "VARCHAR2",
                "name": "timestamp_with_time_zone",
                "nick_name": "timestamp_with_time_zone".upper(),
            },
            {
                "data_type": "VARCHAR2",
                "name": "timestamp_with_local_time_zone",
                "nick_name": "timestamp_with_local_time_zone".upper(),
            },
            {
                "data_type": "VARCHAR2",
                "name": "datetime_with_time_zone",
                "nick_name": "datetime_with_time_zone".upper(),
            },
            {"data_type": "VARCHAR2", "name": "text", "nick_name": "text".upper()},
            {"data_type": "VARCHAR2", "name": "image", "nick_name": "image".upper()},
            {
                "data_type": "VARCHAR2",
                "name": "longvarchar",
                "nick_name": "longvarchar".upper(),
            },
            {
                "data_type": "NUMBER",
                "name": "longvarbinary",
                "nick_name": "longvarbinary".upper(),
            },
            {"data_type": "VARCHAR2", "name": "blob", "nick_name": "blob".upper()},
            {"data_type": "VARCHAR2", "name": "clob", "nick_name": "clob".upper()},
            {
                "data_type": "VARCHAR2",
                "name": "interval_year",
                "nick_name": "interval_year".upper(),
            },
            {
                "data_type": "VARCHAR2",
                "name": "interval_day",
                "nick_name": "interval_day".upper(),
            },
        ]
    ]

    # 测试洞察
    insight_result: ExecuteResult = insight_element(version_id, element_id, DATA_FILTER, process_id)
    assert insight_result.code == 0
    assert insight_result.data["data"] == []
    assert insight_result.data["fields"] == fields_arr[0]


# @pytest.mark.parametrize(
#     "data_arr, fields_arr",
#     [
#         (
#             [
#                 [
#                     {
#                         "c": datetime(2024, 6, 2, 11, 22, 33),
#                         "d": datetime(2024, 6, 2, 11, 22, 33),
#                         "b": 8848,
#                         "e": "blob++++++++++++++++++++++++++++++++++++++++++++++++++++",
#                         "a": "a1",
#                     },
#                     {
#                         "c": datetime(2024, 2, 22, 21, 22, 23),
#                         "d": None,
#                         "b": 3,
#                         "e": "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#                         "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#                         "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本",
#                         "a": "a22",
#                     },
#                 ]
#             ],
#             [
#                 [
#                     {"name": "b", "nick_name": "b", "data_type": "NUMBER"},
#                     {"name": "c", "nick_name": "c", "data_type": "DATE"},
#                     {"name": "d", "nick_name": "d", "data_type": "DATE"},
#                     {"name": "e", "nick_name": "e", "data_type": "VARCHAR2"},
#                     {"name": "a", "nick_name": "a", "data_type": "VARCHAR2"},
#                 ]
#             ],
#         ),
#     ],
# )
# def test_datasource_output(data_arr, fields_arr):
#     # 清理数据
#     db_helper_target = TransientSqlHelper(
#         "jdbc:dm://192.168.1.197:5236/JKY_DT?"
#         "zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=utf-8",
#         "jky_dt",
#         "jky",
#     )  # DM
#     # db_helper_target = TransientSqlHelper("jdbc:oracle:thin:@192.168.1.198:1524/orcl", "jky_dt", "jky")  # Oracle
#     db_helper_target.execute("delete from test_database_output")
#
#     element_id = "element_id-test"
#     version_id = "version_id-test"
#     user_id = "user_id-test"
#     e = DatabaseOutput(element_id, version_id, user_id)
#     process_id = generate_uuid()
#     # 测试执行
#     (
#         code,
#         message,
#         dependency_arr,
#         data_arr,
#         fields_arr,
#         role_arr,
#         model_arr,
#         scaler_arr,
#     ) = e.element_process(process_id, None, data_arr, fields_arr, None, None, None)
#     assert code == 0
#
#     # 测试数据是否插入成功
#     data = db_helper_target.fetchall("select a, b, c, d, e from test_database_output")
#     # DM
#     assert data == [
#         {
#             "a": "a1",
#             "b": 8848,
#             "c": time(11, 22, 33),
#             "d": datetime(2024, 6, 2, 11, 22, 33),
#             "e": "blob++++++++++++++++++++++++++++++++++++++++++++++++++++",
#         },
#         {
#             "a": "a22",
#             "b": 3,
#             "c": time(21, 22, 23),
#             "d": None,
#             "e": "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#             "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#             "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本",
#         },
#     ]
#     # Oracle
#     # assert data == [
#     #     {
#     #         "a": "a1",
#     #         "b": 8848,
#     #         "c": datetime(2024, 6, 2, 11, 22, 33),
#     #         "d": datetime(2024, 6, 2, 11, 22, 33),
#     #         "e": "blob++++++++++++++++++++++++++++++++++++++++++++++++++++",
#     #     }, {
#     #         "a": "a22",
#     #         "b": 3,
#     #         "c": datetime(2024, 2, 22, 21, 22, 23),
#     #         "d": datetime(2024, 2, 22, 21, 22, 23),
#     #         "e": "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#     #              "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#     #              "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本",
#     #     }
#     # ]
#
#     # 测试洞察
#     insight_result: ExecuteResult = insight_element(version_id, element_id, DATA_FILTER, process_id)
#     assert insight_result.code == 0
#     print(insight_result.data["data"])
#     assert insight_result.data["data"] == [
#         {
#             "A": "a1",
#             "B": 8848,
#             "C": datetime(2024, 6, 2, 11, 22, 33),
#             "D": datetime(2024, 6, 2, 11, 22, 33),
#             "E": "blob++++++++++++++++++++++++++++++++++++++++++++++++++++",
#         },
#         {
#             "A": "a22",
#             "B": 3,
#             "C": datetime(2024, 2, 22, 21, 22, 23),
#             "D": None,
#             "E": "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#             "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本"
#             "超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本超长大文本",
#         },
#     ]
#     assert insight_result.data["fields"] == [
#         {"name": "A", "nick_name": "A", "data_type": "VARCHAR2"},
#         {"name": "B", "nick_name": "B", "data_type": "NUMBER"},
#         {"name": "C", "nick_name": "C", "data_type": "DATE"},
#         {"name": "D", "nick_name": "D", "data_type": "DATE"},
#         {"name": "E", "nick_name": "E", "data_type": "VARCHAR2"},
#     # ]
