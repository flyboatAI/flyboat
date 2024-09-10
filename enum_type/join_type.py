from enum import Enum


class JoinType(str, Enum):
    InnerJoin = "inner_join"
    LeftJoin = "left_join"
    FullJoin = "full_join"
