from enum import Enum


class CompareType(str, Enum):
    GreatThan = "great_than"
    GreatThanOrEqual = "great_than_or_equal"
    Equal = "equal"
    LessThan = "less_than"
    LessThanOrEqual = "less_than_or_equal"
    NotEqual = "not_equal"
    IsNull = "is_null"
    IsNotNull = "is_not_null"
