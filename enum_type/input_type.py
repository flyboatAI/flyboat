from enum import Enum


class ValueType(str, Enum):
    IntRange = "int_range"
    YearRange = "year_range"
    MonthRange = "month_range"
    DayRange = "day_range"
    Table = "table"
    SingleValue = "single_value"
    Model = "model"
