def type_convert(value, data_type):
    type_map = {"int": int, "float": float, "string": str}
    return type_map.get(data_type, str)(value)
